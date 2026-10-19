import hashlib
import io
import re
import zipfile
from datetime import UTC, date, datetime, timedelta
from typing import Literal

from alphadata.core.errors import NotAvailableYet, SourceFormatChanged
from alphadata.ingestion.pipeline import FetchContext
from alphadata.ingestion.sources.binance.constants import HOST_RATE, SYMBOL_RE
from alphadata.storage.raw import RawFile

PeriodKind = Literal["daily", "monthly"]
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def parse_period(period: str) -> tuple[PeriodKind, date, date]:
    """`YYYY-MM-DD` → file ngày, `YYYY-MM` → file tháng. Trả (loại, ngày đầu, ngày sau cùng + 1)."""
    if _DAY_RE.fullmatch(period):
        start = date.fromisoformat(period)
        return "daily", start, start + timedelta(days=1)
    if _MONTH_RE.fullmatch(period):
        year, month = int(period[:4]), int(period[5:])
        start = date(year, month, 1)
        end = date(year + month // 12, month % 12 + 1, 1)
        return "monthly", start, end
    raise ValueError(f"period must be YYYY-MM-DD or YYYY-MM, got {period!r}")


def utc_today() -> date:
    return datetime.now(UTC).date()


def backfill_periods(start: date, end: date, today: date, publish_delay_days: int) -> list[str]:
    """Chia [start, end]: tháng trọn vẹn đã có file tháng → `YYYY-MM`, còn lại theo ngày."""
    periods: list[str] = []
    day = start
    while day <= end:
        month_start = day.replace(day=1)
        next_month = date(month_start.year + month_start.month // 12, month_start.month % 12 + 1, 1)
        month_end = next_month - timedelta(days=1)
        whole_month = day == month_start and month_end <= end
        published = today >= next_month + timedelta(days=publish_delay_days)
        if whole_month and published:
            periods.append(f"{month_start.year:04d}-{month_start.month:02d}")
            day = next_month
            continue
        last = min(end, month_end)
        while day <= last:
            periods.append(day.isoformat())
            day += timedelta(days=1)
    return periods


def period_bounds(period: str) -> tuple[datetime, datetime]:
    _, start, end = parse_period(period)
    return (
        datetime(start.year, start.month, start.day, tzinfo=UTC),
        datetime(end.year, end.month, end.day, tzinfo=UTC),
    )


def validate_symbol(symbol: str) -> None:
    if not SYMBOL_RE.fullmatch(symbol):
        raise ValueError(f"symbol must be uppercase letters/digits (e.g. BTCUSDT), got {symbol!r}")


def download_verified(ctx: FetchContext, url: str) -> RawFile:
    """Tải zip và .CHECKSUM, so SHA-256 rồi mới lưu raw. Chưa đăng (404) thì NotAvailableYet."""
    resp = ctx.get(url, host_rate=HOST_RATE)
    if resp.status_code == 404:
        raise NotAvailableYet(f"not published yet: {resp.url}")
    checksum = ctx.get(url + ".CHECKSUM", host_rate=HOST_RATE)
    if checksum.status_code == 404:
        raise NotAvailableYet(f"checksum not published yet: {checksum.url}")
    expected = checksum.content.decode("ascii", errors="replace").split()[:1]
    if not expected or not _SHA256_RE.fullmatch(expected[0].lower()):
        raise SourceFormatChanged(f"unexpected checksum file content at {checksum.url}")
    actual = hashlib.sha256(resp.content).hexdigest()
    if actual != expected[0].lower():
        raise SourceFormatChanged(f"sha256 mismatch for {resp.url}: {actual} != {expected[0]}")
    return ctx.save(resp.content, url.rsplit("/", 1)[1], url)


def read_single_csv(content: bytes, name: str) -> bytes:
    """Nội dung CSV duy nhất trong zip của Binance."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            members = [m for m in zf.namelist() if not m.endswith("/")]
            if len(members) != 1 or not members[0].endswith(".csv"):
                raise SourceFormatChanged(f"{name}: expected one CSV in zip, got {members}")
            return zf.read(members[0])
    except zipfile.BadZipFile as exc:
        raise SourceFormatChanged(f"{name}: not a valid zip ({exc})") from exc
