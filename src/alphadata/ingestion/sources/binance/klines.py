import io

import pandas as pd

from alphadata.core.errors import SourceFormatChanged
from alphadata.core.timeutil import epoch_to_utc
from alphadata.ingestion.pipeline import Dataset, FetchContext, Partition
from alphadata.ingestion.sources.binance.common import (
    download_verified,
    parse_period,
    period_bounds,
    read_single_csv,
    validate_symbol,
)
from alphadata.ingestion.sources.binance.constants import (
    BASE_URL,
    KLINE_COLUMNS,
    LICENSE_NOTE,
    MINUTES_PER_DAY,
    SOURCE_URL,
)
from alphadata.quality import (
    Check,
    default_checks,
    non_negative,
    ohlc_consistent,
    row_count,
    ts_in_range,
)
from alphadata.storage.raw import RawFile

_NUMERIC = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "taker_buy_base",
    "taker_buy_quote",
)


class BinanceSpotKlines1m(Dataset):
    name = "binance_spot_klines_1m"
    source = "binance"
    table = "crypto_bars_1m"
    description = "Nến 1 phút spot Binance (file zip ngày/tháng của data.binance.vision)"
    source_url = SOURCE_URL
    license_note = LICENSE_NOTE
    # period: YYYY-MM-DD (file ngày) hoặc YYYY-MM (file tháng)
    partition_keys = ("symbol", "period")
    keep_raw = False  # raw minute xoá sau khi ghi silver (docs/04-data-model.md mục 9.3)
    refresh_schedule = "0 1 * * *"
    catalog_notes = (
        "open_time của spot là mili-giây trước 2025-01-01, micro-giây từ đó; đã đổi sang UTC"
    )

    def validate(self, partition: Partition) -> None:
        validate_symbol(partition["symbol"])
        parse_period(partition["period"])

    def fetch(self, ctx: FetchContext, partition: Partition) -> list[RawFile]:
        return [download_verified(ctx, kline_url(partition["symbol"], partition["period"]))]

    def normalize(self, raws: list[RawFile], partition: Partition) -> pd.DataFrame:
        frames = [parse_klines(self.read(raw), raw.path.name, partition["symbol"]) for raw in raws]
        return pd.concat(frames, ignore_index=True)

    def checks(self, partition: Partition) -> list[Check]:
        start, end = period_bounds(partition["period"])
        days = (end - start).days
        return [
            *default_checks(self.schema),
            ts_in_range("ts_utc", start, end),
            row_count(MINUTES_PER_DAY * days),  # warn: Binance có lúc bảo trì, thiếu vài phút
            ohlc_consistent(),
            non_negative(["open", "high", "low", "close", "volume", "quote_volume"]),
        ]


def kline_url(symbol: str, period: str) -> str:
    kind, _, _ = parse_period(period)
    return f"{BASE_URL}spot/{kind}/klines/{symbol}/1m/{symbol}-1m-{period}.zip"


def parse_klines(zip_bytes: bytes, name: str, symbol: str) -> pd.DataFrame:
    """CSV klines 12 cột → cột của crypto_bars_1m (chưa có source, ingested_at)."""
    text = read_single_csv(zip_bytes, name)
    # Header khi trường đầu không phải số. Không dùng isdigit(): dòng hỏng kiểu "1.5" sẽ bị coi nhầm
    # là header và âm thầm bị bỏ; ở đây nó được đọc vào rồi bị kiểm kiểu bắt.
    first_field = text.split(b"\n", 1)[0].split(b",", 1)[0].strip()
    has_header = not _is_number(first_field)
    try:
        raw = pd.read_csv(io.BytesIO(text), header=None, skiprows=1 if has_header else 0)
    except pd.errors.EmptyDataError as exc:
        raise SourceFormatChanged(f"{name}: empty CSV") from exc
    if raw.shape[1] != len(KLINE_COLUMNS):
        raise SourceFormatChanged(
            f"{name}: expected {len(KLINE_COLUMNS)} columns, got {raw.shape[1]}"
        )
    raw.columns = list(KLINE_COLUMNS)
    if not pd.api.types.is_integer_dtype(raw["open_time"]) or not pd.api.types.is_integer_dtype(
        raw["count"]
    ):
        raise SourceFormatChanged(f"{name}: open_time/count are not integers")
    try:
        numeric = {c: pd.to_numeric(raw[c], errors="raise").astype("float64") for c in _NUMERIC}
        ts = epoch_to_utc(raw["open_time"])
    except ValueError as exc:
        raise SourceFormatChanged(f"{name}: {exc}") from exc
    return pd.DataFrame(
        {
            "symbol": symbol,
            "ts_utc": ts,
            "open": numeric["open"],
            "high": numeric["high"],
            "low": numeric["low"],
            "close": numeric["close"],
            "volume": numeric["volume"],
            "quote_volume": numeric["quote_volume"],
            "trades": raw["count"].astype("int64"),
            "taker_buy_base": numeric["taker_buy_base"],
            "taker_buy_quote": numeric["taker_buy_quote"],
        }
    )


def _is_number(field: bytes) -> bool:
    try:
        float(field)
    except ValueError:
        return False
    return True
