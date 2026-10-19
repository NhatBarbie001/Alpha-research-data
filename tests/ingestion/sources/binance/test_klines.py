import hashlib
import io
import zipfile
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
import responses

from alphadata.core import http
from alphadata.core.errors import NotAvailableYet, SourceFormatChanged
from alphadata.core.settings import Settings
from alphadata.ingestion.pipeline import FetchContext, get_dataset, run_partition
from alphadata.ingestion.sources.binance.common import parse_period
from alphadata.ingestion.sources.binance.klines import (
    BinanceSpotKlines1m,
    kline_url,
    parse_klines,
)
from alphadata.metadata import MetaStore
from alphadata.storage.raw import manifest_path

FIXTURES = Path(__file__).parent / "fixtures"
BASE = "https://data.binance.vision/data/spot"


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def make_zip(member: str, text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(member, text)
    return buf.getvalue()


def checksum_for(content: bytes, name: str) -> str:
    return f"{hashlib.sha256(content).hexdigest()}  {name}\n"


def utc(text: str) -> pd.Timestamp:
    return pd.Timestamp(text, tz="UTC")


# --- normalize trên mẫu thật -------------------------------------------------------


def test_parse_millisecond_file_before_2025() -> None:
    df = parse_klines(fixture("BTCUSDT-1m-2024-12-31.zip"), "f.zip", "BTCUSDT")
    assert len(df) == 4
    assert df["ts_utc"].iloc[0] == utc("2024-12-31 00:00")
    assert df["ts_utc"].iloc[-1] == utc("2024-12-31 23:59")
    first = df.iloc[0]
    assert (first["open"], first["close"], first["trades"]) == (92792.05, 92782.13, 1660)
    assert str(df["ts_utc"].dtype) == "datetime64[us, UTC]"
    assert set(df["symbol"]) == {"BTCUSDT"}


def test_parse_microsecond_file_from_2025() -> None:
    df = parse_klines(fixture("BTCUSDT-1m-2025-01-01.zip"), "f.zip", "BTCUSDT")
    assert df["ts_utc"].iloc[0] == utc("2025-01-01 00:00")
    assert df["ts_utc"].iloc[-1] == utc("2025-01-01 23:59")
    assert df["open"].iloc[0] == 93576.0


def test_parse_file_with_header_row() -> None:
    with_header = parse_klines(fixture("with-header-BTCUSDT-1m-2025-01-01.zip"), "f.zip", "BTCUSDT")
    plain = parse_klines(fixture("BTCUSDT-1m-2025-01-01.zip"), "f.zip", "BTCUSDT")
    pd.testing.assert_frame_equal(with_header, plain.head(3))


def test_parse_monthly_file() -> None:
    df = parse_klines(fixture("BTCUSDT-1m-2024-01.zip"), "f.zip", "BTCUSDT")
    assert df["ts_utc"].iloc[0] == utc("2024-01-01 00:00")
    assert df["ts_utc"].iloc[-1] == utc("2024-01-31 23:59")


ROW_MS = "1735603200000,1,2,0.5,1.5,10,1735603259999,15,7,4,6,0"
ROW_US = "1735689600000000,1,2,0.5,1.5,10,1735689659999999,15,7,4,6,0"


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"not a zip", "not a valid zip"),
        (make_zip("a.txt", ROW_MS), "expected one CSV"),
        (make_zip("a.csv", ""), "empty CSV"),
        (make_zip("a.csv", "1735603200000,1,2,0.5,1.5,10,1735603259999,15,7,4,6"), "expected 12"),
        (make_zip("a.csv", "1735603200000.5,1,2,0.5,1.5,10,1,15,7,4,6,0"), "not integers"),
        (make_zip("a.csv", "1735603200000,abc,2,0.5,1.5,10,1,15,7,4,6,0"), "Unable to parse"),
        (make_zip("a.csv", f"{ROW_MS}\n{ROW_US}"), "mixed epoch units"),
    ],
    ids=["not-zip", "no-csv", "empty", "11-columns", "float-time", "bad-number", "mixed-units"],
)
def test_format_changes_raise(content: bytes, message: str) -> None:
    with pytest.raises(SourceFormatChanged, match=message):
        parse_klines(content, "f.zip", "BTCUSDT")


def test_zip_with_two_files_rejected() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a.csv", ROW_MS)
        zf.writestr("b.csv", ROW_MS)
    with pytest.raises(SourceFormatChanged, match="expected one CSV"):
        parse_klines(buf.getvalue(), "f.zip", "BTCUSDT")


# --- partition, URL, check ---------------------------------------------------------


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        ("2026-10-11", ("daily", date(2026, 10, 11), date(2026, 10, 12))),
        ("2024-02", ("monthly", date(2024, 2, 1), date(2024, 3, 1))),
        ("2024-12", ("monthly", date(2024, 12, 1), date(2025, 1, 1))),
    ],
)
def test_parse_period(period: str, expected: tuple[str, date, date]) -> None:
    assert parse_period(period) == expected


@pytest.mark.parametrize("period", ["2026-13", "2026-02-30", "20261011", "2026-1-1", "latest"])
def test_invalid_period(period: str) -> None:
    with pytest.raises(ValueError):
        BinanceSpotKlines1m().validate({"symbol": "BTCUSDT", "period": period})


@pytest.mark.parametrize("symbol", ["btcusdt", "BTC/USDT", "../BTC", "BTC", "BTCUSDT?x=1"])
def test_invalid_symbol(symbol: str) -> None:
    with pytest.raises(ValueError, match="symbol"):
        BinanceSpotKlines1m().validate({"symbol": symbol, "period": "2026-10-11"})


def test_urls() -> None:
    assert kline_url("BTCUSDT", "2026-10-11") == (
        f"{BASE}/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-10-11.zip"
    )
    assert (
        kline_url("ETHUSDT", "2024-01")
        == f"{BASE}/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2024-01.zip"
    )


def test_checks_follow_period() -> None:
    ds = BinanceSpotKlines1m()
    daily = {c.name: c for c in ds.checks({"symbol": "BTCUSDT", "period": "2026-10-11"})}
    assert list(daily) == [
        "not_empty",
        "no_null_keys",
        "unique_keys",
        "schema_match",
        "ts_in_range",
        "row_count",
        "ohlc_consistent",
        "non_negative",
    ]
    rows = pd.DataFrame({"x": range(1440)})
    assert daily["row_count"].fn(rows)[0]
    monthly = {c.name: c for c in ds.checks({"symbol": "BTCUSDT", "period": "2024-01"})}
    assert monthly["row_count"].fn(rows) == (False, "rows=1440 expected=44640")


def test_registered() -> None:
    assert isinstance(get_dataset("binance_spot_klines_1m"), BinanceSpotKlines1m)


# --- fetch và run đầu-cuối với HTTP giả ---------------------------------------------


@pytest.fixture(autouse=True)
def no_rate_limit_sleep(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(http, "_sleep", lambda seconds: None)
    yield


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_root=tmp_path / "lake",
        meta_db_url=f"sqlite:///{(tmp_path / 'meta.db').as_posix()}",
        min_free_gb=0.001,
    )


@pytest.fixture
def meta(settings: Settings) -> Iterator[MetaStore]:
    store = MetaStore(settings.meta_db_url)
    yield store
    store.close()


def serve(url: str, content: bytes, checksum: str | None = None) -> None:
    responses.get(url, body=content)
    name = url.rsplit("/", 1)[1]
    responses.get(
        url + ".CHECKSUM", body=checksum if checksum is not None else checksum_for(content, name)
    )


def context(settings: Settings, tmp_path: Path) -> FetchContext:
    return FetchContext(settings, http.make_session(settings), date(2026, 10, 12), tmp_path / "raw")


@responses.activate
def test_fetch_verifies_checksum_before_saving(settings: Settings, tmp_path: Path) -> None:
    url = kline_url("BTCUSDT", "2025-01-01")
    content = fixture("BTCUSDT-1m-2025-01-01.zip")
    serve(url, content)
    [raw] = BinanceSpotKlines1m().fetch(
        context(settings, tmp_path), {"symbol": "BTCUSDT", "period": "2025-01-01"}
    )
    assert raw.path.name == "BTCUSDT-1m-2025-01-01.zip"
    assert raw.sha256 == hashlib.sha256(content).hexdigest()
    assert raw.url == url


@responses.activate
@pytest.mark.parametrize(
    ("checksum", "error", "message"),
    [
        ("0" * 64 + "  x.zip", SourceFormatChanged, "sha256 mismatch"),
        ("<html>oops</html>", SourceFormatChanged, "unexpected checksum"),
    ],
)
def test_bad_checksum_saves_nothing(
    settings: Settings, tmp_path: Path, checksum: str, error: type[Exception], message: str
) -> None:
    url = kline_url("BTCUSDT", "2025-01-01")
    serve(url, fixture("BTCUSDT-1m-2025-01-01.zip"), checksum)
    with pytest.raises(error, match=message):
        BinanceSpotKlines1m().fetch(
            context(settings, tmp_path), {"symbol": "BTCUSDT", "period": "2025-01-01"}
        )
    assert not (tmp_path / "raw").exists()


@responses.activate
def test_not_published_yet(settings: Settings, tmp_path: Path) -> None:
    url = kline_url("BTCUSDT", "2026-10-18")
    responses.get(url, status=404)
    with pytest.raises(NotAvailableYet):
        BinanceSpotKlines1m().fetch(
            context(settings, tmp_path), {"symbol": "BTCUSDT", "period": "2026-10-18"}
        )
    responses.replace(responses.GET, url, body=b"zip")
    responses.get(url + ".CHECKSUM", status=404)
    with pytest.raises(NotAvailableYet, match="checksum"):
        BinanceSpotKlines1m().fetch(
            context(settings, tmp_path), {"symbol": "BTCUSDT", "period": "2026-10-18"}
        )


@responses.activate
def test_run_partition_end_to_end(settings: Settings, meta: MetaStore) -> None:
    ds = BinanceSpotKlines1m()
    serve(kline_url("BTCUSDT", "2025-01-01"), fixture("BTCUSDT-1m-2025-01-01.zip"))
    result = run_partition(
        ds, {"symbol": "BTCUSDT", "period": "2025-01-01"}, settings=settings, meta=meta
    )

    silver = (
        settings.data_root / "silver/crypto_bars_1m/symbol=BTCUSDT/year=2025/month=1/part.parquet"
    )
    assert result.silver_paths == [silver]
    table = pq.read_table(silver)
    assert table.num_rows == 4
    assert table.column("ts_utc").to_pylist()[0] == datetime(2025, 1, 1, tzinfo=UTC)
    assert set(table.column("source").to_pylist()) == {"binance"}

    # Raw minute bị xoá sau khi ghi silver, manifest và dòng raw_files vẫn còn.
    raw = result.raw_files[0]
    assert not raw.path.exists() and manifest_path(raw.path).exists()
    assert meta.raw_files_for(result.run_id)[0]["deleted"] is True

    # Mẫu cắt ngắn chỉ có 4 dòng: row_count (warn) fail nhưng vẫn ghi.
    failed = {
        c.check_name: c.severity
        for c in meta.failed_checks(datetime.now(UTC) - timedelta(minutes=5))
    }
    assert failed == {"row_count": "warn"}


@responses.activate
def test_monthly_file_writes_month(settings: Settings, meta: MetaStore) -> None:
    serve(kline_url("BTCUSDT", "2024-01"), fixture("BTCUSDT-1m-2024-01.zip"))
    result = run_partition(
        BinanceSpotKlines1m(),
        {"symbol": "BTCUSDT", "period": "2024-01"},
        settings=settings,
        meta=meta,
    )
    assert [p.parent.name for p in result.silver_paths] == ["month=1"]
    assert result.silver_paths[0].parent.parent.name == "year=2024"


def test_bad_partition_rejected_before_any_run(settings: Settings, meta: MetaStore) -> None:
    with pytest.raises(ValueError, match="symbol"):
        run_partition(
            BinanceSpotKlines1m(),
            {"symbol": "btc", "period": "2025-01-01"},
            settings=settings,
            meta=meta,
        )
    assert meta.recent_runs() == []


@pytest.mark.live
def test_live_download_two_days_ago(settings: Settings, meta: MetaStore) -> None:
    day = (datetime.now(UTC).date() - timedelta(days=2)).isoformat()
    result = run_partition(
        BinanceSpotKlines1m(), {"symbol": "BTCUSDT", "period": day}, settings=settings, meta=meta
    )
    assert result.rows_in >= 1400
