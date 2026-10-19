from pathlib import Path

import pandas as pd

from alphadata.access.views import connect, view_names
from alphadata.core.settings import Settings
from alphadata.schemas import conform
from alphadata.schemas.silver import CRYPTO_BARS_1M
from alphadata.storage import layout
from alphadata.storage.parquet import write_parquet_atomic


def write_bars(settings: Settings, symbol: str, month: int, n: int) -> None:
    df = pd.DataFrame(
        {
            "symbol": symbol,
            "ts_utc": pd.date_range(f"2026-{month:02d}-01", periods=n, freq="min", tz="UTC"),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 1.0,
            "quote_volume": 1.5,
            "trades": 1,
            "taker_buy_base": 0.5,
            "taker_buy_quote": 0.75,
            "source": "binance",
            "ingested_at": pd.Timestamp("2026-10-12", tz="UTC"),
        }
    )
    path = layout.table_file(
        settings, "silver", "crypto_bars_1m", {"symbol": symbol, "year": 2026, "month": month}
    )
    write_parquet_atomic(conform(df, CRYPTO_BARS_1M), path, schema=CRYPTO_BARS_1M.arrow_schema())


def test_no_files_no_views(tmp_path: Path) -> None:
    con = connect(Settings(data_root=tmp_path))
    assert view_names(con) == []


def test_view_over_all_partitions(tmp_path: Path) -> None:
    # Dấu nháy trong đường dẫn lake không được làm hỏng câu SQL tạo view.
    settings = Settings(data_root=tmp_path / "it's lake")
    write_bars(settings, "BTCUSDT", 9, 3)
    write_bars(settings, "BTCUSDT", 10, 2)
    write_bars(settings, "^WEIRD", 10, 1)
    con = connect(settings)
    assert view_names(con) == ["crypto_bars_1m"]
    rows = con.execute(
        "SELECT symbol, count(*) FROM crypto_bars_1m GROUP BY symbol ORDER BY symbol"
    ).fetchall()
    # Cột symbol lấy từ file (giá trị gốc), không phải từ đường dẫn đã mã hoá %5E.
    assert rows == [("BTCUSDT", 5), ("^WEIRD", 1)]
    columns = [c[0] for c in con.execute("SELECT * FROM crypto_bars_1m LIMIT 0").description]
    assert columns == CRYPTO_BARS_1M.arrow_schema().names


def test_layer_filter(tmp_path: Path) -> None:
    settings = Settings(data_root=tmp_path)
    write_bars(settings, "BTCUSDT", 10, 1)
    assert view_names(connect(settings, layers=("gold",))) == []


def test_timestamps_are_shown_in_utc(tmp_path: Path) -> None:
    settings = Settings(data_root=tmp_path)
    write_bars(settings, "BTCUSDT", 10, 1)
    con = connect(settings)
    [(text,)] = con.execute("SELECT CAST(ts_utc AS VARCHAR) FROM crypto_bars_1m").fetchall()
    assert text == "2026-10-01 00:00:00+00"
    [(ts,)] = con.execute("SELECT ts_utc FROM crypto_bars_1m").fetchall()
    assert ts.utcoffset().total_seconds() == 0
