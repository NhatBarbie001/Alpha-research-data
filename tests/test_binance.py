import pandas as pd

from alphadata.sources.binance import BinanceSpotKlines1m
from alphadata.storage import save_raw
from tests.conftest import binance_zip

DAY_2024 = int(pd.Timestamp("2024-01-01", tz="UTC").timestamp() * 1000)
DAY_2026 = int(pd.Timestamp("2026-10-11", tz="UTC").timestamp() * 1000)


def _normalize(tmp_path, content, partition):
    raw = save_raw(tmp_path, content, "x.zip", "http://test")
    return BinanceSpotKlines1m().normalize([raw], partition)


def test_millisecond_timestamps(tmp_path):
    df = _normalize(tmp_path, binance_zip(DAY_2024, 3, "ms"), {"symbol": "btcusdt", "date": "2024-01-01"})
    assert df["ts_utc"].iloc[0] == pd.Timestamp("2024-01-01 00:00", tz="UTC")
    assert df["ts_utc"].iloc[2] == pd.Timestamp("2024-01-01 00:02", tz="UTC")
    assert df["symbol"].unique().tolist() == ["BTCUSDT"]


def test_microsecond_timestamps_from_2025(tmp_path):
    df = _normalize(tmp_path, binance_zip(DAY_2026, 3, "us"), {"symbol": "BTCUSDT", "date": "2026-10-11"})
    assert df["ts_utc"].iloc[0] == pd.Timestamp("2026-10-11 00:00", tz="UTC")


def test_checks_flag_wrong_day_as_error(tmp_path):
    ds = BinanceSpotKlines1m()
    partition = {"symbol": "BTCUSDT", "date": "2026-10-12"}  # dữ liệu lại là ngày 11
    df = _normalize(tmp_path, binance_zip(DAY_2026, 3, "us"), partition)
    results = {r.name: r for r in ds.checks(df, partition)}
    assert not results["ts_in_range"].passed and results["ts_in_range"].severity == "error"
    assert not results["row_count"].passed and results["row_count"].severity == "warn"


def test_silver_path_daily_goes_to_month_file(tmp_path):
    ds = BinanceSpotKlines1m()
    daily = ds.silver_path(tmp_path, {"symbol": "btcusdt", "date": "2026-10-11"})
    monthly = ds.silver_path(tmp_path, {"symbol": "BTCUSDT", "month": "2026-10"})
    assert daily == monthly == tmp_path / "crypto_bars_1m" / "BTCUSDT" / "2026" / "10.parquet"
