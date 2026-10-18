from datetime import date
from pathlib import Path

import pytest

from alphadata.core.settings import Settings
from alphadata.storage import layout

S = Settings(data_root=Path("/lake"))


def test_raw_dir_orders_declared_keys_then_alphabetical() -> None:
    path = layout.raw_dir(
        S,
        "binance",
        "binance_spot_klines_1m",
        {"interval": "1m", "date": date(2026, 10, 11), "symbol": "BTCUSDT"},
        partition_keys=["symbol", "date"],
        fetch_date=date(2026, 10, 12),
    )
    assert path == Path(
        "/lake/raw/binance/binance_spot_klines_1m/symbol=BTCUSDT/date=2026-10-11/interval=1m/2026-10-12"
    )


@pytest.mark.parametrize(
    ("value", "segment"),
    [
        ("EURUSD=X", "symbol=EURUSD%3DX"),
        ("^GSPC", "symbol=%5EGSPC"),
        ("BRK.B", "symbol=BRK.B"),
        ("../../etc", "symbol=..%2F..%2Fetc"),
    ],
)
def test_values_are_encoded_and_cannot_escape(value: str, segment: str) -> None:
    path = layout.table_file(S, "silver", "equity_bars_1d", {"symbol": value})
    assert path.parent.name == segment
    assert path.parent.parent == Path("/lake/silver/equity_bars_1d")


@pytest.mark.parametrize("value", ["", ".", ".."])
def test_invalid_values_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="partition value"):
        layout.table_file(S, "silver", "t", {"symbol": value})


@pytest.mark.parametrize("name", ["Binance", "../x", "1abc", "a-b", ""])
def test_invalid_names_rejected(name: str) -> None:
    with pytest.raises(ValueError):
        layout.table_dir(S, "silver", name)
    with pytest.raises(ValueError):
        layout.table_file(S, "silver", "t", {name: "x"})


def test_missing_declared_partition_key() -> None:
    with pytest.raises(ValueError, match="missing keys"):
        layout.raw_dir(
            S, "fred", "fred_series", {}, partition_keys=["series_id"], fetch_date=date(2026, 1, 1)
        )


def test_table_file_keeps_partition_order() -> None:
    path = layout.table_file(
        S, "silver", "crypto_bars_1m", {"symbol": "BTCUSDT", "year": 2026, "month": 10}
    )
    assert path == Path(
        "/lake/silver/crypto_bars_1m/symbol=BTCUSDT/year=2026/month=10/part.parquet"
    )


def test_unpartitioned_gold_table() -> None:
    assert layout.table_file(S, "gold", "security_master") == Path(
        "/lake/gold/security_master/part.parquet"
    )


def test_checkpoint_file() -> None:
    path = layout.checkpoint_file(S, "alpaca_bars_1m", "3f2a-9c")
    assert path == Path("/lake/checkpoints/alpaca_bars_1m/3f2a-9c.json")
    with pytest.raises(ValueError, match="backfill_id"):
        layout.checkpoint_file(S, "alpaca_bars_1m", "../evil")
