from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from alphadata.core.errors import SchemaMismatch
from alphadata.schemas import conform, split_partitions
from alphadata.schemas.silver import CRYPTO_BARS_1M
from alphadata.schemas.types import Column, TableSchema
from alphadata.storage.parquet import read_parquet_if_exists, write_parquet_atomic

INGESTED = pd.Timestamp("2026-10-12 01:00", tz="UTC")


def crypto_frame(n: int = 3, start: str = "2026-10-11", symbol: str = "BTCUSDT") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": symbol,
            "ts_utc": pd.date_range(start, periods=n, freq="min", tz="UTC"),
            "open": list(range(1, n + 1)),  # int: phải được ép sang float64
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "quote_volume": 15.0,
            "trades": 7,
            "taker_buy_base": 4.0,
            "taker_buy_quote": 6.0,
            "source": "binance",
            "ingested_at": INGESTED,
        }
    )


def test_conform_casts_and_orders() -> None:
    df = crypto_frame()[::-1].reset_index(drop=True)  # cột đúng, thứ tự cột khác schema
    df = df[list(reversed(df.columns))]
    out = conform(df, CRYPTO_BARS_1M)
    assert list(out.columns) == CRYPTO_BARS_1M.arrow_schema().names
    assert out["open"].dtype == "float64"
    assert str(out["ts_utc"].dtype) == "datetime64[us, UTC]"
    assert out["trades"].dtype == "int64"


def test_conformed_frame_is_accepted_by_storage(tmp_path: Path) -> None:
    path = tmp_path / "part.parquet"
    out = conform(crypto_frame(), CRYPTO_BARS_1M)
    write_parquet_atomic(out, path, schema=CRYPTO_BARS_1M.arrow_schema())
    back = read_parquet_if_exists(path)
    assert back is not None
    assert back["close"].tolist() == [1.5, 1.5, 1.5]


def test_converts_other_timezones_to_utc() -> None:
    df = crypto_frame()
    df["ts_utc"] = df["ts_utc"].dt.tz_convert("Asia/Ho_Chi_Minh")
    out = conform(df, CRYPTO_BARS_1M)
    assert str(out["ts_utc"].dt.tz) == "UTC"
    assert out["ts_utc"].iloc[0] == pd.Timestamp("2026-10-11", tz="UTC")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda df: df.drop(columns=["close"]), "missing required columns \\['close'\\]"),
        (lambda df: df.assign(extra=1), "unexpected columns \\['extra'\\]"),
        (lambda df: df.assign(close=["a", "b", "c"]), "crypto_bars_1m.close"),
        (lambda df: df.assign(trades=[1.5, 2.0, 3.0]), "non-integral"),
        (lambda df: df.assign(trades=[True, False, True]), "bool values"),
        (lambda df: df.assign(symbol=[1, 2, 3]), "expected str"),
        (lambda df: df.assign(ts_utc=df["ts_utc"].dt.tz_localize(None)), "timezone-aware"),
        (lambda df: df.assign(close=[1.0, None, 2.0]), "nulls in non-nullable"),
    ],
    ids=[
        "missing",
        "extra",
        "bad-float",
        "non-integral",
        "bool-as-int",
        "int-as-str",
        "naive-ts",
        "null-required",
    ],
)
def test_contract_violations_raise(mutate: object, message: str) -> None:
    with pytest.raises(SchemaMismatch, match=message):
        conform(mutate(crypto_frame()), CRYPTO_BARS_1M)  # type: ignore[operator]


MIXED = TableSchema(
    "mixed_test",
    "gold",
    (
        Column("d", "date"),
        Column("session", "string", enum="session"),
        Column("flags", "list<string>", nullable=True),
        Column("n", "int32", nullable=True),
        Column("ok", "bool"),
        Column("note", "string", nullable=True),
    ),
    key=("d",),
    partition=("year",),
    partition_time="d",
)


def mixed_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "d": [date(2025, 12, 31), date(2026, 1, 2)],
            "session": ["regular", "pre"],
            "flags": [["stale"], None],
            "n": [1, None],
            "ok": [True, False],
            "built_at": INGESTED,
        }
    )


def test_mixed_types_and_missing_nullable_column() -> None:
    out = conform(mixed_frame(), MIXED)
    assert out["note"].isna().all()
    assert out["n"].dtype == "Int32"
    assert out["flags"].tolist()[0] == ["stale"]
    assert out["d"].tolist() == [date(2025, 12, 31), date(2026, 1, 2)]


def test_unknown_enum_value_rejected() -> None:
    with pytest.raises(SchemaMismatch, match="not in enum \\['lunch'\\]"):
        conform(mixed_frame().assign(session=["regular", "lunch"]), MIXED)


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ([datetime(2026, 1, 1, 9, 30), date(2026, 1, 2)], "non-date"),
        (pd.to_datetime(["2026-01-01 09:30", "2026-01-02"], format="ISO8601"), "time part"),
    ],
)
def test_bad_dates_rejected(values: Any, message: str) -> None:
    with pytest.raises(SchemaMismatch, match=message):
        conform(mixed_frame().assign(d=values), MIXED)


def test_midnight_datetimes_accepted_as_dates() -> None:
    out = conform(mixed_frame().assign(d=pd.to_datetime(["2026-01-01", "2026-01-02"])), MIXED)
    assert out["d"].tolist() == [date(2026, 1, 1), date(2026, 1, 2)]


def test_bad_list_rejected() -> None:
    with pytest.raises(SchemaMismatch, match="list of str"):
        conform(mixed_frame().assign(flags=pd.Series(["stale", None], dtype=object)), MIXED)


def test_split_partitions_by_symbol_and_month() -> None:
    df = pd.concat(
        [
            crypto_frame(2, start="2026-09-30 23:59"),
            crypto_frame(2, start="2026-09-30 23:59", symbol="ETHUSDT"),
        ],
        ignore_index=True,
    )
    parts = split_partitions(conform(df, CRYPTO_BARS_1M), CRYPTO_BARS_1M)
    keys = [p for p, _ in parts]
    assert keys == [
        {"symbol": "BTCUSDT", "year": 2026, "month": 9},
        {"symbol": "BTCUSDT", "year": 2026, "month": 10},
        {"symbol": "ETHUSDT", "year": 2026, "month": 9},
        {"symbol": "ETHUSDT", "year": 2026, "month": 10},
    ]
    assert all(type(v) in (str, int) for p in keys for v in p.values())
    assert all(len(frame) == 1 for _, frame in parts)


def test_split_partitions_by_date_year() -> None:
    parts = split_partitions(conform(mixed_frame(), MIXED), MIXED)
    assert [p for p, _ in parts] == [{"year": 2025}, {"year": 2026}]


def test_split_without_partition_sorts_by_key() -> None:
    schema = TableSchema("flat", "gold", (Column("k", "int64"),), key=("k",))
    df = pd.DataFrame({"k": [3, 1, 2], "built_at": INGESTED})
    [(partition, frame)] = split_partitions(conform(df, schema), schema)
    assert partition == {}
    assert frame["k"].tolist() == [1, 2, 3]


@pytest.mark.parametrize(
    ("column", "values", "message"),
    [
        ("ok", [1, 0], "expected bool"),
        ("d", pd.to_datetime(["2026-01-01", "2026-01-02"]).tz_localize("UTC"), "timezone-aware"),
    ],
)
def test_more_type_violations(column: str, values: Any, message: str) -> None:
    with pytest.raises(SchemaMismatch, match=message):
        conform(mixed_frame().assign(**{column: values}), MIXED)


def test_bool_in_float_column_rejected() -> None:
    with pytest.raises(SchemaMismatch, match="bool values in float"):
        conform(crypto_frame().assign(close=[True, False, True]), CRYPTO_BARS_1M)
