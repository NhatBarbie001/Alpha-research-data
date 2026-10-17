from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from alphadata.core.errors import SchemaMismatch
from alphadata.storage.parquet import read_parquet_if_exists, upsert, write_parquet_atomic

FIELDS: list[pa.Field[Any]] = [
    pa.field("ts_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("close", pa.float64()),
    pa.field("trades", pa.int64()),
]
SCHEMA = pa.schema(FIELDS)


def bars(closes: list[float | None], start: str = "2026-10-11") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_utc": pd.date_range(start, periods=len(closes), freq="min", tz="UTC"),
            "symbol": "BTCUSDT",
            "close": closes,
            "trades": range(len(closes)),
        }
    )


def test_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "silver" / "t" / "part.parquet"
    df = bars([1.0, 2.0, None])
    write_parquet_atomic(df, path, schema=SCHEMA)
    back = read_parquet_if_exists(path)
    assert back is not None
    assert pq.read_schema(path).equals(SCHEMA)
    assert back["close"].tolist()[:2] == [1.0, 2.0]
    assert pd.isna(back["close"].iloc[2])
    assert (back["ts_utc"] == df["ts_utc"]).all()


def test_file_settings(tmp_path: Path) -> None:
    path = tmp_path / "part.parquet"
    write_parquet_atomic(bars([1.0, 2.0]), path, schema=SCHEMA)
    meta = pq.ParquetFile(path).metadata.row_group(0)
    columns = {meta.column(i).path_in_schema: meta.column(i) for i in range(meta.num_columns)}
    assert all(c.compression == "ZSTD" for c in columns.values())
    assert "BYTE_STREAM_SPLIT" in columns["close"].encodings
    stats = columns["ts_utc"].statistics
    assert stats is not None
    assert stats.has_min_max


def test_read_missing_file_returns_none(tmp_path: Path) -> None:
    assert read_parquet_if_exists(tmp_path / "nope.parquet") is None


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda df: df.drop(columns=["trades"]), "missing=\\['trades'\\]"),
        (lambda df: df.assign(extra=1), "extra=\\['extra'\\]"),
        (lambda df: df.assign(close=["a", "b"]), "cannot cast"),
        (lambda df: df.assign(symbol=["BTCUSDT", None]), "non-nullable"),
    ],
    ids=["missing", "extra", "bad-type", "null-in-required"],
)
def test_schema_violations_raise(tmp_path: Path, mutate: Any, message: str) -> None:
    path = tmp_path / "part.parquet"
    with pytest.raises(SchemaMismatch, match=message):
        write_parquet_atomic(mutate(bars([1.0, 2.0])), path, schema=SCHEMA)
    assert not path.exists()


def test_failure_midway_keeps_old_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "part.parquet"
    write_parquet_atomic(bars([1.0]), path, schema=SCHEMA)
    before = path.read_bytes()

    def crash(table: pa.Table, where: Any, **kwargs: Any) -> None:
        where.write(b"PAR1 half-written garbage")
        raise OSError("disk died mid-write")

    monkeypatch.setattr(pq, "write_table", crash)
    with pytest.raises(OSError, match="mid-write"):
        write_parquet_atomic(bars([9.0, 9.0]), path, schema=SCHEMA)

    assert path.read_bytes() == before
    assert [p.name for p in tmp_path.iterdir()] == ["part.parquet"]


def test_upsert_new_wins_and_sorted() -> None:
    existing = bars([1.0, 2.0, 3.0])
    new = bars([20.0, 30.0, 40.0], start="2026-10-11 00:01")
    merged = upsert(existing, new, ["symbol", "ts_utc"])
    assert merged["close"].tolist() == [1.0, 20.0, 30.0, 40.0]
    assert merged["ts_utc"].is_monotonic_increasing
    assert list(merged.index) == [0, 1, 2, 3]


def test_upsert_duplicates_within_new_keep_last() -> None:
    new = pd.concat([bars([1.0]), bars([2.0])], ignore_index=True)
    assert upsert(None, new, ["symbol", "ts_utc"])["close"].tolist() == [2.0]


def test_upsert_empty_inputs() -> None:
    empty = bars([])
    result = upsert(empty, empty, ["symbol", "ts_utc"])
    assert result.empty
    assert list(result.columns) == list(empty.columns)


def test_upsert_requires_valid_keys() -> None:
    with pytest.raises(ValueError):
        upsert(None, bars([1.0]), [])
    with pytest.raises(ValueError, match="missing"):
        upsert(None, bars([1.0]), ["nope"])
