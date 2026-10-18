import pyarrow as pa
import pytest

from alphadata.schemas import get_schema, list_schemas
from alphadata.schemas.silver import CRYPTO_BARS_1M
from alphadata.schemas.types import Column, TableSchema


def test_arrow_schema_appends_layer_columns() -> None:
    schema = CRYPTO_BARS_1M.arrow_schema()
    assert schema.names[-2:] == ["source", "ingested_at"]
    assert schema.field("ts_utc").type == pa.timestamp("us", tz="UTC")
    assert schema.field("trades").type == pa.int64()
    assert not schema.field("close").nullable


def test_path_pattern_and_storage_layer() -> None:
    assert (
        CRYPTO_BARS_1M.path_pattern()
        == "silver/crypto_bars_1m/symbol=*/year=*/month=*/part.parquet"
    )
    ref = TableSchema("trading_calendar", "reference", (Column("d", "date"),), key=("d",))
    assert ref.storage_layer == "gold"
    assert ref.path_pattern() == "gold/trading_calendar/part.parquet"
    assert ref.arrow_schema().names == ["d", "built_at"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"key": ("nope",)}, "key has unknown"),
        ({"key": ("a",), "sort": ("nope",)}, "sort has unknown"),
        ({"key": ("a",), "partition": ("nope",)}, "partition has unknown"),
        ({"key": ("a",), "partition": ("year",)}, "partition_time"),
        ({"key": ("a",), "partition": ("year",), "partition_time": "a"}, "partition_time"),
    ],
)
def test_invalid_definitions_rejected(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        TableSchema("t", "silver", (Column("a", "string"),), **kwargs)  # type: ignore[arg-type]


def test_redeclared_layer_column_and_unknown_enum_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        TableSchema("t", "silver", (Column("source", "string"),), key=("source",))
    with pytest.raises(ValueError, match="unknown enum"):
        TableSchema("t", "silver", (Column("a", "string", enum="nope"),), key=("a",))


def test_column_lookup() -> None:
    assert CRYPTO_BARS_1M.column("ingested_at").type == "timestamp"
    with pytest.raises(KeyError):
        CRYPTO_BARS_1M.column("nope")


def test_registry() -> None:
    assert get_schema("crypto_bars_1m") is CRYPTO_BARS_1M
    assert CRYPTO_BARS_1M in list_schemas("silver")
    assert list_schemas("gold") == []
    with pytest.raises(KeyError, match="unknown table"):
        get_schema("nope")
