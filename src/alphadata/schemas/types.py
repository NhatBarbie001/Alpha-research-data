from dataclasses import dataclass
from typing import Literal

import pyarrow as pa

from alphadata.schemas.enums import ENUMS

LogicalType = Literal[
    "string", "int32", "int64", "float64", "bool", "date", "timestamp", "list<string>"
]
Layer = Literal["silver", "reference", "gold"]

ARROW_TYPES: dict[str, pa.DataType] = {
    "string": pa.string(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "list<string>": pa.list_(pa.string()),
}

# Cột partition suy ra từ cột thời gian; chỉ nằm trong đường dẫn, không có trong file.
DERIVED_PARTITIONS = ("year", "month")


@dataclass(frozen=True)
class Column:
    name: str
    type: LogicalType
    nullable: bool = False
    enum: str | None = None


LAYER_COLUMNS: dict[str, tuple[Column, ...]] = {
    "silver": (Column("source", "string"), Column("ingested_at", "timestamp")),
    "reference": (Column("built_at", "timestamp"),),
    "gold": (Column("built_at", "timestamp"),),
}


@dataclass(frozen=True)
class TableSchema:
    name: str
    layer: Layer
    columns: tuple[Column, ...]  # không gồm cột bắt buộc của tầng: tự thêm vào cuối
    key: tuple[str, ...]
    partition: tuple[str, ...] = ()
    sort: tuple[str, ...] = ()
    partition_time: str | None = None  # cột date/timestamp để suy ra year, month
    version: int = 1

    def __post_init__(self) -> None:
        names = [c.name for c in self.all_columns]
        problems: list[str] = []
        if len(names) != len(set(names)):
            problems.append(f"duplicate columns (or layer column redeclared): {names}")
        for label, cols in (("key", self.key), ("sort", self.sort)):
            unknown = [c for c in cols if c not in names]
            if unknown:
                problems.append(f"{label} has unknown columns {unknown}")
        for p in self.partition:
            if p in DERIVED_PARTITIONS:
                time_col = self._find(self.partition_time)
                if time_col is None or time_col.type not in ("date", "timestamp"):
                    problems.append(
                        f"partition {p!r} needs partition_time = a date/timestamp column"
                    )
            elif p not in names:
                problems.append(f"partition has unknown column {p!r}")
        for c in self.columns:
            if c.enum is not None and c.enum not in ENUMS:
                problems.append(f"column {c.name!r} uses unknown enum {c.enum!r}")
        if problems:
            raise ValueError(f"invalid TableSchema {self.name}: " + "; ".join(problems))

    @property
    def all_columns(self) -> tuple[Column, ...]:
        return (*self.columns, *LAYER_COLUMNS[self.layer])

    @property
    def storage_layer(self) -> Literal["silver", "gold"]:
        """Thư mục gốc trong lake: reference nằm chung dưới gold/."""
        return "silver" if self.layer == "silver" else "gold"

    def column(self, name: str) -> Column:
        col = self._find(name)
        if col is None:
            raise KeyError(f"{self.name} has no column {name!r}")
        return col

    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [pa.field(c.name, ARROW_TYPES[c.type], nullable=c.nullable) for c in self.all_columns]
        )

    def path_pattern(self) -> str:
        parts = [f"{p}=*" for p in self.partition]
        return "/".join([self.storage_layer, self.name, *parts, "part.parquet"])

    def _find(self, name: str | None) -> Column | None:
        return next((c for c in self.all_columns if c.name == name), None)
