from datetime import date, datetime
from typing import cast

import numpy as np
import pandas as pd

from alphadata.core.errors import SchemaMismatch
from alphadata.schemas.enums import ENUMS
from alphadata.schemas.types import DERIVED_PARTITIONS, Column, TableSchema

_NULL_DTYPES: dict[str, str] = {
    "string": "str",
    "int32": "Int32",
    "int64": "Int64",
    "float64": "float64",
    "bool": "boolean",
    "date": "object",
    "timestamp": "datetime64[us, UTC]",
    "list<string>": "object",
}


def conform(df: pd.DataFrame, schema: TableSchema) -> pd.DataFrame:
    """Đưa DataFrame về đúng hợp đồng: thứ tự cột, kiểu, null, enum. Không khớp thì SchemaMismatch.

    Cột nullable bị thiếu được thêm toàn null; cột không null bị thiếu hoặc cột thừa thì raise.
    """
    expected = [c.name for c in schema.all_columns]
    extra = [str(c) for c in df.columns if c not in expected]
    if extra:
        raise SchemaMismatch(f"{schema.name}: unexpected columns {extra}")
    missing = [c.name for c in schema.all_columns if c.name not in df.columns and not c.nullable]
    if missing:
        raise SchemaMismatch(f"{schema.name}: missing required columns {missing}")

    out: dict[str, pd.Series] = {}
    for col in schema.all_columns:
        if col.name in df.columns:
            series = df[col.name].reset_index(drop=True)
        else:
            series = pd.Series([None] * len(df), dtype="object")
        if not col.nullable and series.isna().any():
            raise SchemaMismatch(f"{schema.name}.{col.name}: nulls in non-nullable column")
        try:
            out[col.name] = _cast(series, col)
        except (TypeError, ValueError) as exc:
            raise SchemaMismatch(f"{schema.name}.{col.name}: {exc}") from exc
        if col.enum is not None:
            bad = sorted(set(out[col.name].dropna().unique()) - ENUMS[col.enum])
            if bad:
                raise SchemaMismatch(f"{schema.name}.{col.name}: values not in enum {bad}")
    return pd.DataFrame(out)


def split_partitions(
    df: pd.DataFrame, schema: TableSchema
) -> list[tuple[dict[str, object], pd.DataFrame]]:
    """Chia DataFrame đã conform theo partition của bảng, mỗi phần sắp theo `sort`."""
    if not schema.partition:
        return [({}, _sorted(df, schema))]
    keys = pd.DataFrame(index=df.index)
    for p in schema.partition:
        if p in DERIVED_PARTITIONS:
            # TableSchema.__post_init__ đã bảo đảm partition_time có giá trị khi dùng year/month.
            keys[p] = _derive(df[cast(str, schema.partition_time)], p)
        else:
            keys[p] = df[p]
    parts: list[tuple[dict[str, object], pd.DataFrame]] = []
    for values, group in keys.groupby(list(schema.partition), sort=True):
        partition = {p: _plain(v) for p, v in zip(schema.partition, values, strict=True)}
        parts.append((partition, _sorted(df.loc[group.index], schema)))
    return parts


def _cast(series: pd.Series, col: Column) -> pd.Series:
    if len(series) and series.isna().all():
        return pd.Series([None] * len(series), dtype=_NULL_DTYPES[col.type])
    match col.type:
        case "string":
            _require_instances(series, str, "str")
            return series.astype("str")
        case "int32" | "int64":
            if pd.api.types.is_bool_dtype(series):
                raise TypeError("bool values in integer column")
            numeric = pd.to_numeric(series, errors="raise")
            non_null = numeric.dropna()
            if pd.api.types.is_float_dtype(numeric) and not (non_null % 1 == 0).all():
                raise ValueError("non-integral values in integer column")
            if col.type == "int64":
                return numeric.astype(pd.Int64Dtype() if col.nullable else np.int64)
            return numeric.astype(pd.Int32Dtype() if col.nullable else np.int32)
        case "float64":
            if pd.api.types.is_bool_dtype(series):
                raise TypeError("bool values in float column")
            return pd.to_numeric(series, errors="raise").astype("float64")
        case "bool":
            if not pd.api.types.is_bool_dtype(series):
                _require_instances(series, (bool, np.bool_), "bool")
            return series.astype("boolean" if col.nullable else "bool")
        case "timestamp":
            if not isinstance(series.dtype, pd.DatetimeTZDtype):
                raise TypeError(f"expected timezone-aware datetimes (UTC), got {series.dtype}")
            utc: pd.Series = series.dt.tz_convert("UTC").dt.as_unit("us")
            return utc
        case "date":
            if isinstance(series.dtype, pd.DatetimeTZDtype):
                raise TypeError("expected dates, got timezone-aware datetimes")
            if pd.api.types.is_datetime64_dtype(series):
                present = series.dropna()
                if (present.dt.normalize() != present).any():
                    raise ValueError("datetimes with a time part in date column")
                dates: pd.Series = series.dt.date.astype("object")
                return dates.where(series.notna(), None)
            bad = series.dropna().map(lambda v: isinstance(v, datetime) or not isinstance(v, date))
            if bad.any():
                raise TypeError(f"non-date value {series.dropna()[bad].iloc[0]!r}")
            return series.astype("object")
        case "list<string>":

            def to_list(v: object) -> list[str] | None:
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    return None
                if not isinstance(v, list | tuple) or not all(isinstance(x, str) for x in v):
                    raise TypeError(f"expected list of str, got {v!r}")
                return list(v)

            return series.map(to_list).astype("object")
    raise AssertionError(f"unhandled type {col.type}")


def _require_instances(series: pd.Series, kind: type | tuple[type, ...], label: str) -> None:
    non_null = series.dropna()
    bad = non_null.map(lambda v: not isinstance(v, kind))
    if bad.any():
        raise TypeError(f"expected {label}, got {non_null[bad].iloc[0]!r}")


def _derive(values: pd.Series, part: str) -> pd.Series:
    if isinstance(values.dtype, pd.DatetimeTZDtype):
        derived: pd.Series = values.dt.year if part == "year" else values.dt.month
        return derived
    return values.map(lambda d: d.year if part == "year" else d.month)


def _plain(value: object) -> object:
    return value.item() if isinstance(value, np.generic) else value


def _sorted(df: pd.DataFrame, schema: TableSchema) -> pd.DataFrame:
    by = list(schema.sort or schema.key)
    return df.sort_values(by, kind="stable").reset_index(drop=True)
