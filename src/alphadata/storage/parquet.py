import os
from collections.abc import Sequence
from pathlib import Path
from typing import Final

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from alphadata.core.errors import SchemaMismatch

COMPRESSION: Final = "zstd"
COMPRESSION_LEVEL = 9
ROW_GROUP_SIZE = 1_000_000


def read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    if not path.is_file():
        return None
    return pq.read_table(path).to_pandas()


def write_parquet_atomic(df: pd.DataFrame, path: Path, *, schema: pa.Schema) -> None:
    """Ghi đúng hợp đồng `schema` (từ `TableSchema.arrow_schema()`): file tạm, fsync, os.replace."""
    table = to_arrow(df, schema)
    floats = [f.name for f in schema if pa.types.is_floating(f.type)]
    others = [f.name for f in schema if not pa.types.is_floating(f.type)]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with tmp.open("wb") as f:
            pq.write_table(
                table,
                f,
                compression=COMPRESSION,
                compression_level=COMPRESSION_LEVEL,
                # byte_stream_split chỉ hiệu quả khi cột không dùng dictionary encoding.
                # pyarrow nhận list tên cột cho hai tham số này; stub chỉ khai bool.
                use_dictionary=others,  # type: ignore[arg-type]
                use_byte_stream_split=floats,  # type: ignore[arg-type]
                row_group_size=ROW_GROUP_SIZE,
                write_statistics=True,
            )
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def to_arrow(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    missing = [name for name in schema.names if name not in df.columns]
    extra = [str(c) for c in df.columns if c not in schema.names]
    if missing or extra:
        raise SchemaMismatch(f"columns differ from schema: missing={missing} extra={extra}")
    null_in_required = [f.name for f in schema if not f.nullable and df[f.name].isna().any()]
    if null_in_required:
        raise SchemaMismatch(f"nulls in non-nullable columns: {null_in_required}")
    try:
        return pa.Table.from_pandas(df[schema.names], schema=schema, preserve_index=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        raise SchemaMismatch(f"cannot cast to schema: {exc}") from exc


def upsert(
    existing: pd.DataFrame | None, new: pd.DataFrame, key_cols: Sequence[str]
) -> pd.DataFrame:
    """Nối cũ và mới, trùng khoá giữ bản mới (trùng trong `new` giữ dòng sau), sắp theo khoá."""
    keys = list(key_cols)
    missing = [k for k in keys if k not in new.columns]
    if not keys or missing:
        raise ValueError(f"invalid key_cols {keys}; missing in new: {missing}")
    frames = [f for f in (existing, new) if f is not None and not f.empty]
    if not frames:
        return new.iloc[0:0].copy()
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=keys, keep="last")
    return merged.sort_values(keys, kind="stable").reset_index(drop=True)
