"""Check trước khi ghi silver. Mỗi hàm trả một `Check` đã gắn tham số; runner gọi `Check.fn(df)`."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pandas as pd

from alphadata.core.errors import SchemaMismatch
from alphadata.schemas import TableSchema, conform

Severity = Literal["error", "warn"]
DETAIL_MAX_CHARS = 500
_SAMPLE = 3


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: Severity
    passed: bool
    detail: str  # ví dụ "bad_rows=3 sample=[...]"


@dataclass(frozen=True)
class Check:
    name: str
    severity: Severity
    fn: Callable[[pd.DataFrame], tuple[bool, str]]


def default_checks(schema: TableSchema) -> list[Check]:
    """Bộ mặc định của mọi dataset (Dataset.checks() dùng nếu connector không override)."""
    return [
        not_empty(),
        no_null_keys(schema.key),
        unique_keys(schema.key),
        schema_match(schema),
    ]


def not_empty() -> Check:
    return Check("not_empty", "error", lambda df: (len(df) > 0, f"rows={len(df)}"))


def no_null_keys(key: Sequence[str]) -> Check:
    cols = list(key)

    def fn(df: pd.DataFrame) -> tuple[bool, str]:
        nulls = {c: int(df[c].isna().sum()) for c in cols}
        bad = {c: n for c, n in nulls.items() if n}
        return not bad, f"null_counts={bad}" if bad else "ok"

    return Check("no_null_keys", "error", fn)


def unique_keys(key: Sequence[str]) -> Check:
    cols = list(key)

    def fn(df: pd.DataFrame) -> tuple[bool, str]:
        dup = df.duplicated(subset=cols, keep=False)
        if not dup.any():
            return True, "ok"
        sample = df.loc[dup, cols].drop_duplicates().head(_SAMPLE).to_dict("records")
        return False, f"duplicate_rows={int(dup.sum())} sample={sample}"

    return Check("unique_keys", "error", fn)


def schema_match(schema: TableSchema) -> Check:
    """Đúng hợp đồng bảng; dùng lại `schemas.conform` để chỉ có một bộ luật kiểu dữ liệu."""

    def fn(df: pd.DataFrame) -> tuple[bool, str]:
        try:
            conform(df, schema)
        except SchemaMismatch as exc:
            return False, str(exc)
        return True, "ok"

    return Check("schema_match", "error", fn)


def ts_in_range(column: str, start: datetime, end: datetime) -> Check:
    """Mọi timestamp trong [start, end). Đọc sai đơn vị epoch sẽ ra năm 1970 hoặc năm 50000."""
    lo, hi = pd.Timestamp(start), pd.Timestamp(end)
    if lo.tzinfo is None or hi.tzinfo is None:
        raise ValueError("ts_in_range needs timezone-aware start and end")

    def fn(df: pd.DataFrame) -> tuple[bool, str]:
        ts = df[column]
        outside = (ts < lo) | (ts >= hi)
        if not outside.any():
            return True, "ok"
        bad = ts[outside]
        return False, (
            f"out_of_range={int(outside.sum())} range=[{lo.isoformat()}, {hi.isoformat()}) "
            f"min={bad.min()} max={bad.max()}"
        )

    return Check("ts_in_range", "error", fn)


def ohlc_consistent() -> Check:
    def fn(df: pd.DataFrame) -> tuple[bool, str]:
        body_lo = df[["open", "close"]].min(axis=1)
        body_hi = df[["open", "close"]].max(axis=1)
        bad = (df["low"] > body_lo) | (body_hi > df["high"]) | (df["low"] > df["high"])
        return _rows_result(df, bad)

    return Check("ohlc_consistent", "warn", fn)


def non_negative(columns: Sequence[str]) -> Check:
    cols = list(columns)

    def fn(df: pd.DataFrame) -> tuple[bool, str]:
        return _rows_result(df, (df[cols] < 0).any(axis=1))

    return Check("non_negative", "warn", fn)


def row_count(expected: int) -> Check:
    """Ví dụ crypto 1m: đúng 1440 nến cho một ngày UTC."""
    return Check(
        "row_count", "warn", lambda df: (len(df) == expected, f"rows={len(df)} expected={expected}")
    )


def _rows_result(df: pd.DataFrame, bad: "pd.Series[bool]") -> tuple[bool, str]:
    if not bad.any():
        return True, "ok"
    sample = df.loc[bad].head(_SAMPLE).to_dict("records")
    return False, f"bad_rows={int(bad.sum())} sample={sample}"
