"""Kiểm tra chất lượng dữ liệu trước khi ghi vào silver.

severity="error": chặn không cho ghi. severity="warn": vẫn ghi, nhưng lưu lại để theo dõi.
Không bao giờ âm thầm xoá dòng lỗi.
"""

from dataclasses import dataclass

import pandas as pd


@dataclass
class CheckResult:
    name: str
    severity: str
    passed: bool
    detail: str = ""


def not_empty(df: pd.DataFrame) -> CheckResult:
    return CheckResult("not_empty", "error", len(df) > 0, f"rows={len(df)}")


def no_null_keys(df: pd.DataFrame, keys: list[str]) -> CheckResult:
    n = int(df[keys].isna().any(axis=1).sum())
    return CheckResult("no_null_keys", "error", n == 0, f"null_key_rows={n}")


def unique_keys(df: pd.DataFrame, keys: list[str]) -> CheckResult:
    n = int(df.duplicated(subset=keys).sum())
    return CheckResult("unique_keys", "error", n == 0, f"duplicate_rows={n}")


def ohlc_consistent(df: pd.DataFrame) -> CheckResult:
    lo = df[["open", "close"]].min(axis=1)
    hi = df[["open", "close"]].max(axis=1)
    bad = (df["low"] > lo) | (df["high"] < hi) | (df["low"] > df["high"])
    n = int(bad.sum())
    return CheckResult("ohlc_consistent", "warn", n == 0, f"bad_rows={n}")


def non_negative(df: pd.DataFrame, cols: list[str]) -> CheckResult:
    n = int((df[cols] < 0).any(axis=1).sum())
    return CheckResult("non_negative", "warn", n == 0, f"negative_rows={n} cols={cols}")


def ts_in_range(df: pd.DataFrame, col: str, start: pd.Timestamp, end: pd.Timestamp) -> CheckResult:
    """Bắt lỗi parse timestamp sai đơn vị (ví dụ mili-giây vs micro-giây)."""
    bad = (df[col] < start) | (df[col] >= end)
    n = int(bad.sum())
    return CheckResult("ts_in_range", "error", n == 0, f"out_of_range={n} expected=[{start}, {end})")


def row_count(df: pd.DataFrame, expected: int) -> CheckResult:
    return CheckResult("row_count", "warn", len(df) == expected, f"rows={len(df)} expected={expected}")


def has_errors(results: list[CheckResult]) -> bool:
    return any(r.severity == "error" and not r.passed for r in results)
