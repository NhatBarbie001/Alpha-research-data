import uuid
from collections.abc import Sequence

import pandas as pd

from alphadata.core.errors import DataQualityError
from alphadata.metadata import MetaStore
from alphadata.quality.checks import DETAIL_MAX_CHARS, Check, CheckResult


def run_checks(
    df: pd.DataFrame,
    checks: Sequence[Check],
    *,
    table_name: str,
    meta: MetaStore | None = None,
    run_id: uuid.UUID | None = None,
) -> list[CheckResult]:
    """Chạy mọi check, ghi dq_results, rồi raise DataQualityError nếu có check `error` fail.

    Ghi kết quả trước khi raise để lỗi luôn thấy được trong báo cáo DQ. Check tự lỗi (ví dụ thiếu
    cột) được tính là fail ở đúng mức của nó, không làm hỏng các check còn lại.
    """
    names = [c.name for c in checks]
    if len(names) != len(set(names)):
        raise ValueError(f"duplicate check names: {names}")
    results = [_run_one(df, check) for check in checks]
    if meta is not None:
        meta.record_dq(table_name, results, run_id=run_id)
    failed = [r for r in results if r.severity == "error" and not r.passed]
    if failed:
        summary = "; ".join(f"{r.name}: {r.detail}" for r in failed)
        raise DataQualityError(f"{table_name}: {summary}"[: DETAIL_MAX_CHARS * 2])
    return results


def _run_one(df: pd.DataFrame, check: Check) -> CheckResult:
    try:
        passed, detail = check.fn(df)
    # Bắt mọi lỗi: check hỏng phải thành kết quả fail ghi vào dq_results, không làm crash cả run.
    except Exception as exc:
        passed, detail = False, f"check raised {type(exc).__name__}: {exc}"
    return CheckResult(check.name, check.severity, bool(passed), detail[:DETAIL_MAX_CHARS])
