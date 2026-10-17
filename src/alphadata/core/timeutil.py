from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Literal

import numpy as np
import pandas as pd

TIMESTAMP_DTYPE = pd.DatetimeTZDtype(unit="us", tz="UTC")

EpochUnit = Literal["s", "ms", "us"]

# Suy đơn vị theo độ lớn chỉ đúng với mốc sau 1973-03 (mili-giây >= 1e11) và trước năm ~5138.
_UNIT_LIMITS: tuple[tuple[EpochUnit, float], ...] = (("s", 1e11), ("ms", 1e14), ("us", 1e17))


def infer_epoch_unit(value: float) -> EpochUnit:
    magnitude = abs(value)
    for unit, limit in _UNIT_LIMITS:
        if magnitude < limit:
            return unit
    raise ValueError(f"epoch value too large to be s/ms/us: {value}")


def epoch_to_utc(
    values: pd.Series | Sequence[float] | np.ndarray, unit: EpochUnit | None = None
) -> pd.Series:
    """Epoch sang timestamp UTC. Không truyền `unit` thì suy theo độ lớn; trộn đơn vị thì raise."""
    series = values if isinstance(values, pd.Series) else pd.Series(values)
    if unit is None:
        non_null = series.dropna()
        if non_null.empty:
            unit = "s"
        else:
            lo = infer_epoch_unit(float(non_null.abs().min()))
            hi = infer_epoch_unit(float(non_null.abs().max()))
            if lo != hi:
                raise ValueError(f"mixed epoch units in one batch: {lo} and {hi}")
            unit = lo
    return pd.to_datetime(series, unit=unit, utc=True).astype(TIMESTAMP_DTYPE)


def previous_utc_day(ref: date | datetime | None = None) -> date:
    if ref is None:
        ref = datetime.now(UTC)
    if isinstance(ref, datetime):
        if ref.tzinfo is None:
            raise ValueError("naive datetime is ambiguous; pass a timezone-aware datetime")
        ref = ref.astimezone(UTC).date()
    return ref - timedelta(days=1)
