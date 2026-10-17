from datetime import UTC, date, datetime, timedelta, timezone

import pandas as pd
import pytest

from alphadata.core.timeutil import (
    TIMESTAMP_DTYPE,
    epoch_to_utc,
    infer_epoch_unit,
    previous_utc_day,
)

TS = pd.Timestamp("2025-01-01 00:01:00", tz="UTC")
SECONDS = 1735689660


@pytest.mark.parametrize(
    "values", [[SECONDS], [SECONDS * 1_000], [SECONDS * 1_000_000]], ids=["s", "ms", "us"]
)
def test_infers_unit_from_magnitude(values: list[int]) -> None:
    result = epoch_to_utc(values)
    assert result.iloc[0] == TS
    assert result.dtype == TIMESTAMP_DTYPE


def test_explicit_unit() -> None:
    assert epoch_to_utc([SECONDS * 1_000], unit="ms").iloc[0] == TS


def test_mixed_units_rejected() -> None:
    # Binance đổi từ mili-giây sang micro-giây từ 2025-01-01: một lô không được trộn hai đơn vị.
    with pytest.raises(ValueError, match="mixed epoch units"):
        epoch_to_utc([SECONDS * 1_000, SECONDS * 1_000_000])


def test_keeps_series_index_and_nulls() -> None:
    series = pd.Series([SECONDS * 1_000, None], index=[10, 11])
    result = epoch_to_utc(series)
    assert list(result.index) == [10, 11]
    assert result.iloc[0] == TS
    assert pd.isna(result.iloc[1])


def test_empty_input() -> None:
    result = epoch_to_utc([])
    assert result.empty
    assert result.dtype == TIMESTAMP_DTYPE


def test_too_large_value_rejected() -> None:
    with pytest.raises(ValueError):
        infer_epoch_unit(1e18)


def test_previous_utc_day_from_date() -> None:
    assert previous_utc_day(date(2026, 3, 1)) == date(2026, 2, 28)


def test_previous_utc_day_converts_to_utc_first() -> None:
    # 03:00 giờ Việt Nam ngày 12 là 20:00 UTC ngày 11, nên ngày UTC liền trước là ngày 10.
    ict = timezone(timedelta(hours=7))
    assert previous_utc_day(datetime(2026, 10, 12, 3, 0, tzinfo=ict)) == date(2026, 10, 10)


def test_previous_utc_day_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="naive"):
        previous_utc_day(datetime(2026, 10, 12, 3, 0))


def test_previous_utc_day_default_is_yesterday_utc() -> None:
    assert previous_utc_day() == datetime.now(UTC).date() - timedelta(days=1)
