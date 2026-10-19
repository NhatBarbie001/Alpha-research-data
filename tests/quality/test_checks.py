from datetime import UTC, datetime

import pandas as pd
import pytest

from alphadata.quality import (
    Check,
    default_checks,
    no_null_keys,
    non_negative,
    not_empty,
    ohlc_consistent,
    row_count,
    schema_match,
    ts_in_range,
    unique_keys,
)
from alphadata.schemas.silver import CRYPTO_BARS_1M

DAY_START = datetime(2026, 10, 11, tzinfo=UTC)
DAY_END = datetime(2026, 10, 12, tzinfo=UTC)
KEY = list(CRYPTO_BARS_1M.key)


def bars(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": "BTCUSDT",
            "ts_utc": pd.date_range(DAY_START, periods=n, freq="min"),
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 5.0,
            "quote_volume": 505.0,
            "trades": 12,
            "taker_buy_base": 2.0,
            "taker_buy_quote": 202.0,
            "source": "binance",
            "ingested_at": pd.Timestamp("2026-10-12 01:00", tz="UTC"),
        }
    )


def run(check: Check, df: pd.DataFrame) -> tuple[bool, str]:
    return check.fn(df)


def test_not_empty() -> None:
    assert run(not_empty(), bars())[0]
    assert run(not_empty(), bars(0)) == (False, "rows=0")


def test_no_null_keys() -> None:
    assert run(no_null_keys(KEY), bars())[0]
    df = bars()
    df.loc[1, "symbol"] = None
    passed, detail = run(no_null_keys(KEY), df)
    assert not passed
    assert detail == "null_counts={'symbol': 1}"


def test_unique_keys() -> None:
    assert run(unique_keys(KEY), bars())[0]
    df = pd.concat([bars(2), bars(1)], ignore_index=True)
    passed, detail = run(unique_keys(KEY), df)
    assert not passed
    assert detail.startswith("duplicate_rows=2")


def test_schema_match() -> None:
    assert run(schema_match(CRYPTO_BARS_1M), bars())[0]
    passed, detail = run(schema_match(CRYPTO_BARS_1M), bars().assign(extra=1))
    assert not passed
    assert "unexpected columns ['extra']" in detail
    passed, detail = run(schema_match(CRYPTO_BARS_1M), bars().assign(trades=1.5))
    assert not passed
    assert "non-integral" in detail


def test_ts_in_range() -> None:
    check = ts_in_range("ts_utc", DAY_START, DAY_END)
    assert run(check, bars())[0]

    # Epoch micro-giây bị đọc như mili-giây ra năm ~57000; đọc như giây ra năm 1970.
    misread = bars().assign(ts_utc=pd.to_datetime([1_760_140_800] * 3, unit="ms", utc=True))
    passed, detail = run(check, misread)
    assert not passed
    assert detail.startswith("out_of_range=3")

    next_day = bars(1).assign(ts_utc=pd.Timestamp(DAY_END))
    assert not run(check, next_day)[0]  # đầu cuối không tính


def test_ts_in_range_requires_aware_bounds() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ts_in_range("ts_utc", datetime(2026, 10, 11), DAY_END)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda df: df.assign(low=100.5),  # low > open
        lambda df: df.assign(high=100.5),  # high < close
        lambda df: df.assign(low=103.0, high=102.0, open=102.5, close=102.5),  # low > high
    ],
    ids=["low-above-open", "high-below-close", "low-above-high"],
)
def test_ohlc_consistent(mutate: object) -> None:
    assert run(ohlc_consistent(), bars())[0]
    passed, detail = run(ohlc_consistent(), mutate(bars()))  # type: ignore[operator]
    assert not passed
    assert detail.startswith("bad_rows=3")


def test_non_negative() -> None:
    cols = ["open", "high", "low", "close", "volume"]
    assert run(non_negative(cols), bars())[0]
    df = bars()
    df.loc[0, "volume"] = -1.0
    passed, detail = run(non_negative(cols), df)
    assert not passed
    assert detail.startswith("bad_rows=1")


def test_row_count() -> None:
    assert run(row_count(1440), bars(1440))[0]
    assert run(row_count(1440), bars(1439)) == (False, "rows=1439 expected=1440")


def test_default_checks() -> None:
    checks = default_checks(CRYPTO_BARS_1M)
    assert [c.name for c in checks] == ["not_empty", "no_null_keys", "unique_keys", "schema_match"]
    assert all(c.severity == "error" for c in checks)
    assert all(c.fn(bars())[0] for c in checks)


def test_severities_match_spec() -> None:
    expected = {
        "ts_in_range": "error",
        "ohlc_consistent": "warn",
        "non_negative": "warn",
        "row_count": "warn",
    }
    checks = [
        ts_in_range("ts_utc", DAY_START, DAY_END),
        ohlc_consistent(),
        non_negative([]),
        row_count(1),
    ]
    assert {c.name: c.severity for c in checks} == expected
