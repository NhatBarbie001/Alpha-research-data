from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from alphadata.core.errors import DataQualityError
from alphadata.metadata import MetaStore
from alphadata.quality import Check, default_checks, ohlc_consistent, run_checks
from alphadata.schemas import conform
from alphadata.schemas.silver import CRYPTO_BARS_1M
from alphadata.storage.parquet import write_parquet_atomic

SINCE = datetime.now(UTC) - timedelta(minutes=5)


def bars(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": "BTCUSDT",
            "ts_utc": pd.date_range("2026-10-11", periods=n, freq="min", tz="UTC"),
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


@pytest.fixture
def meta(tmp_path: Path) -> Iterator[MetaStore]:
    store = MetaStore(f"sqlite:///{(tmp_path / 'meta.db').as_posix()}")
    yield store
    store.close()


def test_all_pass_recorded(meta: MetaStore) -> None:
    run_id = meta.start_run("binance", "binance_spot_klines_1m", {"d": "2026-10-11"}, "cli")
    results = run_checks(
        bars(),
        default_checks(CRYPTO_BARS_1M),
        table_name="crypto_bars_1m",
        meta=meta,
        run_id=run_id,
    )
    assert all(r.passed for r in results)
    with meta.engine.connect() as conn:
        count = conn.exec_driver_sql("SELECT count(*) FROM dq_results").scalar_one()
    assert count == 4
    assert meta.failed_checks(SINCE) == []


def test_error_failure_raises_after_recording(meta: MetaStore) -> None:
    dup = pd.concat([bars(2), bars(1)], ignore_index=True)
    with pytest.raises(DataQualityError, match="unique_keys"):
        run_checks(dup, default_checks(CRYPTO_BARS_1M), table_name="crypto_bars_1m", meta=meta)
    [failed] = meta.failed_checks(SINCE)
    assert (failed.check_name, failed.severity) == ("unique_keys", "error")


def test_warn_failure_does_not_raise(meta: MetaStore) -> None:
    results = run_checks(bars().assign(low=100.5), [ohlc_consistent()], table_name="t", meta=meta)
    assert [(r.name, r.passed) for r in results] == [("ohlc_consistent", False)]
    assert meta.failed_checks(SINCE)[0].severity == "warn"


def test_crashing_check_becomes_failure_at_its_severity() -> None:
    missing_low = bars().drop(columns=["low"])
    [result] = run_checks(missing_low, [ohlc_consistent()], table_name="t")
    assert not result.passed
    assert result.detail.startswith("check raised KeyError")

    crash = Check("custom", "error", lambda df: (bool(df["nope"].any()), ""))
    with pytest.raises(DataQualityError, match="custom: check raised KeyError"):
        run_checks(bars(), [crash], table_name="t")


def test_detail_truncated_and_names_unique() -> None:
    noisy = Check("noisy", "warn", lambda df: (False, "x" * 10_000))
    [result] = run_checks(bars(), [noisy], table_name="t")
    assert len(result.detail) == 500
    with pytest.raises(ValueError, match="duplicate"):
        run_checks(bars(), [noisy, noisy], table_name="t")


def test_error_failure_leaves_silver_unchanged(tmp_path: Path) -> None:
    path = tmp_path / "part.parquet"
    schema = CRYPTO_BARS_1M.arrow_schema()
    write_parquet_atomic(conform(bars(), CRYPTO_BARS_1M), path, schema=schema)
    before = path.read_bytes()

    bad = bars().assign(extra="x")  # sai hợp đồng: schema_match (error) fail
    with pytest.raises(DataQualityError):
        run_checks(bad, default_checks(CRYPTO_BARS_1M), table_name="crypto_bars_1m")
        write_parquet_atomic(bad, path, schema=schema)  # không bao giờ tới đây
    assert path.read_bytes() == before
