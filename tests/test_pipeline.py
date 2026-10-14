import pandas as pd
import pytest

from alphadata.meta import MetaStore
from alphadata.pipeline import DataQualityError, run_partition
from alphadata.sources.binance import BinanceSpotKlines1m
from alphadata.storage import DiskFullError, save_raw, upsert
from tests.conftest import binance_zip

DAY = int(pd.Timestamp("2026-10-11", tz="UTC").timestamp() * 1000)


@pytest.fixture
def fake_binance(monkeypatch):
    """Thay fetch bằng file giả để test không cần mạng. Có thể chỉnh ngày bắt đầu của dữ liệu."""
    state = {"start_ms": DAY}

    def fetch(self, ctx, partition):
        content = binance_zip(state["start_ms"], 1440, "us")
        return [save_raw(ctx.raw_dir(self, partition), content, "f.zip", "http://test")]

    monkeypatch.setattr(BinanceSpotKlines1m, "fetch", fetch)
    return state


def test_success_writes_silver_logs_run_and_deletes_raw(settings, fake_binance):
    res = run_partition("binance_spot_klines_1m", {"symbol": "BTCUSDT", "date": "2026-10-11"}, settings)

    assert res.rows_out == 1440 and res.silver_path.exists()
    assert not res.warnings
    assert list(settings.raw_dir.rglob("*.zip")) == []             # raw minute bị xoá
    assert len(list(settings.raw_dir.rglob("*.manifest.json"))) == 1  # manifest vẫn còn

    meta = MetaStore(settings.meta_db_url)
    assert meta.recent_runs()[0]["status"] == "success"
    assert meta.catalog()[0]["table_name"] == "crypto_bars_1m"


def test_rerun_is_idempotent(settings, fake_binance):
    p = {"symbol": "BTCUSDT", "date": "2026-10-11"}
    run_partition("binance_spot_klines_1m", p, settings)
    res = run_partition("binance_spot_klines_1m", p, settings)
    assert res.rows_out == 1440


def test_dq_error_blocks_write(settings, fake_binance):
    fake_binance["start_ms"] = DAY + 86_400_000  # dữ liệu ngày 12 nhưng partition là ngày 11
    with pytest.raises(DataQualityError):
        run_partition("binance_spot_klines_1m", {"symbol": "BTCUSDT", "date": "2026-10-11"}, settings)

    assert not list(settings.silver_dir.rglob("*.parquet"))
    run = MetaStore(settings.meta_db_url).recent_runs()[0]
    assert run["status"] == "failed" and "ts_in_range" in run["error"]


def test_disk_guard(settings, fake_binance):
    full = settings.__class__(**{**settings.__dict__, "min_free_gb": 10**9})
    with pytest.raises(DiskFullError):
        run_partition("binance_spot_klines_1m", {"symbol": "BTCUSDT", "date": "2026-10-11"}, full)


def test_bad_partition_rejected(settings):
    with pytest.raises(ValueError):
        run_partition("binance_spot_klines_1m", {"symbol": "BTCUSDT"}, settings)


def test_upsert_keeps_newest_on_conflict():
    old = pd.DataFrame({"k": [1, 2], "v": ["a", "b"]})
    new = pd.DataFrame({"k": [2, 3], "v": ["B", "c"]})
    assert upsert(old, new, ["k"])["v"].tolist() == ["a", "B", "c"]
