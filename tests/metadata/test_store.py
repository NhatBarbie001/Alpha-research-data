import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
import sqlalchemy as sa

from alphadata.metadata import CatalogEntry, MetaStore
from alphadata.metadata.store import ERROR_MAX_CHARS
from alphadata.storage.raw import save_raw

PARTITION = {"symbol": "BTCUSDT", "date": date(2026, 10, 11)}


@dataclass(frozen=True)
class Check:
    name: str
    severity: str
    passed: bool
    detail: str = ""


def catalog_entry(**overrides: object) -> CatalogEntry:
    values: dict[str, object] = {
        "table_name": "crypto_bars_1m",
        "source": "binance",
        "layer": "silver",
        "dataset": "binance_spot_klines_1m",
        "description": "Nến 1 phút spot",
        "source_url": "https://data.binance.vision",
        "license_note": "Public data",
        "key_cols": "symbol,ts_utc",
        "path_pattern": "silver/crypto_bars_1m/symbol=*/year=*/month=*/part.parquet",
        "refresh_schedule": "0 1 * * *",
        "schema_version": 1,
    }
    values.update(overrides)
    return CatalogEntry(**values)  # type: ignore[arg-type]


def test_full_run_lifecycle(store: MetaStore, tmp_path: Path) -> None:
    run_id = store.start_run(
        "binance",
        "binance_spot_klines_1m",
        PARTITION,
        "airflow",
        airflow_run_id="scheduled__2026-10-12",
    )
    run = store.get_run(run_id)
    assert run.status == "running"
    assert run.partition == {"symbol": "BTCUSDT", "date": "2026-10-11"}
    assert run.started_at.tzinfo is not None

    raw = save_raw(tmp_path / "raw", b"csv-bytes", "BTCUSDT-1m.zip", "https://x/BTCUSDT-1m.zip")
    raw_id = store.record_raw(run_id, raw)
    assert raw_id > 0

    store.record_dq(
        "crypto_bars_1m",
        [Check("not_empty", "error", True), Check("row_count", "warn", False, "rows=1439")],
        run_id=run_id,
    )
    store.upsert_catalog(catalog_entry())
    store.finish_run(run_id, "success", rows_in=1440, rows_out=44640, silver_path="silver/x")
    assert store.mark_raw_deleted(run_id) == 1

    run = store.get_run(run_id)
    assert (run.status, run.rows_in, run.rows_out) == ("success", 1440, 44640)
    assert run.finished_at is not None and run.finished_at >= run.started_at
    [raw_row] = store.raw_files_for(run_id)
    assert raw_row["deleted"] is True
    assert raw_row["sha256"] == raw.sha256
    [failed] = store.failed_checks(since=datetime.now(UTC) - timedelta(minutes=5))
    assert (failed.check_name, failed.detail, failed.run_id) == ("row_count", "rows=1439", run_id)
    assert store.catalog("silver")[0].table_name == "crypto_bars_1m"
    assert store.last_success("binance_spot_klines_1m", PARTITION) == run.finished_at


def test_failed_run_error_masked_and_truncated(store: MetaStore) -> None:
    run_id = store.start_run("fred", "fred_series", {"series_id": "CPIAUCSL"}, "cli")
    message = "HTTP 500 for https://api/x?api_key=SECRET123 " + "x" * 10_000
    store.finish_run(run_id, "failed", error=message)
    run = store.get_run(run_id)
    assert run.status == "failed"
    assert run.error is not None
    assert "SECRET123" not in run.error
    assert len(run.error) == ERROR_MAX_CHARS
    assert store.last_success("fred_series", {"series_id": "CPIAUCSL"}) is None


def test_raw_url_masked_even_if_caller_forgot(store: MetaStore, tmp_path: Path) -> None:
    run_id = store.start_run("fred", "fred_series", {"series_id": "GDP"}, "cli")
    raw = save_raw(tmp_path, b"{}", "gdp.json", "https://api/x?series_id=GDP")
    store.record_raw(run_id, raw.__class__(**{**raw.__dict__, "url": "https://api/x?api_key=K1"}))
    assert store.raw_files_for(run_id)[0]["url"] == "https://api/x?api_key=***"


def test_last_success_matches_exact_partition(store: MetaStore) -> None:
    run_id = store.start_run("binance", "d", {"date": "2026-10-11", "symbol": "BTCUSDT"}, "cli")
    store.finish_run(run_id, "success")
    # Thứ tự key và kiểu date/str không ảnh hưởng.
    assert store.last_success("d", PARTITION) is not None
    assert store.last_success("d", {"symbol": "ETHUSDT", "date": date(2026, 10, 11)}) is None
    assert store.last_success("other", PARTITION) is None


def test_recent_runs_filters(store: MetaStore) -> None:
    ids = [store.start_run("s", "a" if i % 2 else "b", {"i": i}, "cli") for i in range(5)]
    store.finish_run(ids[0], "failed", error="boom")
    assert [r.run_id for r in store.recent_runs(limit=2)] == [ids[4], ids[3]]
    assert {r.dataset for r in store.recent_runs(dataset="a")} == {"a"}
    assert [r.run_id for r in store.recent_runs(status="failed")] == [ids[0]]


def test_catalog_upsert_updates_in_place(store: MetaStore) -> None:
    store.upsert_catalog(catalog_entry())
    store.upsert_catalog(catalog_entry(description="Mô tả mới", schema_version=2))
    store.upsert_catalog(catalog_entry(source="other"))
    entries = store.catalog()
    assert len(entries) == 2
    updated = next(e for e in entries if e.source == "binance")
    assert (updated.description, updated.schema_version) == ("Mô tả mới", 2)
    assert updated.updated_at is not None
    assert store.catalog("gold") == []


def test_failed_checks_since(store: MetaStore) -> None:
    store.record_dq("t", [Check("freshness", "warn", False)])
    assert store.failed_checks(since=datetime.now(UTC) + timedelta(minutes=1)) == []
    assert store.failed_checks(since=datetime.now(UTC) - timedelta(minutes=1))[0].run_id is None
    assert store.record_dq("t", []) == 0


@pytest.mark.parametrize(
    "call",
    [
        lambda s: s.start_run("s", "d", {}, "manual"),
        lambda s: s.finish_run(uuid.uuid4(), "running"),
        lambda s: s.record_dq("t", [Check("c", "fatal", False)]),
        lambda s: s.upsert_catalog(catalog_entry(layer="bronze")),
    ],
    ids=["trigger", "status", "severity", "layer"],
)
def test_invalid_enum_values_rejected(store: MetaStore, call: object) -> None:
    with pytest.raises(ValueError):
        call(store)  # type: ignore[operator]


def test_unknown_run_rejected(store: MetaStore, tmp_path: Path) -> None:
    with pytest.raises(KeyError):
        store.finish_run(uuid.uuid4(), "success")
    with pytest.raises(KeyError):
        store.get_run(uuid.uuid4())
    raw = save_raw(tmp_path, b"x", "f.bin", "u")
    with pytest.raises(sa.exc.IntegrityError):
        store.record_raw(uuid.uuid4(), raw)


def test_reopen_existing_database(db_url: str) -> None:
    first = MetaStore(db_url)
    run_id = first.start_run("s", "d", {}, "cli")
    first.close()
    second = MetaStore(db_url)
    assert second.get_run(run_id).status == "running"
    second.close()
