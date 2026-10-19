import socket
import threading
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
import pytest

from alphadata.core.errors import (
    DataQualityError,
    DiskFullError,
    MissingCredential,
    NotAvailableYet,
)
from alphadata.core.settings import Settings
from alphadata.ingestion.pipeline import RawUnavailable, reprocess_partition, run_partition
from alphadata.metadata import MetaStore
from alphadata.storage.raw import manifest_path, save_raw

MINUTES_PER_DAY = 1440

DAY = "2026-10-11"
PART = {"symbol": "BTCUSDT", "date": DAY}
SINCE = datetime.now(UTC) - timedelta(minutes=5)


def silver_file(settings: Settings, month: int = 10) -> Path:
    return (
        settings.data_root
        / f"silver/crypto_bars_1m/symbol=BTCUSDT/year=2026/month={month}/part.parquet"
    )


def run(dataset: Any, settings: Settings, meta: MetaStore, **part: Any) -> Any:
    return run_partition(dataset, {**PART, **part}, settings=settings, meta=meta)


def test_all_ten_steps(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    result = run(dataset, settings, meta)

    # Kết quả và silver.
    assert (result.status, result.rows_in, result.rows_out) == ("success", 1440, 1440)
    assert result.silver_paths == [silver_file(settings)]
    table = pq.read_table(silver_file(settings))
    assert table.num_rows == MINUTES_PER_DAY
    assert set(table.column("source").to_pylist()) == {"fake"}

    # Raw + manifest, URL đã che key, nằm đúng raw/<source>/<dataset>/<k=v>/<fetch_date>/.
    [raw] = result.raw_files
    assert raw.path.is_file() and manifest_path(raw.path).is_file()
    assert (
        raw.path.parent.parent
        == settings.data_root / "raw/fake/fake_klines/symbol=BTCUSDT/date=2026-10-11"
    )
    assert "SECRET123" not in manifest_path(raw.path).read_text(encoding="utf-8")

    # Metadata: run, raw_files (đường dẫn tương đối với lake), dq_results, catalog.
    rec = meta.get_run(result.run_id)
    assert (rec.status, rec.trigger, rec.rows_in, rec.rows_out) == ("success", "cli", 1440, 1440)
    assert rec.silver_path == "silver/crypto_bars_1m/symbol=BTCUSDT/year=2026/month=10/part.parquet"
    [raw_row] = meta.raw_files_for(result.run_id)
    assert raw_row["path"].startswith("raw/fake/fake_klines/")
    assert raw_row["deleted"] is False
    assert [c.name for c in result.checks] == [
        "not_empty",
        "no_null_keys",
        "unique_keys",
        "schema_match",
        "ts_in_range",
        "row_count",
        "ohlc_consistent",
    ]
    assert meta.failed_checks(SINCE) == []
    [entry] = meta.catalog("silver")
    assert (entry.table_name, entry.source, entry.dataset) == (
        "crypto_bars_1m",
        "fake",
        "fake_klines",
    )
    assert entry.path_pattern == "silver/crypto_bars_1m/symbol=*/year=*/month=*/part.parquet"

    # ingested_at = fetched_at của raw (để chạy lại từ raw cho cùng một file).
    ingested = table.column("ingested_at").to_pylist()[0]
    assert ingested == datetime.fromisoformat(raw.fetched_at)


def test_rerun_same_partition_is_idempotent(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    run(dataset, settings, meta)
    first = pq.read_table(silver_file(settings)).drop(["ingested_at"])
    second_result = run(dataset, settings, meta)
    second = pq.read_table(silver_file(settings)).drop(["ingested_at"])
    assert second.equals(first)
    assert second_result.rows_out == MINUTES_PER_DAY


def test_next_day_merges_into_month_file(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    write_day("BTCUSDT", "2026-10-12")
    run(dataset, settings, meta)
    result = run(dataset, settings, meta, date="2026-10-12")
    assert (result.rows_in, result.rows_out) == (1440, 2880)
    ts = pq.read_table(silver_file(settings)).column("ts_utc").to_pandas()
    assert ts.is_monotonic_increasing and ts.is_unique


def test_rows_spanning_two_months_go_to_two_files(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", "2026-09-30")
    dataset.normalize_hook = lambda df: df.assign(ts_utc=df["ts_utc"] + pd.Timedelta(hours=12))
    dataset.checks = lambda partition: []
    result = run(dataset, settings, meta, date="2026-09-30")
    assert sorted(result.silver_paths) == sorted(
        [silver_file(settings, 9), silver_file(settings, 10)]
    )
    assert pq.read_table(silver_file(settings, 9)).num_rows == 720
    assert pq.read_table(silver_file(settings, 10)).num_rows == 720
    assert result.rows_out == 1440


def test_reprocess_from_raw_offline_gives_identical_silver(
    dataset: Any,
    settings: Settings,
    meta: MetaStore,
    write_day: Callable[..., Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_day("BTCUSDT", DAY)
    run(dataset, settings, meta)
    original = pq.read_table(silver_file(settings))
    silver_file(settings).unlink()

    def no_network(*args: object, **kwargs: object) -> None:
        raise AssertionError("network access during reprocess")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    calls_before = dataset.fetch_calls
    result = reprocess_partition(dataset, PART, settings=settings, meta=meta)

    assert dataset.fetch_calls == calls_before
    assert pq.read_table(silver_file(settings)).equals(original)
    assert result.raw_files[0].sha256 == meta.raw_files_for(result.run_id)[0]["sha256"]
    assert meta.get_run(result.run_id).status == "success"


def test_reprocess_needs_kept_raw(
    write_day: Callable[..., Path], settings: Settings, meta: MetaStore, fake_cls: Any, remote: Path
) -> None:
    dataset = fake_cls(remote, keep_raw=False)
    write_day("BTCUSDT", DAY)
    result = run(dataset, settings, meta)
    assert not result.raw_files[0].path.exists()
    assert manifest_path(result.raw_files[0].path).exists()
    assert meta.raw_files_for(result.run_id)[0]["deleted"] is True
    with pytest.raises(RawUnavailable, match="deleted"):
        reprocess_partition(dataset, PART, settings=settings, meta=meta)
    with pytest.raises(RawUnavailable, match="no successful run"):
        reprocess_partition(
            dataset, {"symbol": "ETHUSDT", "date": DAY}, settings=settings, meta=meta
        )


def test_reprocess_detects_corrupted_raw(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    raw = run(dataset, settings, meta).raw_files[0]
    raw.path.write_bytes(b"tampered")
    with pytest.raises(Exception, match="sha256"):
        reprocess_partition(dataset, PART, settings=settings, meta=meta)
    assert meta.recent_runs(limit=1)[0].status == "failed"


def test_error_check_blocks_write_and_is_recorded(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    run(dataset, settings, meta)
    before = silver_file(settings).read_bytes()

    write_day("BTCUSDT", DAY)
    dataset.normalize_hook = lambda df: pd.concat([df, df.head(1)], ignore_index=True)
    with pytest.raises(DataQualityError, match="unique_keys"):
        run(dataset, settings, meta)
    assert silver_file(settings).read_bytes() == before
    last = meta.recent_runs(limit=1)[0]
    assert last.status == "failed"
    assert last.error is not None and last.error.startswith("DataQualityError")
    assert {c.check_name for c in meta.failed_checks(SINCE)} >= {"unique_keys"}


def test_epoch_misread_caught_by_ts_in_range(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    dataset.normalize_hook = lambda df: df.assign(
        # Giá trị mili-giây bị đọc như micro-giây: ra tháng 1/1970.
        ts_utc=pd.to_datetime(df["ts_utc"].astype("int64") // 1000, unit="us", utc=True)
    )
    with pytest.raises(DataQualityError, match="ts_in_range"):
        run(dataset, settings, meta)
    assert not silver_file(settings).exists()


def test_not_available_yet_is_skipped(dataset: Any, settings: Settings, meta: MetaStore) -> None:
    with pytest.raises(NotAvailableYet):
        run(dataset, settings, meta)
    last = meta.recent_runs(limit=1)[0]
    assert last.status == "skipped_not_available"


def test_unexpected_error_recorded_with_key_masked(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)

    def boom(df: pd.DataFrame) -> pd.DataFrame:
        raise RuntimeError("parser failed on https://x?api_key=SECRET123")

    dataset.normalize_hook = boom
    with pytest.raises(RuntimeError):
        run(dataset, settings, meta)
    last = meta.recent_runs(limit=1)[0]
    assert last.status == "failed"
    assert last.error == "RuntimeError: parser failed on https://x?api_key=***"


def test_normalize_must_not_set_layer_columns(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    dataset.normalize_hook = lambda df: df.assign(source="wrong")
    with pytest.raises(ValueError, match="must not set"):
        run(dataset, settings, meta)


def test_preflight_failures_record_no_run(
    dataset: Any, settings: Settings, meta: MetaStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ValueError, match="missing=\\['date'\\]"):
        run_partition(dataset, {"symbol": "BTCUSDT"}, settings=settings, meta=meta)
    with pytest.raises(ValueError, match="extra=\\['x'\\]"):
        run_partition(dataset, {**PART, "x": "1"}, settings=settings, meta=meta)
    with pytest.raises(ValueError, match="empty"):
        run_partition(dataset, {"symbol": " ", "date": DAY}, settings=settings, meta=meta)

    monkeypatch.setattr(type(dataset), "requires", ("FRED_API_KEY",))
    with pytest.raises(MissingCredential):
        run(dataset, settings, meta)
    monkeypatch.setattr(type(dataset), "requires", ())

    full = Settings(data_root=settings.data_root, meta_db_url=settings.meta_db_url, min_free_gb=1e9)
    with pytest.raises(DiskFullError):
        run(dataset, full, meta)
    assert meta.recent_runs() == []


def test_partition_values_normalised(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    result = run_partition(
        dataset, {"date": date(2026, 10, 11), "symbol": "BTCUSDT"}, settings=settings, meta=meta
    )
    assert result.partition == PART
    assert meta.last_success("fake_klines", PART) is not None


def test_concurrent_days_into_same_month_file_lose_nothing(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    days = [f"2026-10-{d:02d}" for d in range(1, 7)]
    for day in days:
        write_day("BTCUSDT", day)
    errors: list[BaseException] = []

    def worker(day: str) -> None:
        try:
            run(dataset, settings, meta, date=day)
        except BaseException as exc:  # pragma: no cover - chỉ chạy khi test fail
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(d,)) for d in days]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert pq.read_table(silver_file(settings)).num_rows == MINUTES_PER_DAY * len(days)


def test_fetch_must_return_raw_inside_lake(
    dataset: Any,
    settings: Settings,
    meta: MetaStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dataset, "fetch", lambda ctx, partition: [])
    with pytest.raises(ValueError, match="no raw files"):
        run(dataset, settings, meta)

    outside = save_raw(tmp_path / "elsewhere", b"x", "f.csv", "u")
    monkeypatch.setattr(dataset, "fetch", lambda ctx, partition: [outside])
    with pytest.raises(ValueError, match="outside data_root"):
        run(dataset, settings, meta)
    assert [r.status for r in meta.recent_runs()] == ["failed", "failed"]


def test_reprocess_reports_raw_missing_on_disk(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    run(dataset, settings, meta).raw_files[0].path.unlink()
    with pytest.raises(RawUnavailable, match="missing on disk"):
        reprocess_partition(dataset, PART, settings=settings, meta=meta)


def test_metadata_failure_does_not_hide_original_error(
    dataset: Any,
    settings: Settings,
    meta: MetaStore,
    write_day: Callable[..., Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    write_day("BTCUSDT", DAY)
    dataset.normalize_hook = lambda df: df.assign(source="wrong")

    def broken_finish(*args: object, **kwargs: object) -> None:
        raise ConnectionError("metadata DB down")

    monkeypatch.setattr(meta, "finish_run", broken_finish)
    with pytest.raises(ValueError, match="must not set"):
        run(dataset, settings, meta)
    assert "could not record status failed" in caplog.text


def test_opens_and_closes_its_own_metastore(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    write_day("BTCUSDT", DAY)
    result = run_partition(dataset, PART, settings=settings)
    assert meta.get_run(result.run_id).status == "success"
