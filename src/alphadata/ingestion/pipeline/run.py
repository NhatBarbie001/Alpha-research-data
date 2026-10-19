"""Điều phối một lần chạy của một partition. CLI và Airflow đều gọi vào đây."""

import logging
import uuid
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
from filelock import FileLock

from alphadata.core.errors import AlphadataError, NotAvailableYet
from alphadata.core.http import TimeoutSession, make_session
from alphadata.core.settings import Settings, get_settings
from alphadata.ingestion.pipeline.dataset import Dataset, FetchContext, Partition
from alphadata.ingestion.pipeline.registry import get_dataset
from alphadata.metadata import CatalogEntry, MetaStore
from alphadata.quality import CheckResult, run_checks
from alphadata.schemas import conform, split_partitions
from alphadata.storage import layout
from alphadata.storage.disk import check_disk
from alphadata.storage.parquet import read_parquet_if_exists, write_parquet_atomic
from alphadata.storage.raw import RawFile, delete_raw, read_manifest

logger = logging.getLogger(__name__)

LOCK_TIMEOUT_SECONDS = 600
LAYER_COLUMNS = ("source", "ingested_at")


class RawUnavailable(AlphadataError):
    """Không chạy lại từ raw được: chưa có lần chạy thành công, hoặc raw đã bị xoá."""


@dataclass(frozen=True)
class RunResult:
    run_id: uuid.UUID
    dataset: str
    partition: Partition
    status: str
    rows_in: int  # số dòng mới của lần chạy
    rows_out: int  # tổng số dòng các file silver đã ghi, sau merge
    silver_paths: list[Path]
    raw_files: list[RawFile]
    checks: list[CheckResult]


def run_partition(
    dataset: str | Dataset,
    partition: Mapping[str, object],
    *,
    trigger: str = "cli",
    settings: Settings | None = None,
    meta: MetaStore | None = None,
    session: TimeoutSession | None = None,
    airflow_run_id: str | None = None,
    backfill_id: uuid.UUID | None = None,
) -> RunResult:
    """Tải, chuẩn hoá, kiểm tra, ghi silver cho một partition (10 bước, xem spec pipeline)."""
    # 1. Dataset, partition, secret. 2. Disk. Lỗi ở đây chưa có run nào được ghi.
    ds = _resolve(dataset)
    part = validate_partition(ds, partition)
    settings = settings or get_settings()
    for name in ds.requires:
        settings.secret(name)
    check_disk(settings)

    with ExitStack() as stack:
        store = meta or _owned(stack, MetaStore.from_settings(settings))
        http = session or stack.enter_context(make_session(settings))
        # 3. Bắt đầu run.
        run_id = store.start_run(
            ds.source,
            ds.name,
            part,
            trigger,
            airflow_run_id=airflow_run_id,
            backfill_id=backfill_id,
        )

        def body() -> RunResult:
            # 4. Tải raw.
            run_date = datetime.now(UTC).date()
            ctx = FetchContext(
                settings=settings,
                session=http,
                run_date=run_date,
                raw_dir=layout.raw_dir(
                    settings,
                    ds.source,
                    ds.name,
                    part,
                    partition_keys=ds.partition_keys,
                    fetch_date=run_date,
                ),
            )
            raws = ds.fetch(ctx, dict(part))
            if not raws:
                raise ValueError(f"{ds.name}.fetch returned no raw files")
            for raw in raws:
                store.record_raw(run_id, _in_lake(raw, settings))
            return _process(ds, part, raws, settings, store, run_id)

        return _guarded(store, run_id, body)
    raise AssertionError("unreachable")


def reprocess_partition(
    dataset: str | Dataset,
    partition: Mapping[str, object],
    *,
    trigger: str = "cli",
    settings: Settings | None = None,
    meta: MetaStore | None = None,
) -> RunResult:
    """Chạy lại bước 5 đến 10 từ các file raw của lần chạy thành công gần nhất; không gọi mạng."""
    ds = _resolve(dataset)
    part = validate_partition(ds, partition)
    settings = settings or get_settings()
    check_disk(settings)

    with ExitStack() as stack:
        store = meta or _owned(stack, MetaStore.from_settings(settings))
        raws = _raws_of_last_success(ds, part, settings, store)
        run_id = store.start_run(ds.source, ds.name, part, trigger)

        def body() -> RunResult:
            for raw in raws:
                store.record_raw(run_id, _in_lake(raw, settings))
            return _process(ds, part, raws, settings, store, run_id)

        return _guarded(store, run_id, body)
    raise AssertionError("unreachable")


def validate_partition(ds: Dataset, partition: Mapping[str, object]) -> Partition:
    """Đúng bộ khoá `partition_keys`; giá trị thành chuỗi (ngày dạng ISO)."""
    expected, given = set(ds.partition_keys), set(partition)
    if expected != given:
        raise ValueError(
            f"{ds.name} partition keys must be {list(ds.partition_keys)}; "
            f"missing={sorted(expected - given)} extra={sorted(given - expected)}"
        )
    part = {k: _as_text(partition[k]) for k in ds.partition_keys}
    empty = [k for k, v in part.items() if not v.strip()]
    if empty:
        raise ValueError(f"{ds.name} partition has empty values for {empty}")
    return part


def _process(
    ds: Dataset,
    part: Partition,
    raws: list[RawFile],
    settings: Settings,
    meta: MetaStore,
    run_id: uuid.UUID,
) -> RunResult:
    # 5. Chuẩn hoá. ingested_at = fetched_at muộn nhất của raw, nên chạy lại từ cùng raw cho
    # đúng cùng một file silver; tải lại thì ingested_at mới.
    df = ds.normalize(raws, dict(part))
    clash = [c for c in LAYER_COLUMNS if c in df.columns]
    if clash:
        raise ValueError(f"{ds.name}.normalize must not set {clash}; the pipeline adds them")
    ingested_at = max(pd.Timestamp(datetime.fromisoformat(r.fetched_at)) for r in raws)
    df = df.assign(source=ds.source, ingested_at=ingested_at)

    # 6. Check trước khi ghi. Check `error` fail thì raise, chưa ghi gì.
    results = run_checks(df, ds.checks(dict(part)), table_name=ds.table, meta=meta, run_id=run_id)

    # 7. Ghi silver: mỗi file lock riêng, theo thứ tự đường dẫn để hai run không chờ nhau vòng tròn.
    schema = ds.schema
    targets = sorted(
        (
            (layout.table_file(settings, schema.storage_layer, schema.name, p), frame)
            for p, frame in split_partitions(conform(df, schema), schema)
        ),
        key=lambda item: item[0],
    )
    order = list(schema.sort or schema.key)
    rows_out = 0
    for path, frame in targets:
        path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(f"{path}.lock", timeout=LOCK_TIMEOUT_SECONDS):
            merged = conform(ds.merge(read_parquet_if_exists(path), frame), schema)
            merged = merged.sort_values(order, kind="stable").reset_index(drop=True)
            write_parquet_atomic(merged, path, schema=schema.arrow_schema())
        rows_out += len(merged)

    # 8. Catalog.
    meta.upsert_catalog(
        CatalogEntry(
            table_name=schema.name,
            source=ds.source,
            layer="silver",
            dataset=ds.name,
            description=ds.description,
            source_url=ds.source_url,
            license_note=ds.license_note,
            key_cols=",".join(schema.key),
            path_pattern=schema.path_pattern(),
            refresh_schedule=ds.refresh_schedule,
            schema_version=schema.version,
            notes=ds.catalog_notes,
        )
    )

    # 9. Xoá raw nếu dataset không giữ (minute): manifest và dòng raw_files vẫn còn.
    if not ds.keep_raw:
        for raw in raws:
            delete_raw(raw.path)
        meta.mark_raw_deleted(run_id)

    # 10. Xong.
    paths = [p for p, _ in targets]
    meta.finish_run(
        run_id,
        "success",
        rows_in=len(df),
        rows_out=rows_out,
        silver_path=",".join(p.relative_to(settings.data_root).as_posix() for p in paths),
    )
    logger.info(
        "%s %s: %d rows in, %d rows in %d file(s)", ds.name, part, len(df), rows_out, len(paths)
    )
    return RunResult(run_id, ds.name, part, "success", len(df), rows_out, paths, raws, results)


def _guarded[T](meta: MetaStore, run_id: uuid.UUID, body: Callable[[], T]) -> T:
    """Lỗi nào cũng ghi vào run (đã che key) rồi raise lại nguyên lỗi gốc."""
    try:
        return body()
    except NotAvailableYet as exc:
        _finish_quietly(meta, run_id, "skipped_not_available", exc)
        raise
    except BaseException as exc:
        _finish_quietly(meta, run_id, "failed", exc)
        raise


def _finish_quietly(meta: MetaStore, run_id: uuid.UUID, status: str, exc: BaseException) -> None:
    try:
        meta.finish_run(run_id, status, error=f"{type(exc).__name__}: {exc}")
    except Exception:
        # Không để lỗi ghi metadata che mất lỗi gốc của run.
        logger.exception("could not record status %s for run %s", status, run_id)


def _raws_of_last_success(
    ds: Dataset, part: Partition, settings: Settings, meta: MetaStore
) -> list[RawFile]:
    last = meta.last_success_run(ds.name, part)
    if last is None:
        raise RawUnavailable(f"{ds.name} {part}: no successful run to reprocess from")
    rows = meta.raw_files_for(last.run_id)
    deleted = [r["path"] for r in rows if r["deleted"]]
    if not rows or deleted:
        raise RawUnavailable(
            f"{ds.name} {part}: raw files of run {last.run_id} were deleted "
            f"(keep_raw=False); fetch again with run_partition"
        )
    raws: list[RawFile] = []
    for r in rows:
        path = settings.data_root / r["path"]
        if not path.is_file():
            raise RawUnavailable(f"{ds.name} {part}: raw file missing on disk: {r['path']}")
        raws.append(read_manifest(path))
    return raws


def _in_lake(raw: RawFile, settings: Settings) -> RawFile:
    """Metadata lưu đường dẫn tương đối với data_root, để chuyển máy/đổi mount vẫn đúng."""
    try:
        relative = raw.path.absolute().relative_to(settings.data_root.absolute())
    except ValueError:
        raise ValueError(f"raw file {raw.path} is outside data_root; use ctx.save") from None
    return replace(raw, path=relative)


def _resolve(dataset: str | Dataset) -> Dataset:
    return dataset if isinstance(dataset, Dataset) else get_dataset(dataset)


def _as_text(value: object) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def _owned(stack: ExitStack, store: MetaStore) -> MetaStore:
    stack.callback(store.close)
    return store
