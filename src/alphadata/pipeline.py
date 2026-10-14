"""Chạy một partition của một dataset: fetch -> normalize -> check -> merge -> ghi silver -> log.

Hàm run_partition được gọi từ cả CLI lẫn Airflow, nên pipeline không phụ thuộc Airflow.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd

from alphadata import quality
from alphadata.config import Settings, get_settings
from alphadata.meta import MetaStore
from alphadata.sources import get_dataset
from alphadata.sources.base import FetchContext, make_session
from alphadata.storage import check_disk, read_parquet_if_exists, write_parquet_atomic

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass
class RunResult:
    run_id: str
    dataset: str
    partition: dict
    rows_in: int
    rows_out: int
    silver_path: Path
    checks: list[quality.CheckResult] = field(default_factory=list)

    @property
    def warnings(self) -> list[quality.CheckResult]:
        return [c for c in self.checks if not c.passed]


def run_partition(dataset_name: str, partition: dict, settings: Settings | None = None,
                  meta: MetaStore | None = None, session=None) -> RunResult:
    settings = settings or get_settings()
    ds = get_dataset(dataset_name)
    ds.validate_partition(partition)
    check_disk(settings)
    meta = meta or MetaStore(settings.meta_db_url)

    run_id = str(uuid4())
    now = datetime.now(timezone.utc)
    ctx = FetchContext(settings=settings, session=session or make_session(), run_date=now.strftime("%Y-%m-%d"))
    meta.start_run(run_id, ds.source, ds.name, partition)
    log.info("run %s: %s %s", run_id[:8], ds.name, partition)

    try:
        raws = ds.fetch(ctx, partition)
        meta.record_raw(run_id, raws)

        new = ds.normalize(raws, partition)
        new["source"] = ds.source
        new["ingested_at"] = pd.Series(pd.Timestamp(now), index=new.index).astype("datetime64[us, UTC]")

        results = ds.checks(new, partition)
        meta.record_dq(run_id, ds.table, results)
        for r in results:
            if not r.passed:
                log.warning("DQ %s [%s] %s: %s", r.severity, ds.table, r.name, r.detail)
        if quality.has_errors(results):
            failed = [f"{r.name}({r.detail})" for r in results if r.severity == "error" and not r.passed]
            raise DataQualityError(f"{ds.name} {partition}: " + ", ".join(failed))

        path = ds.silver_path(settings.silver_dir, partition)
        merged = ds.merge(read_parquet_if_exists(path), new)
        write_parquet_atomic(merged, path)

        meta.upsert_catalog(
            table_name=ds.table, source=ds.source, description=ds.description,
            source_url=ds.source_url, dataset=ds.name, key_cols=",".join(ds.key_cols),
            path_pattern=ds.path_pattern, notes=ds.notes,
        )

        if not ds.keep_raw:
            for r in raws:
                r.path.unlink(missing_ok=True)
            meta.mark_raw_deleted(run_id)

        meta.finish_run(run_id, "success", rows_in=len(new), rows_out=len(merged), silver_path=str(path))
        log.info("run %s: ok, %d dòng mới, file có %d dòng -> %s", run_id[:8], len(new), len(merged), path)
        return RunResult(run_id, ds.name, partition, len(new), len(merged), path, results)

    except Exception as e:
        meta.finish_run(run_id, "failed", error=f"{type(e).__name__}: {e}"[:4000])
        raise
