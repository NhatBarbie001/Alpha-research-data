import json
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql, sqlite

from alphadata.core.http import mask_secrets
from alphadata.core.settings import Settings
from alphadata.metadata import tables as t
from alphadata.metadata.schema import init_schema
from alphadata.storage.raw import RawFile

ERROR_MAX_CHARS = 4000


def canonical_json(value: object) -> str:
    """JSON sort key, ngày dạng ISO: cùng partition luôn ra cùng chuỗi (dùng so sánh và hash)."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)


def _json_default(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"not JSON serializable: {value!r}")


def normalize_partition(partition: Mapping[str, object]) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(canonical_json(dict(partition)))
    return loaded


class CheckLike(Protocol):
    """Kết quả một check (quality.CheckResult thoả protocol này; metadata không import quality)."""

    @property
    def name(self) -> str: ...
    @property
    def severity(self) -> str: ...
    @property
    def passed(self) -> bool: ...
    @property
    def detail(self) -> str: ...


@dataclass(frozen=True)
class RunRecord:
    run_id: uuid.UUID
    source: str
    dataset: str
    partition: dict[str, Any]
    trigger: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    rows_in: int | None
    rows_out: int | None
    silver_path: str | None
    error: str | None
    airflow_run_id: str | None
    backfill_id: uuid.UUID | None


@dataclass(frozen=True)
class DqRecord:
    table_name: str
    check_name: str
    severity: str
    passed: bool
    detail: str | None
    checked_at: datetime
    run_id: uuid.UUID | None


@dataclass(frozen=True)
class CatalogEntry:
    table_name: str
    source: str
    layer: str
    description: str
    source_url: str
    license_note: str
    key_cols: str
    path_pattern: str
    refresh_schedule: str
    schema_version: int
    dataset: str | None = None
    notes: str = ""
    updated_at: datetime | None = field(default=None, compare=False)


def utcnow() -> datetime:
    return datetime.now(UTC)


def _utc(value: datetime | None) -> datetime | None:
    # SQLite trả datetime không có timezone; mọi giá trị ghi vào đều là UTC.
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def _require(value: str, allowed: frozenset[str], what: str) -> None:
    if value not in allowed:
        raise ValueError(f"invalid {what} {value!r}; allowed: {sorted(allowed)}")


class MetaStore:
    def __init__(self, url: str, *, migrations_dir: Path | None = None) -> None:
        sa_url = sa.make_url(url)
        if sa_url.get_backend_name() == "sqlite" and sa_url.database not in (None, "", ":memory:"):
            Path(sa_url.database).parent.mkdir(parents=True, exist_ok=True)
        self.engine = sa.create_engine(sa_url, json_serializer=canonical_json, pool_pre_ping=True)
        if self.engine.dialect.name == "sqlite":
            sa.event.listen(self.engine, "connect", _sqlite_foreign_keys)
        init_schema(self.engine, migrations_dir)

    @classmethod
    def from_settings(cls, settings: Settings) -> "MetaStore":
        return cls(settings.meta_db_url)

    def close(self) -> None:
        self.engine.dispose()

    # --- ghi ---------------------------------------------------------------

    def start_run(
        self,
        source: str,
        dataset: str,
        partition: Mapping[str, object],
        trigger: str,
        *,
        airflow_run_id: str | None = None,
        backfill_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        _require(trigger, t.RUN_TRIGGERS, "trigger")
        run_id = uuid.uuid4()
        with self.engine.begin() as conn:
            conn.execute(
                t.ingestion_runs.insert().values(
                    run_id=run_id,
                    source=source,
                    dataset=dataset,
                    partition=normalize_partition(partition),
                    trigger=trigger,
                    airflow_run_id=airflow_run_id,
                    backfill_id=backfill_id,
                    started_at=utcnow(),
                    status="running",
                )
            )
        return run_id

    def record_raw(self, run_id: uuid.UUID, raw: RawFile) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(
                t.raw_files.insert()
                .values(
                    run_id=run_id,
                    path=raw.path.as_posix(),
                    url=mask_secrets(raw.url),
                    sha256=raw.sha256,
                    bytes=raw.bytes,
                    fetched_at=datetime.fromisoformat(raw.fetched_at),
                )
                .returning(t.raw_files.c.id)
            )
            return int(result.scalar_one())

    def mark_raw_deleted(self, run_id: uuid.UUID) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(
                t.raw_files.update().where(t.raw_files.c.run_id == run_id).values(deleted=True)
            )
            return result.rowcount

    def record_dq(
        self, table_name: str, results: Iterable[CheckLike], run_id: uuid.UUID | None = None
    ) -> int:
        rows = []
        checked_at = utcnow()
        for r in results:
            _require(r.severity, t.SEVERITIES, "severity")
            rows.append(
                {
                    "run_id": run_id,
                    "table_name": table_name,
                    "check_name": r.name,
                    "severity": r.severity,
                    "passed": r.passed,
                    "detail": r.detail,
                    "checked_at": checked_at,
                }
            )
        if rows:
            with self.engine.begin() as conn:
                conn.execute(t.dq_results.insert(), rows)
        return len(rows)

    def finish_run(
        self,
        run_id: uuid.UUID,
        status: str,
        *,
        rows_in: int | None = None,
        rows_out: int | None = None,
        silver_path: str | None = None,
        error: str | None = None,
    ) -> None:
        _require(status, t.RUN_STATUSES - {"running"}, "final status")
        if error is not None:
            # Message lỗi có thể chứa URL: che key trước khi lưu.
            error = mask_secrets(error)[:ERROR_MAX_CHARS]
        with self.engine.begin() as conn:
            result = conn.execute(
                t.ingestion_runs.update()
                .where(t.ingestion_runs.c.run_id == run_id)
                .values(
                    status=status,
                    finished_at=utcnow(),
                    rows_in=rows_in,
                    rows_out=rows_out,
                    silver_path=silver_path,
                    error=error,
                )
            )
            if result.rowcount != 1:
                raise KeyError(f"unknown run_id {run_id}")

    def upsert_catalog(self, entry: CatalogEntry) -> None:
        _require(entry.layer, t.LAYERS, "layer")
        values = {
            c.name: getattr(entry, c.name)
            for c in t.table_catalog.columns
            if c.name != "updated_at"
        }
        values["updated_at"] = utcnow()
        stmt = dialect_insert(self.engine, t.table_catalog).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["table_name", "source"],
            set_={k: stmt.excluded[k] for k in values if k not in ("table_name", "source")},
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)

    # --- đọc ---------------------------------------------------------------

    def get_run(self, run_id: uuid.UUID) -> RunRecord:
        with self.engine.connect() as conn:
            row = conn.execute(
                sa.select(t.ingestion_runs).where(t.ingestion_runs.c.run_id == run_id)
            ).one_or_none()
        if row is None:
            raise KeyError(f"unknown run_id {run_id}")
        return _run_record(row)

    def recent_runs(
        self, limit: int = 20, dataset: str | None = None, status: str | None = None
    ) -> list[RunRecord]:
        q = sa.select(t.ingestion_runs).order_by(t.ingestion_runs.c.started_at.desc()).limit(limit)
        if dataset is not None:
            q = q.where(t.ingestion_runs.c.dataset == dataset)
        if status is not None:
            q = q.where(t.ingestion_runs.c.status == status)
        with self.engine.connect() as conn:
            return [_run_record(r) for r in conn.execute(q)]

    def raw_files_for(self, run_id: uuid.UUID) -> list[dict[str, Any]]:
        q = sa.select(t.raw_files).where(t.raw_files.c.run_id == run_id).order_by(t.raw_files.c.id)
        with self.engine.connect() as conn:
            return [dict(r._mapping) for r in conn.execute(q)]

    def catalog(self, layer: str | None = None) -> list[CatalogEntry]:
        q = sa.select(t.table_catalog).order_by(
            t.table_catalog.c.table_name, t.table_catalog.c.source
        )
        if layer is not None:
            q = q.where(t.table_catalog.c.layer == layer)
        with self.engine.connect() as conn:
            rows = list(conn.execute(q))
        return [
            CatalogEntry(**{**r._mapping, "updated_at": _utc(r._mapping["updated_at"])})
            for r in rows
        ]

    def failed_checks(self, since: datetime) -> list[DqRecord]:
        q = (
            sa.select(t.dq_results)
            .where(t.dq_results.c.passed.is_(False), t.dq_results.c.checked_at >= since)
            .order_by(t.dq_results.c.checked_at.desc(), t.dq_results.c.id.desc())
        )
        with self.engine.connect() as conn:
            rows = list(conn.execute(q))
        return [
            DqRecord(
                table_name=r.table_name,
                check_name=r.check_name,
                severity=r.severity,
                passed=r.passed,
                detail=r.detail,
                checked_at=_utc(r.checked_at) or r.checked_at,
                run_id=r.run_id,
            )
            for r in rows
        ]

    def last_success(self, dataset: str, partition: Mapping[str, object]) -> datetime | None:
        """Thời điểm xong của lần chạy thành công gần nhất cho đúng (dataset, partition)."""
        run = self.last_success_run(dataset, partition)
        return None if run is None else run.finished_at

    def last_success_run(self, dataset: str, partition: Mapping[str, object]) -> RunRecord | None:
        q = (
            sa.select(t.ingestion_runs)
            .where(
                t.ingestion_runs.c.dataset == dataset,
                t.ingestion_runs.c.status == "success",
                _partition_equals(self.engine, t.ingestion_runs.c.partition, partition),
            )
            .order_by(t.ingestion_runs.c.finished_at.desc())
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(q).one_or_none()
        return None if row is None else _run_record(row)


def _partition_equals(
    engine: sa.Engine, column: sa.ColumnElement[Any], partition: Mapping[str, object]
) -> sa.ColumnElement[bool]:
    if engine.dialect.name == "postgresql":
        return column == sa.cast(sa.literal(canonical_json(dict(partition))), postgresql.JSONB)
    # SQLite lưu JSON dạng text do canonical_json sinh ra, nên so chuỗi là đủ.
    return sa.cast(column, sa.Text) == canonical_json(dict(partition))


def dialect_insert(engine: sa.Engine, table: sa.Table) -> Any:
    """INSERT hỗ trợ ON CONFLICT cho đúng dialect."""
    if engine.dialect.name == "postgresql":
        return postgresql.insert(table)
    if engine.dialect.name == "sqlite":
        return sqlite.insert(table)
    raise NotImplementedError(f"unsupported database {engine.dialect.name}")


def _run_record(row: sa.Row[Any]) -> RunRecord:
    m = row._mapping
    return RunRecord(
        run_id=m["run_id"],
        source=m["source"],
        dataset=m["dataset"],
        partition=m["partition"],
        trigger=m["trigger"],
        status=m["status"],
        started_at=_utc(m["started_at"]) or m["started_at"],
        finished_at=_utc(m["finished_at"]),
        rows_in=m["rows_in"],
        rows_out=m["rows_out"],
        silver_path=m["silver_path"],
        error=m["error"],
        airflow_run_id=m["airflow_run_id"],
        backfill_id=m["backfill_id"],
    )


def _sqlite_foreign_keys(dbapi_conn: Any, _record: Any) -> None:
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
