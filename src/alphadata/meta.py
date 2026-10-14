"""Metadata vận hành: lịch sử chạy, file raw, kết quả kiểm tra chất lượng, catalog bảng.

Chạy được trên SQLite (máy dev) và Postgres (server) nhờ SQLAlchemy Core.
"""

import json
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    delete,
    insert,
    select,
    text,
    update,
)

metadata = MetaData()

ingestion_runs = Table(
    "ingestion_runs",
    metadata,
    Column("run_id", String(36), primary_key=True),
    Column("source", String(64), nullable=False),
    Column("dataset", String(128), nullable=False),
    Column("partition", Text, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", String(16), nullable=False),  # running | success | failed
    Column("rows_in", Integer),
    Column("rows_out", Integer),
    Column("silver_path", Text),
    Column("error", Text),
)

raw_files = Table(
    "raw_files",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(36), nullable=False),
    Column("path", Text, nullable=False),
    Column("url", Text, nullable=False),
    Column("sha256", String(64), nullable=False),
    Column("bytes", Integer, nullable=False),
    Column("fetched_at", String(40), nullable=False),
    Column("deleted", Boolean, nullable=False, default=False),
)

dq_results = Table(
    "dq_results",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(36), nullable=False),
    Column("table_name", String(128), nullable=False),
    Column("check_name", String(128), nullable=False),
    Column("severity", String(8), nullable=False),  # error | warn
    Column("passed", Boolean, nullable=False),
    Column("detail", Text),
    Column("checked_at", DateTime(timezone=True), nullable=False),
)

table_catalog = Table(
    "table_catalog",
    metadata,
    # Một bảng có thể được nạp từ nhiều nguồn, nên khoá là (table_name, source)
    Column("table_name", String(128), primary_key=True),
    Column("source", String(64), primary_key=True),
    Column("description", Text, nullable=False),
    Column("source_url", Text, nullable=False),
    Column("dataset", String(128), nullable=False),
    Column("key_cols", Text, nullable=False),
    Column("path_pattern", Text, nullable=False),
    Column("notes", Text),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class MetaStore:
    def __init__(self, url: str):
        self.engine = create_engine(url, future=True)
        with self.engine.begin() as c:
            if c.dialect.name == "postgresql":
                # Nhiều task Airflow chạy song song cùng tạo bảng sẽ đụng nhau; khoá để làm lần lượt
                c.execute(text("SELECT pg_advisory_xact_lock(4242)"))
            metadata.create_all(c)

    def start_run(self, run_id: str, source: str, dataset: str, partition: dict) -> None:
        with self.engine.begin() as c:
            c.execute(
                insert(ingestion_runs).values(
                    run_id=run_id,
                    source=source,
                    dataset=dataset,
                    partition=json.dumps(partition, sort_keys=True),
                    started_at=_now(),
                    status="running",
                )
            )

    def finish_run(self, run_id: str, status: str, **fields) -> None:
        with self.engine.begin() as c:
            c.execute(
                update(ingestion_runs)
                .where(ingestion_runs.c.run_id == run_id)
                .values(status=status, finished_at=_now(), **fields)
            )

    def record_raw(self, run_id: str, raws) -> None:
        if not raws:
            return
        with self.engine.begin() as c:
            c.execute(
                insert(raw_files),
                [
                    dict(run_id=run_id, path=str(r.path), url=r.url, sha256=r.sha256,
                         bytes=r.bytes, fetched_at=r.fetched_at, deleted=False)
                    for r in raws
                ],
            )

    def mark_raw_deleted(self, run_id: str) -> None:
        with self.engine.begin() as c:
            c.execute(update(raw_files).where(raw_files.c.run_id == run_id).values(deleted=True))

    def record_dq(self, run_id: str, table_name: str, results) -> None:
        if not results:
            return
        now = _now()
        with self.engine.begin() as c:
            c.execute(
                insert(dq_results),
                [
                    dict(run_id=run_id, table_name=table_name, check_name=r.name,
                         severity=r.severity, passed=r.passed, detail=r.detail, checked_at=now)
                    for r in results
                ],
            )

    def upsert_catalog(self, **row) -> None:
        """INSERT ... ON CONFLICT DO UPDATE: an toàn khi nhiều task cùng ghi một dòng catalog."""
        row = {**row, "updated_at": _now()}
        with self.engine.begin() as c:
            if c.dialect.name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as dialect_insert
            elif c.dialect.name == "sqlite":
                from sqlalchemy.dialects.sqlite import insert as dialect_insert
            else:
                c.execute(delete(table_catalog).where(
                    (table_catalog.c.table_name == row["table_name"]) & (table_catalog.c.source == row["source"])
                ))
                c.execute(insert(table_catalog).values(**row))
                return
            stmt = dialect_insert(table_catalog).values(**row)
            stmt = stmt.on_conflict_do_update(
                index_elements=["table_name", "source"],
                set_={k: stmt.excluded[k] for k in row if k not in ("table_name", "source")},
            )
            c.execute(stmt)

    def catalog(self) -> list[dict]:
        with self.engine.connect() as c:
            return [dict(r._mapping) for r in c.execute(select(table_catalog))]

    def recent_runs(self, limit: int = 20) -> list[dict]:
        with self.engine.connect() as c:
            q = select(ingestion_runs).order_by(ingestion_runs.c.started_at.desc()).limit(limit)
            return [dict(r._mapping) for r in c.execute(q)]
