"""Bảng metadata theo docs/04-data-model.md mục 7. Sửa ở data model trước, rồi sửa ở đây."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

metadata = sa.MetaData()

# SQLite chỉ tự tăng với INTEGER PRIMARY KEY; Postgres dùng bigint (bigserial).
BigId = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
Json = sa.JSON().with_variant(JSONB(), "postgresql")
Timestamp = sa.DateTime(timezone=True)

RUN_TRIGGERS = frozenset({"cli", "airflow", "backfill"})
RUN_STATUSES = frozenset({"running", "success", "failed", "skipped_not_available"})
SEVERITIES = frozenset({"error", "warn"})
LAYERS = frozenset({"silver", "reference", "gold"})
CHECKPOINT_STATUSES = frozenset({"pending", "done", "failed"})

ingestion_runs = sa.Table(
    "ingestion_runs",
    metadata,
    sa.Column("run_id", sa.Uuid(), primary_key=True),
    sa.Column("source", sa.Text(), nullable=False),
    sa.Column("dataset", sa.Text(), nullable=False),
    sa.Column("partition", Json, nullable=False),
    sa.Column("trigger", sa.Text(), nullable=False),
    sa.Column("airflow_run_id", sa.Text()),
    sa.Column("backfill_id", sa.Uuid()),
    sa.Column("started_at", Timestamp, nullable=False),
    sa.Column("finished_at", Timestamp),
    sa.Column("status", sa.Text(), nullable=False),
    sa.Column("rows_in", sa.BigInteger()),
    sa.Column("rows_out", sa.BigInteger()),
    sa.Column("silver_path", sa.Text()),
    sa.Column("error", sa.Text()),
    sa.Index("ix_ingestion_runs_dataset_started", "dataset", sa.text("started_at DESC")),
    sa.Index("ix_ingestion_runs_status_started", "status", sa.text("started_at DESC")),
)

raw_files = sa.Table(
    "raw_files",
    metadata,
    sa.Column("id", BigId, primary_key=True, autoincrement=True),
    sa.Column("run_id", sa.Uuid(), sa.ForeignKey("ingestion_runs.run_id"), nullable=False),
    sa.Column("path", sa.Text(), nullable=False),
    sa.Column("url", sa.Text(), nullable=False),
    sa.Column("sha256", sa.CHAR(64), nullable=False),
    sa.Column("bytes", sa.BigInteger(), nullable=False),
    sa.Column("fetched_at", Timestamp, nullable=False),
    sa.Column("deleted", sa.Boolean(), nullable=False, server_default=sa.false()),
    sa.Index("ix_raw_files_run_id", "run_id"),
    sa.Index("ix_raw_files_sha256", "sha256"),
)

dq_results = sa.Table(
    "dq_results",
    metadata,
    sa.Column("id", BigId, primary_key=True, autoincrement=True),
    sa.Column("run_id", sa.Uuid(), sa.ForeignKey("ingestion_runs.run_id")),
    sa.Column("table_name", sa.Text(), nullable=False),
    sa.Column("check_name", sa.Text(), nullable=False),
    sa.Column("severity", sa.Text(), nullable=False),
    sa.Column("passed", sa.Boolean(), nullable=False),
    sa.Column("detail", sa.Text()),
    sa.Column("checked_at", Timestamp, nullable=False),
    sa.Index("ix_dq_results_table_checked", "table_name", sa.text("checked_at DESC")),
    sa.Index("ix_dq_results_passed_checked", "passed", sa.text("checked_at DESC")),
    sa.Index("ix_dq_results_run_id", "run_id"),
)

table_catalog = sa.Table(
    "table_catalog",
    metadata,
    sa.Column("table_name", sa.Text(), primary_key=True),
    sa.Column("source", sa.Text(), primary_key=True),
    sa.Column("layer", sa.Text(), nullable=False),
    sa.Column("dataset", sa.Text()),
    sa.Column("description", sa.Text(), nullable=False),
    sa.Column("source_url", sa.Text(), nullable=False),
    sa.Column("license_note", sa.Text(), nullable=False),
    sa.Column("key_cols", sa.Text(), nullable=False),
    sa.Column("path_pattern", sa.Text(), nullable=False),
    sa.Column("refresh_schedule", sa.Text(), nullable=False),
    sa.Column("schema_version", sa.Integer(), nullable=False),
    sa.Column("notes", sa.Text(), nullable=False, server_default=""),
    sa.Column("updated_at", Timestamp, nullable=False),
)

backfill_checkpoints = sa.Table(
    "backfill_checkpoints",
    metadata,
    sa.Column("backfill_id", sa.Uuid(), primary_key=True),
    sa.Column("partition_hash", sa.CHAR(64), primary_key=True),
    sa.Column("dataset", sa.Text(), nullable=False),
    sa.Column("partition", Json, nullable=False),
    sa.Column("status", sa.Text(), nullable=False),
    sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("last_error", sa.Text()),
    sa.Column("updated_at", Timestamp, nullable=False),
    sa.Index("ix_backfill_checkpoints_status", "backfill_id", "status"),
)

# Ghi lại migration đã áp dụng (xem schema.py). Không có trong data model vì là bảng kỹ thuật.
schema_migrations = sa.Table(
    "schema_migrations",
    metadata,
    sa.Column("version", sa.Text(), primary_key=True),
    sa.Column("applied_at", Timestamp, nullable=False),
)
