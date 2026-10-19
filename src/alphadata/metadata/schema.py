"""Tạo và nâng cấp schema metadata.

DB mới: `create_all` từ tables.py rồi đánh dấu mọi migration là đã áp dụng (tables.py luôn là
schema mới nhất). DB đã có: chỉ chạy các file migrations/NNNN_*.sql chưa áp dụng, theo thứ tự.
"""

import re
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path

import sqlalchemy as sa

from alphadata.metadata.tables import metadata, schema_migrations

# Khoá advisory cố định: nhiều task Airflow khởi tạo MetaStore cùng lúc chỉ một task tạo bảng.
_ADVISORY_LOCK_KEY = 0x616C70686164  # "alphad"
_MIGRATION_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")


def migration_files(directory: Path | None = None) -> list[tuple[str, str]]:
    """[(version, sql)] theo thứ tự tên file."""
    if directory is None:
        root = resources.files("alphadata.metadata") / "migrations"
        entries = [(e.name, e.read_text(encoding="utf-8")) for e in root.iterdir()]
    else:
        entries = [(p.name, p.read_text(encoding="utf-8")) for p in directory.iterdir()]
    found = sorted((name, sql) for name, sql in entries if name.endswith(".sql"))
    bad = [name for name, _ in found if not _MIGRATION_RE.match(name)]
    if bad:
        raise ValueError(f"migration files must be named NNNN_snake_case.sql: {bad}")
    return [(name.removesuffix(".sql"), sql) for name, sql in found]


def init_schema(engine: sa.Engine, migrations_dir: Path | None = None) -> list[str]:
    """Trả về danh sách migration vừa chạy (rỗng với DB mới hoặc đã cập nhật)."""
    migrations = migration_files(migrations_dir)
    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(sa.text("SELECT pg_advisory_xact_lock(:k)"), {"k": _ADVISORY_LOCK_KEY})
        now = datetime.now(UTC)
        if not sa.inspect(conn).has_table(schema_migrations.name):
            metadata.create_all(conn)
            if migrations:
                conn.execute(
                    schema_migrations.insert(),
                    [{"version": v, "applied_at": now} for v, _ in migrations],
                )
            return []
        applied = set(conn.scalars(sa.select(schema_migrations.c.version)))
        ran: list[str] = []
        for version, sql in migrations:
            if version in applied:
                continue
            for statement in _split_sql(sql):
                conn.exec_driver_sql(statement)
            conn.execute(schema_migrations.insert().values(version=version, applied_at=now))
            ran.append(version)
        return ran


def _split_sql(sql: str) -> list[str]:
    # Migration chỉ chứa DDL đơn giản, mỗi câu kết thúc bằng ";". Không hỗ trợ ";" trong chuỗi.
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
