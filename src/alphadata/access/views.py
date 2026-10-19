"""DuckDB với một view cho mỗi bảng đã có file trong lake. Phase 0 dùng cho CLI `query` và debug."""

from collections.abc import Sequence

import duckdb

from alphadata.core.settings import Settings, get_settings
from alphadata.schemas import list_schemas

MEMORY_LIMIT = "2GB"
THREADS = 2


def connect(
    settings: Settings | None = None,
    *,
    layers: Sequence[str] = ("silver", "reference", "gold"),
    memory_limit: str = MEMORY_LIMIT,
    threads: int = THREADS,
) -> duckdb.DuckDBPyConnection:
    """Mở DuckDB trong bộ nhớ, tạo view `<bảng>` cho mọi bảng của `layers` đã có ít nhất một file.

    Không bật hive_partitioning: giá trị partition trong đường dẫn đã mã hoá %XX (ví dụ ^GSPC), còn
    cột trong file giữ giá trị gốc. Cột suy ra (year, month) vì vậy không có trong view.
    """
    settings = settings or get_settings()
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = '{_sql_literal(memory_limit)}'")
    con.execute(f"SET threads = {int(threads)}")
    # Mặc định DuckDB hiển thị TIMESTAMPTZ theo giờ máy (ví dụ +07:00); quy ước của lake là UTC.
    con.execute("SET TimeZone = 'UTC'")
    for schema in list_schemas():
        if schema.layer not in layers:
            continue
        pattern = settings.data_root / schema.path_pattern()
        if not any(settings.data_root.glob(schema.path_pattern())):
            continue
        glob = _sql_literal(pattern.as_posix())
        source = f"read_parquet('{glob}', union_by_name = true, hive_partitioning = false)"
        # Tên bảng từ schemas (đã kiểm snake_case), đường dẫn đã escape dấu nháy.
        con.execute(f'CREATE VIEW "{schema.name}" AS SELECT * FROM {source}')  # noqa: S608
    return con


def view_names(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        "SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY view_name"
    ).fetchall()
    return [r[0] for r in rows]


def _sql_literal(text: str) -> str:
    return text.replace("'", "''")
