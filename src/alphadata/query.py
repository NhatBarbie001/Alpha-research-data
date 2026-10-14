"""Truy vấn tầng silver bằng DuckDB. Mỗi bảng silver là một view đọc thẳng các file Parquet."""

import duckdb

from alphadata.config import Settings, get_settings

# view -> glob tương đối trong thư mục silver
SILVER_VIEWS = {
    "crypto_bars_1m": "crypto_bars_1m/*/*/*.parquet",
    "equity_bars_1d": "equity_bars_1d/*/*.parquet",
    "macro_observations": "macro_observations/*/*.parquet",
}


def connect(settings: Settings | None = None) -> duckdb.DuckDBPyConnection:
    settings = settings or get_settings()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    created = set()
    for view, pattern in SILVER_VIEWS.items():
        if any(settings.silver_dir.glob(pattern)):
            glob = (settings.silver_dir / pattern).as_posix()
            con.execute(f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{glob}', union_by_name = true)")
            created.add(view)
    if "macro_observations" in created:
        # Giá trị hiện hành mới nhất của mỗi quan sát (KHÔNG phải point-in-time, chỉ để xem nhanh)
        con.execute("""
            CREATE VIEW macro_latest AS
            SELECT series_id, obs_date, value, available_from
            FROM macro_observations
            QUALIFY row_number() OVER (PARTITION BY series_id, obs_date ORDER BY available_from DESC) = 1
        """)
    return con


def macro_as_of(con: duckdb.DuckDBPyConnection, series_id: str, as_of: str):
    """Chuỗi macro đúng như những gì đã biết vào ngày `as_of` (point-in-time)."""
    return con.execute("""
        SELECT obs_date, value, available_from
        FROM macro_observations
        WHERE series_id = ? AND available_from <= ?::DATE AND ?::DATE <= available_to
        ORDER BY obs_date
    """, [series_id, as_of, as_of]).df()
