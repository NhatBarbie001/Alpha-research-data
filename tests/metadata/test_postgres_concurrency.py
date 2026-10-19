"""Nhiều task Airflow cùng khởi tạo MetaStore và cùng ghi catalog trên một DB Postgres mới."""

import subprocess
import sys

import sqlalchemy as sa

WORKER = """
import sys
from alphadata.metadata import CatalogEntry, MetaStore

store = MetaStore(sys.argv[1])
for i in range(30):
    store.upsert_catalog(CatalogEntry(
        table_name="crypto_bars_1m", source="binance", layer="silver",
        description=f"worker {sys.argv[2]} write {i}", source_url="u", license_note="l",
        key_cols="symbol,ts_utc", path_pattern="p", refresh_schedule="c", schema_version=1,
    ))
store.close()
"""


def test_parallel_init_and_catalog_upsert(pg_url: str) -> None:
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", WORKER, pg_url, str(n)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for n in range(4)
    ]
    results = [p.communicate(timeout=120) for p in procs]
    for proc, (_, err) in zip(procs, results, strict=True):
        assert proc.returncode == 0, err
    engine = sa.create_engine(pg_url)
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM table_catalog").scalar_one() == 1
    engine.dispose()
