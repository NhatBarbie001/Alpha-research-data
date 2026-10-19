import os
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa

from alphadata.metadata import MetaStore

PG_URL = os.environ.get("ALPHADATA_TEST_PG_URL", "")


def reset_postgres(url: str) -> None:
    """Xoá sạch bảng trong DB test. Chỉ chạy khi tên DB kết thúc bằng _test."""
    database = sa.make_url(url).database or ""
    if not database.endswith("_test"):
        raise RuntimeError(f"refusing to reset database {database!r}: name must end with _test")
    engine = sa.create_engine(url)
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA public CASCADE")
        conn.exec_driver_sql("CREATE SCHEMA public")
    engine.dispose()


BACKENDS = [
    "sqlite",
    pytest.param(
        "postgres",
        marks=pytest.mark.skipif(not PG_URL, reason="set ALPHADATA_TEST_PG_URL to test Postgres"),
    ),
]


@pytest.fixture(params=BACKENDS)
def db_url(request: pytest.FixtureRequest, tmp_path: Path) -> str:
    if request.param == "postgres":
        reset_postgres(PG_URL)
        return PG_URL
    return f"sqlite:///{(tmp_path / 'meta' / 'meta.db').as_posix()}"


@pytest.fixture
def store(db_url: str) -> Iterator[MetaStore]:
    s = MetaStore(db_url)
    yield s
    s.close()


@pytest.fixture
def pg_url() -> str:
    if not PG_URL:
        pytest.skip("set ALPHADATA_TEST_PG_URL to test Postgres")
    reset_postgres(PG_URL)
    return PG_URL
