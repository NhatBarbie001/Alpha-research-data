import re
from pathlib import Path

import pytest
import sqlalchemy as sa

from alphadata.metadata import MetaStore
from alphadata.metadata import tables as t
from alphadata.metadata.schema import init_schema, migration_files

DATA_MODEL = Path(__file__).resolve().parents[2] / "docs" / "04-data-model.md"
DOC_TYPES = {
    "uuid": sa.Uuid,
    "text": sa.Text,
    "jsonb": sa.JSON,
    "timestamptz": sa.DateTime,
    "bigint": sa.BigInteger,
    "bigserial": sa.BigInteger,
    "boolean": sa.Boolean,
    "char(64)": sa.CHAR,
    "integer": sa.Integer,
}


def doc_tables() -> dict[str, list[tuple[str, str, bool | None]]]:
    """Mục 7 của data model: {bảng: [(cột, kiểu, nullable; None nếu không có cột Null)]}."""
    text = DATA_MODEL.read_text(encoding="utf-8")
    section = text.split("## 7.")[1].split("## 8.")[0]
    result: dict[str, list[tuple[str, str, bool | None]]] = {}
    for block in re.split(r"^\*\*`", section, flags=re.M)[1:]:
        name = block.split("`", 1)[0]
        rows = [line for line in block.splitlines() if line.startswith("|")]
        header = [c.strip() for c in rows[0].strip("|").split("|")]
        null_idx = header.index("Null") if "Null" in header else None
        cols: list[tuple[str, str, bool | None]] = []
        for row in rows[2:]:
            cells = [c.strip() for c in row.strip("|").split("|")]
            nullable = None if null_idx is None else cells[null_idx] == "✓"
            cols += [(n.strip(), cells[1], nullable) for n in cells[0].split(",")]
        result[name] = cols
    return result


@pytest.mark.parametrize("name", sorted(doc_tables()))
def test_tables_match_data_model(name: str) -> None:
    table = t.metadata.tables[name]
    documented = doc_tables()[name]
    assert [c for c, _, _ in documented] == [c.name for c in table.columns]
    for col, typ, nullable in documented:
        sa_col = table.columns[col]
        assert isinstance(sa_col.type, DOC_TYPES[typ]), (col, typ, sa_col.type)
        if nullable is not None and not sa_col.primary_key:
            assert sa_col.nullable == nullable, col


def test_every_lake_metadata_table_is_documented() -> None:
    assert set(t.metadata.tables) - set(doc_tables()) == {"schema_migrations"}


def write(directory: Path, name: str, sql: str) -> None:
    directory.mkdir(exist_ok=True)
    (directory / name).write_text(sql, encoding="utf-8")


def test_new_database_is_stamped_not_migrated(db_url: str, tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    write(migrations, "0001_add_note.sql", "ALTER TABLE raw_files ADD COLUMN note TEXT;")
    store = MetaStore(db_url, migrations_dir=migrations)
    with store.engine.connect() as conn:
        versions = list(conn.scalars(sa.select(t.schema_migrations.c.version)))
        columns = {c["name"] for c in sa.inspect(conn).get_columns("raw_files")}
    assert versions == ["0001_add_note"]
    assert "note" not in columns  # DB mới tạo từ tables.py, không chạy migration
    store.close()


def test_existing_database_runs_pending_migrations(db_url: str, tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    MetaStore(db_url, migrations_dir=migrations).close()

    write(
        migrations,
        "0001_add_note.sql",
        "-- thêm cột ghi chú\nALTER TABLE raw_files ADD COLUMN note TEXT;\n"
        "CREATE INDEX ix_raw_files_note ON raw_files (note);",
    )
    engine = sa.create_engine(db_url)
    assert init_schema(engine, migrations) == ["0001_add_note"]
    assert init_schema(engine, migrations) == []
    with engine.connect() as conn:
        assert "note" in {c["name"] for c in sa.inspect(conn).get_columns("raw_files")}
    engine.dispose()


def test_migration_file_names_validated(tmp_path: Path) -> None:
    write(tmp_path, "add_note.sql", "SELECT 1;")
    with pytest.raises(ValueError, match="NNNN"):
        migration_files(tmp_path)


def test_packaged_migrations_load() -> None:
    assert all(re.fullmatch(r"\d{4}_[a-z0-9_]+", v) for v, _ in migration_files())
