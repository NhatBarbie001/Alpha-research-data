"""So `schemas` với docs/04-data-model.md. Data model là nguồn sự thật; code phải khớp."""

import dataclasses
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path

import pytest

from alphadata.schemas import list_schemas
from alphadata.schemas.enums import ENUMS
from alphadata.schemas.silver import CRYPTO_BARS_1M
from alphadata.schemas.types import LAYER_COLUMNS, Column, TableSchema

DATA_MODEL = Path(__file__).resolve().parents[2] / "docs" / "04-data-model.md"
LAYER_BY_SECTION = {"4": "silver", "5": "reference", "6": "gold"}
LOGICAL_TYPES = ("list<string>", "timestamp", "float64", "int64", "int32", "string", "bool", "date")
EMPTY = ("", "—", "-", "không")

# Cột enum trong data model mục 8 tương ứng với tên enum trong code.
ENUM_DOC_REFS = {
    "session": "equity_bars_1m.session",
    "action_type": "corporate_actions.action_type",
    "frequency": "macro_series.frequency",
    "vintage_kind": "macro_observations.vintage_kind",
    "asset_class": "security_master.asset_class",
    "security_status": "security_master.status",
    "symbol_source": "security_symbols.symbol_source",
    "corporate_action_status": "corporate_actions.status",
    "universe_code": "universe_daily.universe_code",
}


@dataclass
class DocTable:
    name: str
    layer: str | None = None
    columns: list[tuple[str, str, bool]] | None = None
    key: list[str] = field(default_factory=list)
    partition: list[str] = field(default_factory=list)
    sort: list[str] = field(default_factory=list)


def cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def names(cell: str) -> list[str]:
    cleaned = cell.replace("`", "").strip()
    return [] if cleaned in EMPTY else [n.strip() for n in cleaned.split(",")]


def parse_type(cell: str) -> tuple[str, bool]:
    for t in LOGICAL_TYPES:
        if cell.replace("`", "").strip().startswith(t):
            return t, "(null)" in cell
    raise ValueError(f"unknown type cell {cell!r}")


def parse_data_model(text: str) -> tuple[dict[str, DocTable], list[tuple[list[str], set[str]]]]:
    lines = text.splitlines()
    tables: dict[str, DocTable] = {}
    enums: list[tuple[list[str], set[str]]] = []
    section = ""
    i = 0
    while i < len(lines):
        line = lines[i]
        if m := re.match(r"^## (\d+)\.", line):
            section = m.group(1)
        elif line.startswith("| Bảng | Khoá | Partition |"):
            i += 2
            while i < len(lines) and lines[i].startswith("|"):
                c = cells(lines[i])
                t = tables.setdefault(c[0].strip("`"), DocTable(c[0].strip("`")))
                t.key, t.partition, t.sort = names(c[1]), names(c[2]), names(c[3])
                i += 1
            continue
        elif section == "8" and line.startswith("| `"):
            c = cells(line)
            enums.append((re.findall(r"`([^`]+)`", c[0]), set(re.findall(r"`([^`]+)`", c[1]))))
        elif (m := re.match(r"^\*\*`(\w+)`\*\*(.*)$", line)) and section in LAYER_BY_SECTION:
            t = tables.setdefault(m.group(1), DocTable(m.group(1)))
            t.layer = LAYER_BY_SECTION[section]
            t.columns = parse_column_table(lines, i + 1)
        i += 1
    return tables, enums


def parse_column_table(lines: list[str], start: int) -> list[tuple[str, str, bool]] | None:
    """Bảng cột dạng `| Cột | Kiểu | Null |` ngay sau tiêu đề **`tên`**; dạng khác trả None."""
    i = start
    while i < len(lines) and not lines[i].startswith("|"):
        if lines[i].startswith(("**`", "#")):
            return None
        i += 1
    if i >= len(lines):
        return None
    header = cells(lines[i])
    if header[:2] != ["Cột", "Kiểu"]:
        return None
    null_idx = header.index("Null") if "Null" in header else None
    result: list[tuple[str, str, bool]] = []
    for row in lines[i + 2 :]:
        if not row.startswith("|"):
            break
        c = cells(row)
        col_names = names(c[0])
        types = [parse_type(t) for t in c[1].split(",")]
        if len(types) == 1:
            types = types * len(col_names)
        if len(types) != len(col_names):
            raise ValueError(f"cannot pair names and types in row {row!r}")
        for name, (typ, null_in_type) in zip(col_names, types, strict=True):
            nullable = null_in_type or (null_idx is not None and c[null_idx] == "✓")
            result.append((name, typ, nullable))
    return result


DOC_TABLES, DOC_ENUMS = parse_data_model(DATA_MODEL.read_text(encoding="utf-8"))


def render(columns: Sequence[tuple[str, str, bool]]) -> str:
    rows = [f"| {n} | {t} | {'✓' if nullable else ''} |" for n, t, nullable in columns]
    return "\n".join(["| Cột | Kiểu | Null |", "|---|---|---|", *rows])


def differences(schema: TableSchema) -> list[str]:
    doc = DOC_TABLES.get(schema.name)
    if doc is None:
        return [f"{schema.name} không có trong data model"]
    if doc.columns is None:
        return [f"{schema.name}: data model chưa có bảng cột dạng | Cột | Kiểu | Null |"]
    problems: list[str] = []
    if doc.layer != schema.layer:
        problems.append(f"layer: code={schema.layer} doc={doc.layer}")
    layer_cols = {c.name for c in LAYER_COLUMNS[schema.layer]}
    doc_cols = [c for c in doc.columns if c[0] not in layer_cols]
    code_cols = [(c.name, c.type, c.nullable) for c in schema.columns]
    if code_cols != doc_cols:
        problems.append(
            f"columns differ.\n--- code:\n{render(code_cols)}\n--- data model:\n{render(doc_cols)}"
        )
    for label, code, documented in (
        ("key", list(schema.key), doc.key),
        ("partition", list(schema.partition), doc.partition),
        ("sort", list(schema.sort), doc.sort),
    ):
        if code != documented:
            problems.append(f"{label}: code={code} doc={documented}")
    return problems


def test_data_model_parsed() -> None:
    assert DOC_TABLES["crypto_bars_1m"].columns is not None
    assert DOC_TABLES["crypto_bars_1m"].key == ["symbol", "ts_utc"]
    assert len(DOC_ENUMS) >= len(ENUM_DOC_REFS)


@pytest.mark.parametrize("schema", list_schemas(), ids=lambda s: s.name)
def test_schema_matches_data_model(schema: TableSchema) -> None:
    problems = differences(schema)
    assert not problems, "\n".join(problems)


@pytest.mark.parametrize("schema", list_schemas(), ids=lambda s: s.name)
def test_schema_enums_match_data_model(schema: TableSchema) -> None:
    for col in schema.columns:
        if col.enum is None:
            continue
        rows = [
            v for refs, v in DOC_ENUMS if any(fnmatch(f"{schema.name}.{col.name}", r) for r in refs)
        ]
        assert rows, f"{schema.name}.{col.name} không có trong data model mục 8"
        assert rows[0] == set(ENUMS[col.enum])


@pytest.mark.parametrize("enum_name", sorted(ENUMS))
def test_enum_values_match_data_model(enum_name: str) -> None:
    ref = ENUM_DOC_REFS[enum_name]
    rows = [v for refs, v in DOC_ENUMS if any(fnmatch(ref, r) for r in refs)]
    assert rows, f"{ref} không có trong data model mục 8"
    assert rows[0] == set(ENUMS[enum_name])


@pytest.mark.parametrize(
    "mutated",
    [
        dataclasses.replace(
            CRYPTO_BARS_1M,
            columns=tuple(
                Column(c.name, "float64") if c.name == "trades" else c
                for c in CRYPTO_BARS_1M.columns
            ),
        ),
        dataclasses.replace(
            CRYPTO_BARS_1M,
            columns=tuple(
                dataclasses.replace(c, nullable=c.name == "close") for c in CRYPTO_BARS_1M.columns
            ),
        ),
        dataclasses.replace(CRYPTO_BARS_1M, columns=CRYPTO_BARS_1M.columns[:-1]),
        dataclasses.replace(CRYPTO_BARS_1M, sort=("symbol", "ts_utc")),
    ],
    ids=["type", "nullable", "dropped-column", "sort"],
)
def test_contract_detects_drift(mutated: TableSchema) -> None:
    assert differences(mutated)
