import re
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from alphadata.core.settings import Settings

Layer = Literal["silver", "gold"]
PART_FILE = "part.parquet"

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def raw_dir(
    settings: Settings,
    source: str,
    dataset: str,
    partition: Mapping[str, object],
    *,
    partition_keys: Sequence[str],
    fetch_date: date,
) -> Path:
    """raw/<source>/<dataset>/<k=v>.../<fetch_date>; khoá khai báo trước, còn lại theo alphabet."""
    _check_name(source, "source")
    _check_name(dataset, "dataset")
    return (
        settings.raw_dir.joinpath(source, dataset, *partition_segments(partition, partition_keys))
        / fetch_date.isoformat()
    )


def table_dir(settings: Settings, layer: Layer, table: str) -> Path:
    _check_name(table, "table")
    root = {"silver": settings.silver_dir, "gold": settings.gold_dir}[layer]
    return root / table


def table_file(
    settings: Settings, layer: Layer, table: str, partition: Mapping[str, object] | None = None
) -> Path:
    """<layer>/<table>/<k=v>.../part.parquet; thứ tự k=v giữ nguyên thứ tự của `partition`."""
    segments = partition_segments(partition or {}, list(partition or {}))
    return table_dir(settings, layer, table).joinpath(*segments) / PART_FILE


def checkpoint_file(settings: Settings, dataset: str, backfill_id: str) -> Path:
    _check_name(dataset, "dataset")
    if not _ID_RE.fullmatch(backfill_id):
        raise ValueError(f"invalid backfill_id: {backfill_id!r}")
    return settings.checkpoint_dir / dataset / f"{backfill_id}.json"


def partition_segments(partition: Mapping[str, object], key_order: Sequence[str]) -> list[str]:
    unknown_order = [k for k in key_order if k not in partition]
    if unknown_order:
        raise ValueError(f"partition is missing keys: {unknown_order}")
    rest = sorted(k for k in partition if k not in key_order)
    return [f"{_key(k)}={_value(partition[k])}" for k in [*key_order, *rest]]


def _key(key: str) -> str:
    _check_name(key, "partition key")
    return key


def _value(value: object) -> str:
    text = value.isoformat() if isinstance(value, date) else str(value)
    if text in ("", ".", ".."):
        raise ValueError(f"invalid partition value: {value!r}")
    # Mã như "EURUSD=X", "^GSPC" có ký tự không an toàn cho đường dẫn hive: mã hoá %XX.
    return quote(text, safe="")


def _check_name(name: str, what: str) -> None:
    if not _NAME_RE.fullmatch(name):
        raise ValueError(f"invalid {what}: {name!r} (expected lowercase snake_case)")
