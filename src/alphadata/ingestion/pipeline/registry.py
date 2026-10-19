import importlib
import re

from alphadata.core.settings import SECRET_NAMES
from alphadata.ingestion.pipeline.dataset import Dataset
from alphadata.schemas import get_schema

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_REQUIRED_ATTRS = (
    "name",
    "source",
    "table",
    "description",
    "source_url",
    "license_note",
    "partition_keys",
)

_DATASETS: dict[str, Dataset] = {}
_sources_loaded = False


def register(dataset: Dataset) -> Dataset:
    """Đăng ký lúc import sub package nguồn. Sai khai báo thì lỗi ngay lúc import."""
    problems = [f"missing attribute {a!r}" for a in _REQUIRED_ATTRS if not hasattr(dataset, a)]
    if problems:
        raise ValueError(f"invalid dataset {type(dataset).__name__}: {problems}")
    for label, value in (("name", dataset.name), ("source", dataset.source)):
        if not _NAME_RE.fullmatch(value):
            problems.append(f"{label} {value!r} is not snake_case")
    try:
        schema = get_schema(dataset.table)
        if schema.layer != "silver":
            problems.append(f"table {dataset.table!r} is {schema.layer}, not silver")
    except KeyError:
        problems.append(f"table {dataset.table!r} not in schemas")
    keys = list(dataset.partition_keys)
    if not keys or len(keys) != len(set(keys)) or not all(_NAME_RE.fullmatch(k) for k in keys):
        problems.append(f"partition_keys must be unique snake_case names, got {keys}")
    unknown = [s for s in dataset.requires if s not in SECRET_NAMES]
    if unknown:
        problems.append(f"unknown secrets {unknown}")
    if dataset.name in _DATASETS:
        problems.append(f"dataset {dataset.name!r} already registered")
    if problems:
        raise ValueError(f"invalid dataset {dataset.name!r}: " + "; ".join(problems))
    _DATASETS[dataset.name] = dataset
    return dataset


def get_dataset(name: str) -> Dataset:
    _load_sources()
    try:
        return _DATASETS[name]
    except KeyError:
        raise KeyError(f"unknown dataset {name!r}; known: {sorted(_DATASETS)}") from None


def list_datasets() -> list[Dataset]:
    _load_sources()
    return [_DATASETS[n] for n in sorted(_DATASETS)]


def _load_sources() -> None:
    # Import muộn: ingestion.sources import pipeline, nên không import được lúc nạp module này.
    global _sources_loaded
    if not _sources_loaded:
        importlib.import_module("alphadata.ingestion.sources")
        _sources_loaded = True
