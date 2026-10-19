from pathlib import Path
from typing import Any

import pytest

from alphadata.ingestion.pipeline import get_dataset, list_datasets, register
from alphadata.ingestion.pipeline import registry as reg


@pytest.fixture(autouse=True)
def empty_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reg, "_DATASETS", {})


@pytest.fixture
def make(fake_cls: Any, tmp_path: Path) -> Any:
    def build(**attrs: object) -> Any:
        ds = fake_cls(tmp_path)
        for k, v in attrs.items():
            setattr(ds, k, v)
        return ds

    return build


def test_register_and_lookup(make: Any) -> None:
    ds = register(make())
    assert get_dataset("fake_klines") is ds
    assert list_datasets() == [ds]


def test_unknown_dataset(make: Any) -> None:
    register(make())
    with pytest.raises(KeyError, match="known: \\['fake_klines'\\]"):
        get_dataset("nope")


@pytest.mark.parametrize(
    ("attrs", "message"),
    [
        ({"name": "Fake-Klines"}, "not snake_case"),
        ({"source": "Fake"}, "not snake_case"),
        ({"table": "nope"}, "not in schemas"),
        ({"partition_keys": ()}, "partition_keys"),
        ({"partition_keys": ("symbol", "symbol")}, "partition_keys"),
        ({"requires": ("NOT_A_KEY",)}, "unknown secrets"),
    ],
)
def test_invalid_declarations_rejected(make: Any, attrs: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        register(make(**attrs))


def test_duplicate_name_rejected(make: Any) -> None:
    register(make())
    with pytest.raises(ValueError, match="already registered"):
        register(make())


def test_missing_attribute_rejected(
    fake_cls: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delattr(fake_cls, "license_note")
    with pytest.raises(ValueError, match="license_note"):
        register(fake_cls(tmp_path))
