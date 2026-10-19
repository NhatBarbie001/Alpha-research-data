"""Đối chiếu config/universe/ với registry dataset.

universe không import ingestion (sai hướng phụ thuộc), nên việc kiểm dataset có tồn tại và
member hợp lệ với connector nằm ở test này: file universe sai thì CI fail.
"""

from pathlib import Path

import pytest

from alphadata.ingestion.pipeline import get_dataset
from alphadata.universe import list_universes, load

CONFIG = Path(__file__).resolve().parents[2] / "config" / "universe"
# Giá trị mẫu cho các khoá partition không do universe điền, để gọi dataset.validate.
SAMPLE_VALUES = {"period": "2024-01", "date": "2024-01-02", "month": "2024-01"}


def test_pilot_universes_present() -> None:
    assert "crypto_pilot" in list_universes(CONFIG)


@pytest.mark.parametrize("name", list_universes(CONFIG))
def test_universe_matches_registry(name: str) -> None:
    universe = load(name, CONFIG)
    for dataset_name in universe.datasets:
        dataset = get_dataset(dataset_name)  # KeyError nếu dataset không tồn tại
        assert universe.key in dataset.partition_keys, (
            f"{name}: key {universe.key!r} not in {dataset_name}.partition_keys"
        )
        others = [k for k in dataset.partition_keys if k != universe.key]
        missing = [k for k in others if k not in SAMPLE_VALUES]
        assert not missing, f"add sample values for {missing} in SAMPLE_VALUES"
        for partition in universe.partitions({k: SAMPLE_VALUES[k] for k in others}):
            dataset.validate(partition)
