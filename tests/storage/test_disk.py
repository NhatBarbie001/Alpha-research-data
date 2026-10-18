from pathlib import Path

import pytest

from alphadata.core.errors import DiskFullError
from alphadata.core.settings import Settings
from alphadata.storage.disk import check_disk


def test_returns_free_gb_when_above_threshold(tmp_path: Path) -> None:
    assert check_disk(Settings(data_root=tmp_path, min_free_gb=0.001)) > 0.001


def test_raises_when_threshold_above_free_space(tmp_path: Path) -> None:
    with pytest.raises(DiskFullError, match="min_free_gb"):
        check_disk(Settings(data_root=tmp_path, min_free_gb=10**9))


def test_data_root_not_created_yet(tmp_path: Path) -> None:
    assert check_disk(Settings(data_root=tmp_path / "a" / "b", min_free_gb=0.001)) > 0
