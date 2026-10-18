import shutil
from pathlib import Path

from alphadata.core.errors import DiskFullError
from alphadata.core.settings import Settings

_GB = 1024**3


def free_gb(path: Path) -> float:
    """GB trống của phân vùng chứa `path`; path chưa tồn tại thì đo ở thư mục cha gần nhất."""
    probe = path.absolute()
    while not probe.exists():
        probe = probe.parent
    return shutil.disk_usage(probe).free / _GB


def check_disk(settings: Settings) -> float:
    free = free_gb(settings.data_root)
    if free < settings.min_free_gb:
        raise DiskFullError(
            f"{free:.2f} GB free at {settings.data_root} < min_free_gb={settings.min_free_gb}"
        )
    return free
