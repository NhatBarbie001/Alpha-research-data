import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from alphadata.core.errors import AlphadataError
from alphadata.core.http import mask_secrets

MANIFEST_SUFFIX = ".manifest.json"


class RawChecksumError(AlphadataError):
    """File raw trên disk không khớp checksum trong manifest (hỏng hoặc bị sửa)."""


@dataclass(frozen=True)
class RawFile:
    path: Path
    url: str  # đã che key
    sha256: str
    bytes: int
    fetched_at: str  # ISO UTC


def manifest_path(raw_path: Path) -> Path:
    return raw_path.with_name(raw_path.name + MANIFEST_SUFFIX)


def save_raw(raw_dir: Path, content: bytes, filename: str, url: str) -> RawFile:
    """Ghi nguyên bản + manifest, cả hai atomic. Cùng thư mục (cùng fetch_date) thì ghi đè."""
    if not filename or filename.startswith(".") or Path(filename).name != filename:
        raise ValueError(f"invalid raw filename: {filename!r}")
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / filename
    raw = RawFile(
        path=path,
        url=mask_secrets(url),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        fetched_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )
    _write_atomic(path, content)
    manifest = {**asdict(raw), "path": filename}
    _write_atomic(
        manifest_path(path), json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
    )
    return raw


def read_manifest(raw_path: Path) -> RawFile:
    data = json.loads(manifest_path(raw_path).read_text(encoding="utf-8"))
    return RawFile(
        path=raw_path,
        url=data["url"],
        sha256=data["sha256"],
        bytes=data["bytes"],
        fetched_at=data["fetched_at"],
    )


def read_raw(raw_path: Path) -> bytes:
    """Đọc raw và kiểm checksum với manifest, để ETL chạy lại không dùng nhầm file hỏng."""
    content = raw_path.read_bytes()
    expected = read_manifest(raw_path).sha256
    actual = hashlib.sha256(content).hexdigest()
    if actual != expected:
        raise RawChecksumError(f"{raw_path}: sha256 {actual} != manifest {expected}")
    return content


def delete_raw(raw_path: Path) -> None:
    """Xoá file raw, giữ manifest. Gọi lại khi file đã xoá thì không lỗi."""
    raw_path.unlink(missing_ok=True)


def _write_atomic(path: Path, content: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    try:
        with tmp.open("wb") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
