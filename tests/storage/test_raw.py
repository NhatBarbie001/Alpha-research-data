import hashlib
import json
from pathlib import Path

import pytest

from alphadata.storage.raw import (
    RawChecksumError,
    delete_raw,
    manifest_path,
    read_manifest,
    read_raw,
    save_raw,
)

CONTENT = b"open_time,open,high,low,close\n1735689600000,1,2,0.5,1.5\n"


def test_save_raw_writes_file_and_manifest(tmp_path: Path) -> None:
    raw = save_raw(tmp_path / "d", CONTENT, "BTCUSDT-1m.csv", "https://x/a.csv")
    assert raw.path.read_bytes() == CONTENT
    assert raw.sha256 == hashlib.sha256(CONTENT).hexdigest()
    assert raw.bytes == len(CONTENT)
    assert raw.fetched_at.endswith("+00:00")

    manifest = json.loads(manifest_path(raw.path).read_text(encoding="utf-8"))
    assert manifest == {
        "path": "BTCUSDT-1m.csv",
        "url": "https://x/a.csv",
        "sha256": raw.sha256,
        "bytes": raw.bytes,
        "fetched_at": raw.fetched_at,
    }
    assert read_manifest(raw.path) == raw


def test_manifest_never_contains_key(tmp_path: Path) -> None:
    raw = save_raw(tmp_path, b"{}", "cpi.json", "https://api/x?series_id=CPI&api_key=SECRET123")
    assert raw.url == "https://api/x?series_id=CPI&api_key=***"
    assert "SECRET123" not in manifest_path(raw.path).read_text(encoding="utf-8")


def test_resave_same_dir_overwrites_without_leftovers(tmp_path: Path) -> None:
    save_raw(tmp_path, b"old", "f.json", "u")
    save_raw(tmp_path, b"new", "f.json", "u")
    assert (tmp_path / "f.json").read_bytes() == b"new"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["f.json", "f.json.manifest.json"]


def test_read_raw_detects_corruption(tmp_path: Path) -> None:
    raw = save_raw(tmp_path, CONTENT, "f.csv", "u")
    assert read_raw(raw.path) == CONTENT
    raw.path.write_bytes(CONTENT + b"tampered")
    with pytest.raises(RawChecksumError):
        read_raw(raw.path)


def test_delete_raw_keeps_manifest_and_is_idempotent(tmp_path: Path) -> None:
    raw = save_raw(tmp_path, CONTENT, "f.csv", "u")
    delete_raw(raw.path)
    delete_raw(raw.path)
    assert not raw.path.exists()
    assert manifest_path(raw.path).exists()


@pytest.mark.parametrize("filename", ["", ".hidden", "../escape.csv", "a/b.csv"])
def test_invalid_filename_rejected(tmp_path: Path, filename: str) -> None:
    with pytest.raises(ValueError):
        save_raw(tmp_path, CONTENT, filename, "u")
