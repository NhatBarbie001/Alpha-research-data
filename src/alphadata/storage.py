"""Đọc/ghi file: raw (giữ nguyên bản gốc) và silver (Parquet đã chuẩn hoá)."""

import hashlib
import json
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from alphadata.config import Settings


class DiskFullError(RuntimeError):
    pass


@dataclass
class RawFile:
    path: Path
    url: str
    sha256: str
    bytes: int
    fetched_at: str


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def check_disk(settings: Settings) -> float:
    """Dừng pipeline nếu disk sắp đầy, để không làm hỏng các service khác trên server."""
    settings.data_root.mkdir(parents=True, exist_ok=True)
    free_gb = shutil.disk_usage(settings.data_root).free / 1024**3
    if free_gb < settings.min_free_gb:
        raise DiskFullError(
            f"Disk chỉ còn {free_gb:.1f} GB trống (ngưỡng {settings.min_free_gb} GB). Dừng pipeline."
        )
    return free_gb


def save_raw(raw_dir: Path, content: bytes, filename: str, url: str) -> RawFile:
    """Lưu nguyên bản file tải về, kèm manifest (URL, checksum, thời điểm tải)."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / filename
    path.write_bytes(content)
    raw = RawFile(
        path=path,
        url=url,
        sha256=sha256_bytes(content),
        bytes=len(content),
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )
    manifest = {**asdict(raw), "path": path.name}
    (raw_dir / f"{filename}.manifest.json").write_text(json.dumps(manifest, indent=2))
    return raw


def read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    return pd.read_parquet(path) if path.exists() else None


def write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    """Ghi ra file tạm rồi đổi tên, để file silver không bao giờ bị ghi dở."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    table = pa.Table.from_pandas(df, preserve_index=False)
    float_cols = [f.name for f in table.schema if pa.types.is_floating(f.type)]
    # zstd mức 9 + byte_stream_split cho cột số thực: nhỏ hơn ~20% so với mặc định (đo trên nến 1 phút)
    pq.write_table(table, tmp, compression="zstd", compression_level=9,
                   use_byte_stream_split=float_cols or False)
    os.replace(tmp, path)


def upsert(existing: pd.DataFrame | None, new: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    """Gộp dữ liệu mới vào dữ liệu cũ. Trùng khoá thì giữ bản mới."""
    if existing is None or existing.empty:
        merged = new
    else:
        merged = pd.concat([existing, new], ignore_index=True)
        merged = merged.drop_duplicates(subset=key_cols, keep="last")
    return merged.sort_values(key_cols).reset_index(drop=True)
