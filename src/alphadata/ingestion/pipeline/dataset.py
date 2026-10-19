from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from alphadata.core.http import HttpResponse, TimeoutSession, get_bytes
from alphadata.core.settings import Settings
from alphadata.quality import Check, default_checks
from alphadata.schemas import TableSchema, get_schema
from alphadata.storage.parquet import upsert
from alphadata.storage.raw import RawFile, read_raw, save_raw

Partition = dict[str, str]


@dataclass(frozen=True)
class FetchContext:
    """Những gì `fetch` được dùng: HTTP có retry/rate limit và chỗ lưu raw của partition này."""

    settings: Settings
    session: TimeoutSession
    run_date: date  # ngày UTC lúc chạy, là <fetch_date> trong đường dẫn raw
    raw_dir: Path

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        host_rate: float | None = None,
    ) -> HttpResponse:
        return get_bytes(self.session, url, params=params, headers=headers, host_rate=host_rate)

    def save(self, content: bytes, filename: str, url: str) -> RawFile:
        return save_raw(self.raw_dir, content, filename, url)


class Dataset(ABC):
    """Hợp đồng của một connector. Mỗi dataset ghi đúng một bảng silver.

    Lớp con khai báo các thuộc tính dưới dạng thuộc tính lớp và gọi `register(MyDataset())`.
    """

    name: str  # id duy nhất: CLI, universe, schedules
    source: str  # giá trị cột `source`
    table: str  # bảng silver trong schemas
    description: str
    source_url: str
    license_note: str  # tóm tắt điều khoản, ghi vào catalog
    partition_keys: tuple[str, ...]  # ví dụ ("symbol", "date")
    keep_raw: bool = True  # False: xoá raw sau khi ghi silver (giữ manifest)
    requires: tuple[str, ...] = ()  # tên secret cần có
    refresh_schedule: str = "manual"  # cron UTC, khớp orchestration/schedules.py
    catalog_notes: str = ""  # lưu ý ghi vào catalog (ví dụ Yahoo close đã điều chỉnh split)

    @property
    def schema(self) -> TableSchema:
        return get_schema(self.table)

    @abstractmethod
    def fetch(self, ctx: FetchContext, partition: Partition) -> list[RawFile]:
        """Chỉ tải và lưu nguyên bản qua `ctx.save`. Chưa có dữ liệu thì raise NotAvailableYet."""

    @abstractmethod
    def normalize(self, raws: list[RawFile], partition: Partition) -> pd.DataFrame:
        """Chỉ đọc raw đã lưu (qua `self.read`), không gọi mạng.

        Trả đủ cột của bảng trừ `source`, `ingested_at` (pipeline tự thêm). Format lạ thì raise
        SourceFormatChanged.
        """

    def validate(self, partition: Partition) -> None:  # noqa: B027 - hook tuỳ chọn
        """Kiểm giá trị partition (raise ValueError) trước khi ghi run nào. Mặc định không kiểm."""

    def checks(self, partition: Partition) -> list[Check]:
        return default_checks(self.schema)

    def merge(self, existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
        return upsert(existing, new, self.schema.key)

    def backfill_partitions(self, member: str, start: date, end: date) -> list[Partition]:
        """Chia lịch sử [start, end] (cả hai đầu tính) của một member thành các partition.

        Pipeline đã cắt khoảng theo member và hôm qua (UTC). Nguồn có file tháng nên trả partition
        tháng cho các tháng trọn vẹn đã được đăng.
        """
        raise NotImplementedError(f"{self.name} does not support backfill")

    def read(self, raw: RawFile) -> bytes:
        """Đọc raw có kiểm checksum với manifest."""
        return read_raw(raw.path)
