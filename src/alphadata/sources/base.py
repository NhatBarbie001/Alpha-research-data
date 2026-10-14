"""Khung chung cho mọi nguồn dữ liệu.

Thêm nguồn mới = viết một class con của Dataset với 3 việc:
  fetch()      tải về, lưu nguyên bản vào raw. Không biến đổi gì.
  normalize()  đọc raw, trả về DataFrame đúng schema chuẩn.
  silver_path() file silver mà partition này ghi vào.
Có thể override checks() và merge() nếu nguồn có quy tắc riêng.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from alphadata import quality
from alphadata.config import Settings
from alphadata.storage import RawFile, upsert


class NotAvailableYet(RuntimeError):
    """Nguồn chưa có dữ liệu cho partition này (ví dụ file ngày hôm qua chưa được đăng). Thử lại sau."""


@dataclass
class FetchContext:
    settings: Settings
    session: requests.Session
    run_date: str  # ngày chạy (UTC), dùng để tách các lần tải raw khác nhau

    def raw_dir(self, dataset: "Dataset", partition: dict) -> Path:
        keys = dataset.partition_keys + sorted(k for k in partition if k not in dataset.partition_keys)
        part = "/".join(f"{k}={partition[k]}" for k in keys)
        return self.settings.raw_dir / dataset.source / dataset.name / part / self.run_date


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "alphadata/0.1 (internal research)"
    return s


class Dataset(ABC):
    name: str          # id của dataset, dùng trong CLI và Airflow
    source: str        # tên nguồn, ghi vào cột `source`
    table: str         # bảng silver
    description: str
    source_url: str
    key_cols: list[str]
    path_pattern: str  # mẫu đường dẫn silver, ghi vào catalog
    partition_keys: list[str]
    notes: str = ""
    keep_raw: bool = True  # False: xoá file raw sau khi ghi silver thành công (vẫn giữ manifest)

    @abstractmethod
    def fetch(self, ctx: FetchContext, partition: dict) -> list[RawFile]: ...

    @abstractmethod
    def normalize(self, raws: list[RawFile], partition: dict) -> pd.DataFrame: ...

    @abstractmethod
    def silver_path(self, silver_dir: Path, partition: dict) -> Path: ...

    def checks(self, df: pd.DataFrame, partition: dict) -> list[quality.CheckResult]:
        results = [quality.not_empty(df)]
        if len(df):
            results += [quality.no_null_keys(df, self.key_cols), quality.unique_keys(df, self.key_cols)]
        return results

    def merge(self, existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
        return upsert(existing, new, self.key_cols)

    def validate_partition(self, partition: dict) -> None:
        missing = [k for k in self.partition_keys if k not in partition]
        if missing:
            raise ValueError(f"{self.name}: thiếu tham số partition {missing}")
