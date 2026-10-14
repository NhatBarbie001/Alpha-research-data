"""Chuỗi macro từ FRED/ALFRED qua API chính thức (cần FRED_API_KEY, đăng ký miễn phí).

Point-in-time: API trả về toàn bộ lịch sử vintage. Mỗi dòng là một giá trị kèm khoảng thời gian
[available_from, available_to] mà giá trị đó là "số liệu hiện hành". Khi số liệu bị revise,
dòng cũ kết thúc và một dòng mới bắt đầu. Nhờ vậy query "tại ngày t ta biết gì" là chính xác.
"""

import json
import os
from datetime import date
from pathlib import Path

import pandas as pd

from alphadata.sources.base import Dataset, FetchContext
from alphadata.storage import RawFile, save_raw

API_URL = "https://api.stlouisfed.org/fred/series/observations"
PAGE_LIMIT = 100_000


class MissingApiKey(RuntimeError):
    pass


class FredSeries(Dataset):
    name = "fred_series"
    source = "fred"
    table = "macro_observations"
    description = ("Quan sát của các chuỗi macro FRED (lãi suất, lạm phát, việc làm, giá dầu...), "
                   "kèm lịch sử revise từ ALFRED để query point-in-time.")
    source_url = "https://fred.stlouisfed.org/docs/api/fred/series_observations.html"
    key_cols = ["series_id", "obs_date", "available_from"]
    path_pattern = "silver/macro_observations/fred/<SERIES_ID>.parquet"
    partition_keys = ["series_id"]
    notes = ("Giá trị biết tại ngày t: available_from <= t AND t <= available_to. "
             "available_from là NGÀY công bố (không có giờ); số liệu Mỹ thường ra 8:30 ET, "
             "nên an toàn nhất là chỉ dùng từ ngày hôm sau. Giá trị thiếu ('.') bị bỏ.")

    def fetch(self, ctx: FetchContext, partition: dict) -> list[RawFile]:
        api_key = os.getenv("FRED_API_KEY")
        if not api_key:
            raise MissingApiKey("Thiếu FRED_API_KEY trong .env (đăng ký miễn phí tại "
                                "https://fredaccount.stlouisfed.org/apikeys)")
        sid = partition["series_id"].upper()
        params = {
            "series_id": sid, "api_key": api_key, "file_type": "json",
            "realtime_start": "1776-07-04", "realtime_end": "9999-12-31",  # = toàn bộ vintage
            "limit": PAGE_LIMIT, "offset": 0,
        }
        observations = []
        while True:
            resp = ctx.session.get(API_URL, params=params, timeout=ctx.settings.http_timeout)
            resp.raise_for_status()
            page = resp.json()
            observations += page["observations"]
            if len(observations) >= page["count"] or not page["observations"]:
                break
            params["offset"] += PAGE_LIMIT

        content = json.dumps({"series_id": sid, "count": len(observations), "observations": observations}).encode()
        url = f"{API_URL}?series_id={sid}&realtime_start=1776-07-04&realtime_end=9999-12-31"  # không lưu key
        return [save_raw(ctx.raw_dir(self, partition), content, f"{sid}.json", url)]

    def normalize(self, raws: list[RawFile], partition: dict) -> pd.DataFrame:
        payload = json.loads(raws[0].path.read_bytes())
        df = pd.DataFrame(payload["observations"], columns=["realtime_start", "realtime_end", "date", "value"])
        out = pd.DataFrame({
            "series_id": partition["series_id"].upper(),
            "obs_date": pd.to_datetime(df["date"]).dt.date,
            "value": pd.to_numeric(df["value"], errors="coerce"),
            "available_from": [date.fromisoformat(s) for s in df["realtime_start"]],
            # 9999-12-31 (= vẫn hiện hành) vượt giới hạn Timestamp của pandas, nên dùng datetime.date
            "available_to": [date.fromisoformat(s) for s in df["realtime_end"]],
        })
        return out.dropna(subset=["value"]).reset_index(drop=True)

    def merge(self, existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
        # API luôn trả toàn bộ lịch sử vintage, nên thay thế cả file
        return new.sort_values(self.key_cols).reset_index(drop=True)

    def silver_path(self, silver_dir: Path, partition: dict) -> Path:
        return silver_dir / self.table / self.source / f"{partition['series_id'].upper()}.parquet"
