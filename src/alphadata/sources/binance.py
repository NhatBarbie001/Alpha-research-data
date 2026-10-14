"""Binance spot klines 1 phút, từ file công khai data.binance.vision (không cần API key)."""

import zipfile
from pathlib import Path

import pandas as pd

from alphadata import quality
from alphadata.sources.base import Dataset, FetchContext, NotAvailableYet
from alphadata.storage import RawFile, save_raw, sha256_bytes

BASE_URL = "https://data.binance.vision/data/spot"
RAW_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "trades", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
]


class ChecksumMismatch(RuntimeError):
    pass


class BinanceSpotKlines1m(Dataset):
    name = "binance_spot_klines_1m"
    source = "binance"
    table = "crypto_bars_1m"
    description = "Nến 1 phút spot Binance (OHLCV, số lệnh, taker volume). Thời gian là thời điểm mở nến, UTC."
    source_url = "https://data.binance.vision"
    key_cols = ["symbol", "ts_utc"]
    path_pattern = "silver/crypto_bars_1m/<SYMBOL>/<YYYY>/<MM>.parquet"
    partition_keys = ["symbol"]  # cộng thêm `date` (YYYY-MM-DD) hoặc `month` (YYYY-MM)
    notes = ("File từ 2025-01-01 dùng timestamp micro-giây, trước đó mili-giây; parser tự nhận. "
             "Checksum SHA-256 của Binance được kiểm tra khi tải. Raw zip bị xoá sau khi ghi silver.")
    keep_raw = False

    def validate_partition(self, partition: dict) -> None:
        super().validate_partition(partition)
        if ("date" in partition) == ("month" in partition):
            raise ValueError("Cần đúng một trong hai: date=YYYY-MM-DD hoặc month=YYYY-MM")

    @staticmethod
    def _period(partition: dict) -> tuple[str, str]:
        return ("daily", partition["date"]) if "date" in partition else ("monthly", partition["month"])

    @staticmethod
    def _range(partition: dict) -> tuple[pd.Timestamp, pd.Timestamp]:
        if "date" in partition:
            start = pd.Timestamp(partition["date"], tz="UTC")
            return start, start + pd.Timedelta(days=1)
        start = pd.Timestamp(partition["month"] + "-01", tz="UTC")
        return start, start + pd.offsets.MonthBegin(1)

    def fetch(self, ctx: FetchContext, partition: dict) -> list[RawFile]:
        freq, period = self._period(partition)
        symbol = partition["symbol"].upper()
        filename = f"{symbol}-1m-{period}.zip"
        url = f"{BASE_URL}/{freq}/klines/{symbol}/1m/{filename}"

        resp = ctx.session.get(url, timeout=ctx.settings.http_timeout)
        if resp.status_code == 404:
            raise NotAvailableYet(url)
        resp.raise_for_status()

        checksum = ctx.session.get(url + ".CHECKSUM", timeout=ctx.settings.http_timeout)
        checksum.raise_for_status()
        expected = checksum.text.split()[0]
        if sha256_bytes(resp.content) != expected:
            raise ChecksumMismatch(f"{url}: checksum không khớp")

        return [save_raw(ctx.raw_dir(self, partition), resp.content, filename, url)]

    def normalize(self, raws: list[RawFile], partition: dict) -> pd.DataFrame:
        with zipfile.ZipFile(raws[0].path) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df = pd.read_csv(f, header=None, names=RAW_COLUMNS, dtype=str)
        if not df.iloc[0]["open_time"].isdigit():  # một số file có dòng header
            df = df.iloc[1:]

        open_time = pd.to_numeric(df["open_time"])
        unit = "us" if open_time.max() >= 10**15 else "ms"
        out = pd.DataFrame({
            "symbol": partition["symbol"].upper(),
            "ts_utc": pd.to_datetime(open_time, unit=unit, utc=True).astype("datetime64[us, UTC]"),
        })
        for col in ["open", "high", "low", "close", "volume", "quote_volume",
                    "taker_buy_base_volume", "taker_buy_quote_volume"]:
            out[col] = pd.to_numeric(df[col]).astype("float64")
        out["trades"] = pd.to_numeric(df["trades"]).astype("int64")
        return out.reset_index(drop=True)

    def checks(self, df: pd.DataFrame, partition: dict) -> list[quality.CheckResult]:
        results = super().checks(df, partition)
        if not len(df):
            return results
        start, end = self._range(partition)
        expected_bars = int((end - start) / pd.Timedelta(minutes=1))
        return results + [
            quality.ts_in_range(df, "ts_utc", start, end),
            quality.ohlc_consistent(df),
            quality.non_negative(df, ["volume", "quote_volume", "trades"]),
            quality.row_count(df, expected_bars),  # thiếu nến khi sàn bảo trì là bình thường, nên chỉ warn
        ]

    def silver_path(self, silver_dir: Path, partition: dict) -> Path:
        _, period = self._period(partition)
        year, month = period[:4], period[5:7]
        return silver_dir / self.table / partition["symbol"].upper() / year / f"{month}.parquet"
