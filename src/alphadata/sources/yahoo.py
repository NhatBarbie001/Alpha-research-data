"""Giá daily từ Yahoo Finance qua thư viện yfinance (không chính thức, chỉ dùng prototype/đối chiếu)."""

import io
from pathlib import Path

import pandas as pd

from alphadata import quality
from alphadata.sources.base import Dataset, FetchContext
from alphadata.storage import RawFile, save_raw

START_DATE = "2005-01-01"


def _safe(symbol: str) -> str:
    return symbol.upper().replace("^", "IDX_").replace("=", "_").replace("/", "_")


class YahooDaily(Dataset):
    name = "yahoo_daily"
    source = "yahoo"
    table = "equity_bars_1d"
    description = "Giá daily cổ phiếu, ETF, index (OHLCV, adj_close, cổ tức, split) từ Yahoo Finance."
    source_url = "https://finance.yahoo.com"
    key_cols = ["symbol", "date"]
    path_pattern = "silver/equity_bars_1d/yahoo/<SYMBOL>.parquet"
    partition_keys = ["symbol"]
    notes = ("open/high/low/close của Yahoo ĐÃ điều chỉnh split nhưng CHƯA điều chỉnh cổ tức; "
             "adj_close điều chỉnh cả hai. Yahoo tính lại toàn bộ lịch sử mỗi khi có cổ tức/split, "
             "nên mỗi lần chạy thay thế cả file. Nguồn không chính thức, có thể hỏng bất cứ lúc nào.")

    def fetch(self, ctx: FetchContext, partition: dict) -> list[RawFile]:
        import yfinance as yf

        symbol = partition["symbol"].upper()
        start = partition.get("start", START_DATE)
        hist = yf.Ticker(symbol).history(start=start, interval="1d", auto_adjust=False, actions=True)
        if hist.empty:
            raise RuntimeError(f"Yahoo không trả dữ liệu cho {symbol}")
        url = f"yfinance://history?symbol={symbol}&start={start}&interval=1d&auto_adjust=False"
        return [save_raw(ctx.raw_dir(self, partition), hist.to_csv().encode(), f"{_safe(symbol)}.csv", url)]

    def normalize(self, raws: list[RawFile], partition: dict) -> pd.DataFrame:
        df = pd.read_csv(raws[0].path, dtype={"Date": str})
        out = pd.DataFrame({
            "symbol": partition["symbol"].upper(),
            # Ngày theo giờ của sàn, lấy thẳng từ chuỗi để không lệch ngày khi đổi timezone
            "date": pd.to_datetime(df["Date"].str[:10]).dt.date,
        })
        for src, dst in [("Open", "open"), ("High", "high"), ("Low", "low"), ("Close", "close"),
                         ("Adj Close", "adj_close"), ("Volume", "volume"),
                         ("Dividends", "dividends"), ("Stock Splits", "splits")]:
            out[dst] = pd.to_numeric(df[src], errors="coerce").astype("float64") if src in df else float("nan")
        return out

    def checks(self, df: pd.DataFrame, partition: dict) -> list[quality.CheckResult]:
        results = super().checks(df, partition)
        if len(df):
            results += [quality.ohlc_consistent(df), quality.non_negative(df, ["volume"])]
        return results

    def merge(self, existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
        return new.sort_values(self.key_cols).reset_index(drop=True)

    def silver_path(self, silver_dir: Path, partition: dict) -> Path:
        return silver_dir / self.table / self.source / f"{_safe(partition['symbol'])}.parquet"
