"""Dòng lệnh. Ví dụ:

  alphadata run binance_spot_klines_1m symbol=BTCUSDT date=2026-10-11
  alphadata run fred_series series_id=DGS10
  alphadata run yahoo_daily symbol=SPY
  alphadata backfill-binance --symbols BTCUSDT,ETHUSDT --start 2024-01 --end 2024-03
  alphadata pilot
  alphadata query "SELECT symbol, count(*) FROM crypto_bars_1m GROUP BY 1"
  alphadata catalog
  alphadata runs
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

from alphadata import universe
from alphadata.config import get_settings
from alphadata.meta import MetaStore
from alphadata.pipeline import run_partition
from alphadata.sources.base import NotAvailableYet


def _parse_kv(items: list[str]) -> dict:
    out = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"Tham số phải có dạng key=value: {item}")
        k, v = item.split("=", 1)
        out[k] = v
    return out


def _months(start: str, end: str) -> list[str]:
    return [p.strftime("%Y-%m") for p in pd.period_range(start, end, freq="M")]


def _run_many(jobs: list[tuple[str, dict]]) -> tuple[int, int]:
    """Chạy lần lượt, lỗi một partition không chặn các partition khác. Trả về (số lỗi, số chưa có dữ liệu)."""
    failed = pending = 0
    for dataset, partition in jobs:
        try:
            res = run_partition(dataset, partition)
            warn = f" ({len(res.warnings)} cảnh báo)" if res.warnings else ""
            print(f"OK    {dataset} {partition}: +{res.rows_in} dòng{warn}")
        except NotAvailableYet as e:
            pending += 1
            print(f"CHƯA CÓ {dataset} {partition}: {e}")
        except Exception as e:
            failed += 1
            print(f"LỖI   {dataset} {partition}: {type(e).__name__}: {e}")
    print(f"\nXong {len(jobs)} partition, {failed} lỗi, {pending} chưa có dữ liệu.")
    return failed, pending


def main(argv: list[str] | None = None) -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # console Windows không mặc định UTF-8
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="alphadata")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="Chạy một partition")
    r.add_argument("dataset")
    r.add_argument("params", nargs="+", help="key=value")

    b = sub.add_parser("backfill-binance", help="Tải lịch sử Binance theo tháng")
    b.add_argument("--symbols", default=",".join(universe.PILOT_CRYPTO))
    b.add_argument("--start", required=True, help="YYYY-MM")
    b.add_argument("--end", required=True, help="YYYY-MM")

    pl = sub.add_parser("pilot", help="Chạy toàn bộ universe pilot (FRED, Yahoo, Binance N ngày gần nhất)")
    pl.add_argument("--days", type=int, default=3)

    q = sub.add_parser("query", help="Chạy SQL trên các view silver")
    q.add_argument("sql")

    sub.add_parser("catalog", help="Liệt kê bảng và nguồn")
    sub.add_parser("runs", help="Các lần chạy gần nhất")

    args = p.parse_args(argv)

    if args.cmd == "run":
        failed, pending = _run_many([(args.dataset, _parse_kv(args.params))])
        return 1 if failed or pending else 0  # chạy lẻ: "chưa có" cũng là lỗi để scheduler retry

    if args.cmd == "backfill-binance":
        jobs = [("binance_spot_klines_1m", {"symbol": s.strip().upper(), "month": m})
                for s in args.symbols.split(",") for m in _months(args.start, args.end)]
        return 1 if _run_many(jobs)[0] else 0

    if args.cmd == "pilot":
        today = datetime.now(timezone.utc).date()
        days = [(today - timedelta(days=i)).isoformat() for i in range(args.days, 0, -1)]
        jobs = [("fred_series", {"series_id": s}) for s in universe.PILOT_FRED]
        jobs += [("yahoo_daily", {"symbol": s}) for s in universe.PILOT_YAHOO]
        jobs += [("binance_spot_klines_1m", {"symbol": s, "date": d}) for s in universe.PILOT_CRYPTO for d in days]
        return 1 if _run_many(jobs)[0] else 0

    if args.cmd == "query":
        from alphadata.query import connect

        with pd.option_context("display.width", 200, "display.max_columns", 20):
            print(connect().sql(args.sql).df())
        return 0

    meta = MetaStore(get_settings().meta_db_url)
    rows = meta.catalog() if args.cmd == "catalog" else meta.recent_runs()
    with pd.option_context("display.width", 200, "display.max_columns", 20, "display.max_colwidth", 60):
        print(pd.DataFrame(rows).drop(columns=["updated_at"], errors="ignore").to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
