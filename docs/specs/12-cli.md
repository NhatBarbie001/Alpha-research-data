# Spec: `cli`

> Code: `src/alphadata/cli/` · Tầng: vận hành · Phase 0
> Phụ thuộc: mọi module. Không module nào import `cli`.

## Mục đích

Chạy mọi việc của pipeline mà không cần Airflow: chạy một partition, backfill, xem catalog, lịch sử chạy, kết quả DQ, query nhanh. Dùng khi phát triển, debug, và backfill lần đầu trên server.

Entrypoint: `alphadata` (khai báo trong `pyproject.toml`) và `python -m alphadata`.

## Lệnh

| Lệnh | Ví dụ | Gọi |
|---|---|---|
| `run <dataset> k=v ...` | `alphadata run binance_spot_klines_1m symbol=BTCUSDT period=2026-10-11` | `ingestion.pipeline.run_partition` |
| `reprocess <dataset> k=v ...` | `alphadata reprocess fred_series series_id=CPIAUCSL` | `ingestion.pipeline.reprocess_partition`: chạy lại từ raw đã lưu, không gọi mạng |
| `run-universe <universe> [k=v]` | `alphadata run-universe crypto_pilot period=2026-10-11` | `run_partition` cho từng member |
| `backfill <dataset> <universe> --start --end [--resume ID]` | `alphadata backfill alpaca_bars_1m us_etf_core --start 2024-01 --end 2026-09` | `ingestion.pipeline.run_backfill` |
| `datasets` | | Liệt kê dataset đã đăng ký, nguồn, bảng, secret cần có |
| `universes` | | Liệt kê universe và số member |
| `catalog [--layer silver\|gold]` | | `metadata.catalog` |
| `runs [--dataset] [--status] [--limit]` | `alphadata runs --status failed` | `metadata.recent_runs` |
| `dq report [--since]` | | Check fail gần đây, gom theo bảng |
| `dq post-checks` | | Chạy check sau khi ghi |
| `build reference \| gold [--table]` | `alphadata build gold --table bars_1d` | Job dựng `reference.*`, `gold.*` |
| `query "<SQL>"` | `alphadata query "SELECT count(*) FROM crypto_bars_1m"` | `access.views.connect`, in kết quả dạng bảng |
| `disk` | | Dung lượng trống, dung lượng từng tầng của lake |

## Quy ước

- Exit code: 0 thành công, 1 lỗi, 2 sai tham số, 3 `NotAvailableYet` (để script bên ngoài biết mà thử lại).
- Log ra stderr, kết quả ra stdout.
- Không hỏi tương tác (chạy được trong cron, nohup).
- `trigger = "cli"` hoặc `"backfill"` khi ghi `ingestion_runs`.

## Done when

- Mỗi lệnh có test gọi qua `CliRunner` với lake tạm.
- `alphadata --help` liệt kê đủ lệnh với mô tả tiếng Việt ngắn.
