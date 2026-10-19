# Spec: `cli`

> Code: `src/alphadata/cli/` · Tầng: vận hành · Phase 0
> Phụ thuộc: mọi module. Không module nào import `cli`.

## Mục đích

Chạy mọi việc của pipeline mà không cần Airflow: chạy một partition, backfill, xem catalog, lịch sử chạy, kết quả DQ, query nhanh. Dùng khi phát triển, debug, và backfill lần đầu trên server.

Entrypoint: `alphadata` (khai báo trong `pyproject.toml`) và `python -m alphadata`. Viết bằng `click`; test bằng `click.testing.CliRunner`.

**Tiến độ:** `run`, `reprocess`, `run-universe`, `datasets`, `universes`, `runs`, `catalog`, `disk`, `query` có từ P0-10, `backfill` từ P0-13; `dq` ở P1-07, `build` ở P2/P3.

## Lệnh

| Lệnh | Ví dụ | Gọi |
|---|---|---|
| `run <dataset> k=v ...` | `alphadata run binance_spot_klines_1m symbol=BTCUSDT period=2026-10-11` | `ingestion.pipeline.run_partition` |
| `reprocess <dataset> k=v ...` | `alphadata reprocess fred_series series_id=CPIAUCSL` | `ingestion.pipeline.reprocess_partition`: chạy lại từ raw đã lưu, không gọi mạng |
| `run-universe <universe> [--dataset D] [k=v]` | `alphadata run-universe crypto_pilot period=2026-10-11` | `run_partition` cho từng member đang hoạt động; universe có nhiều dataset thì bắt buộc `--dataset` |
| `backfill <dataset> [<universe>] [--start] [--end] [--resume ID] [--workers N]` | `alphadata backfill binance_spot_klines_1m crypto_pilot --start 2024-01` | `ingestion.pipeline.run_backfill`. `--start`/`--end` nhận `YYYY-MM` (đầu/cuối tháng) hoặc `YYYY-MM-DD`; `--end` mặc định là hôm qua. `--resume ID` thì bỏ universe và ngày |
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
  - **2 (sai tham số)**: token không phải `key=value`, khoá lặp lại, dataset hoặc universe không tồn tại, partition sai khoá hoặc bị `dataset.validate` từ chối, option sai. Kiểm trước khi gọi pipeline, nên không tạo run nào.
  - **1 (lỗi)**: lỗi trong lúc chạy (pipeline, DQ, checksum, raw không còn, SQL sai, file universe hỏng, disk dưới ngưỡng ở lệnh `disk`).
  - Message lỗi in ra stderr dạng `error: <Loại lỗi>: <message>`, đã che key. `-v/--verbose` in thêm traceback (cũng đã che key) và log mức DEBUG.
- `run-universe`: chạy từng member, member lỗi không dừng member khác, cuối cùng in bảng `member, status, rows_in, rows_out, error`. Exit 0 nếu tất cả thành công; 3 nếu mọi member không thành công đều là `NotAvailableYet` (cron thử lại sau); còn lại 1. Chỉ chạy member hoạt động vào ngày `--on`, hoặc ngày lấy từ `period`/`date` nếu đó là một ngày `YYYY-MM-DD`.
- `backfill`: in `backfill_id=<id> partitions_to_run=<n>` ra stderr **trước khi chạy** (bị kill vẫn còn id để `--resume`), cuối cùng in tổng kết lần chạy và toàn bộ backfill. Exit 0 nếu không còn partition nào chưa `done`; 3 nếu phần còn lại chỉ do `NotAvailableYet`; còn lại 1 (kể cả dừng vì hết disk), kèm gợi ý lệnh `--resume`.
- `query`: DuckDB trong bộ nhớ qua `access.views.connect` (view cho mọi bảng đã có file, timezone UTC), in tối đa `--limit` dòng (mặc định 100). SQL sai thì in danh sách view có sẵn.
- Log ra stderr, kết quả ra stdout (bảng chữ căn cột). Stdout/stderr được chuyển sang UTF-8 khi chạy, vì console Windows mặc định không in được tiếng Việt.
- Không hỏi tương tác (chạy được trong cron, nohup).
- `trigger = "cli"` hoặc `"backfill"` khi ghi `ingestion_runs`.

## Done when

- Mỗi lệnh có test gọi qua `CliRunner` với lake tạm.
- `alphadata --help` liệt kê đủ lệnh với mô tả tiếng Việt ngắn.
