# 03. Hệ thống hoạt động thế nào

> Đọc sau [02-architecture.md](02-architecture.md). Tài liệu kiến trúc mô tả **có những module nào**; tài liệu này mô tả **chúng gọi nhau thế nào khi chạy**: thành phần nào chạy ở đâu, mỗi luồng công việc đi qua module nào theo thứ tự nào, dữ liệu nằm ở đâu sau mỗi bước, và chuyện gì xảy ra khi lỗi.
> Tiếp theo: thiết kế từng bảng ở [04-data-model.md](04-data-model.md), chi tiết hàm của từng module ở [specs/](specs/).

---

## 1. Thành phần khi chạy trên server bee

```
   Internet: 17 nguồn miễn phí                         Team (qua SSH tunnel): UI Airflow, CLI
              ▲                                                         │
┌─────────────┼───────────────────── server bee ────────────────────────┼──────────────────┐
│             │                                                         ▼                  │
│  ┌──────────┴───────────────────┐  @task.external_python  ┌──────────────────────────┐   │
│  │ virtualenv alphadata         │ ◄────────────────────── │ Airflow 3                │   │
│  │  ingestion, reference, gold, │                         │  scheduler, dag-processor│   │
│  │  quality, metadata, storage  │ ◄── CLI gọi cùng hàm    │  apiserver (UI)          │   │
│  └───────┬──────────────┬───────┘                         └────────────┬─────────────┘   │
│          │ Parquet      │ metadata                                     │                 │
│          ▼              ▼                                              ▼                 │
│  ┌───────────────┐  ┌──────────────────────────── Postgres ───────────────────────────┐  │
│  │/data/alphadata│  │  DB alphadata_meta (runs, raw_files, dq, catalog)   DB airflow   │  │
│  │ raw/ silver/  │  └──────────────────────────────────────────────────────────────────┘  │
│  │ gold/         │                                                                       │
│  └───────▲───────┘                                                                       │
│          │ DuckDB read-only                                                              │
│  ┌───────┴───────┐                                                                       │
│  │ Agent → access│                                                                       │
│  └───────────────┘                                                                       │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

| Thành phần | Vai trò | Chạy code nào |
|---|---|---|
| Airflow scheduler | Kích hoạt job theo lịch, retry, gọi alert | `orchestration` (chỉ thư viện chuẩn + Airflow) |
| virtualenv `alphadata` | Nơi mọi logic dữ liệu thực sự chạy | Mọi module trừ `orchestration` |
| Postgres | DB của Airflow và DB metadata của pipeline | |
| `/data/alphadata` | Lake: raw, silver, gold | |
| CLI | Team chạy tay qua SSH: backfill, debug, query | `cli` → cùng các hàm mà Airflow gọi |
| Agent | Đọc dữ liệu | `access` (read-only) |

Điểm quan trọng: **Airflow và CLI gọi cùng một hàm** (`run_partition`, `run_backfill`, job dựng reference/gold). Airflow chỉ quyết định *khi nào* chạy và *thử lại bao nhiêu lần*; *chạy gì* nằm hoàn toàn trong package.

---

## 2. Ai đọc, ai ghi cái gì

| Module | raw | silver | gold | metadata | Nguồn ngoài |
|---|---|---|---|---|---|
| `ingestion.sources.*` | **ghi** (fetch), đọc (normalize) | | | | **gọi** |
| `ingestion.pipeline` | xoá (raw minute) | **ghi** (merge) | | **ghi** runs, raw_files, dq, catalog | |
| `quality` (trước khi ghi) | | | | **ghi** dq_results | |
| `quality` (sau khi ghi) | | đọc | đọc | **ghi** dq_results, đọc runs | |
| `reference.*` | | đọc | **ghi** bảng reference | ghi catalog | |
| `gold.*` | | đọc | đọc reference, **ghi** bảng gold | ghi catalog | |
| `access` | | đọc (chỉ CLI/debug) | đọc | | |
| `orchestration` | | | | đọc (freshness, alert) | Telegram |
| `cli` | | đọc | đọc | đọc | |

Mỗi bảng chỉ có **một module ghi**. Nhờ vậy khi một số liệu sai, biết ngay phải tìm ở module nào.

---

## 3. Luồng 1: ingest hằng ngày

Ví dụ: DAG `binance_daily` lúc 01:00 UTC ngày 2026-10-12, partition `{symbol: BTCUSDT, date: 2026-10-11}`.

```
orchestration          ingestion.pipeline        sources.binance     storage           quality        metadata
     │                        │                        │                │                 │              │
     │ cron 01:00 UTC         │                        │                │                 │              │
     │ universe.partitions("crypto_pilot", date=hôm qua) → 10 task (một task / cặp)        │              │
     │── run_partition ──────►│                        │                │                 │              │
     │                        │── check_disk ─────────────────────────►│                 │              │
     │                        │── start_run ───────────────────────────────────────────────────────────►│
     │                        │── fetch ──────────────►│                │                 │              │
     │                        │                        │ GET zip + .CHECKSUM (core.http)  │              │
     │                        │                        │── save_raw ───►│ raw/.../2026-10-12/*.zip       │
     │                        │◄── [RawFile] ──────────│                │ + manifest      │              │
     │                        │── record_raw ──────────────────────────────────────────────────────────►│
     │                        │── normalize ──────────►│ đọc raw, parse, ts → UTC         │              │
     │                        │◄── DataFrame ──────────│                │                 │              │
     │                        │ thêm source, ingested_at                │                 │              │
     │                        │── checks ─────────────────────────────────────────────────►│             │
     │                        │◄── [CheckResult] ──────────────────────────────────────────│             │
     │                        │── record_dq ───────────────────────────────────────────────────────────►│
     │                        │   có lỗi error? → DataQualityError, KHÔNG ghi silver       │             │
     │                        │── read file tháng + merge + write_parquet_atomic ──►│ silver/crypto_bars_1m/
     │                        │                        │                │ symbol=BTCUSDT/year=2026/month=10/
     │                        │── upsert_catalog ──────────────────────────────────────────────────────►│
     │                        │── xoá raw zip (keep_raw=False), mark_raw_deleted ──────────────────────►│
     │                        │── finish_run("success") ───────────────────────────────────────────────►│
     │◄── RunResult ──────────│                        │                │                 │              │
```

Trạng thái sau khi chạy:

| Nơi | Có gì |
|---|---|
| raw | Chỉ còn manifest (zip đã xoá vì `keep_raw=False`) |
| silver | File tháng 10 của BTCUSDT có thêm 1440 nến ngày 11 |
| metadata | 1 dòng `ingestion_runs` (success), 2 dòng `raw_files`, ~6 dòng `dq_results`, catalog cập nhật |

Nếu file của Binance chưa được đăng (404): `fetch` raise `NotAvailableYet` → run ghi `skipped_not_available` → Airflow retry sau 30 phút, tối đa 4 lần.

---

## 4. Luồng 2: backfill

Ví dụ: `alphadata backfill alpaca_bars_1m us_etf_core --start 2024-01 --end 2026-09` chạy tay qua SSH.

```
cli ──► ingestion.pipeline.run_backfill
          │
          ├─ universe.load("us_etf_core")                       → 30 ETF
          ├─ dataset.backfill_partitions(member, start, end)    → 30 × 33 tháng = 990 partition
          ├─ metadata: ghi 990 dòng backfill_checkpoints (pending), sinh backfill_id
          │
          └─ lặp từng partition pending:
               ├─ storage.check_disk          hết disk → dừng ngay (không đánh failed)
               ├─ run_partition(..., trigger="backfill")    (giống hệt luồng 1)
               └─ metadata: checkpoint → done | failed (+ lỗi, số lần thử)
```

- Bị kill giữa chừng (mất SSH, server restart): chạy lại với `--resume <backfill_id>`, chỉ làm các partition chưa `done`.
- Rate limit được giữ bởi `core.http` (Alpaca 200 request/phút), nên backfill không cần tự đếm.
- Nguồn có file tháng (Binance) thì partition theo tháng, nhanh hơn nhiều so với theo ngày.

---

## 5. Luồng 3: dựng reference và gold

Reference và gold **không** được dựng trong `run_partition`. Chúng được dựng lại bằng job riêng, sau khi các DAG ingest trong ngày đã xong, theo thứ tự phụ thuộc:

```
05:00 UTC  reference_rebuild
             trading_calendar   (từ thư viện exchange_calendars, không phụ thuộc silver)
                   │
             security_master    ◄── silver: listing_snapshots, index_constituent_*, equity_bars_1d,
                   │                         equity_corporate_actions (symbol_change)
             ┌─────┴──────────────┐
       corporate_actions    index_membership
       ◄── equity_corporate_actions, equity_bars_1d     ◄── index_constituent_*
             └─────┬──────────────┘
05:30 UTC  gold_rebuild
             bars            ◄── equity_bars_1d/1m, crypto_bars_1m + security_master + corporate_actions
               │
             universe_daily  ◄── index_membership + bars
             macro_pit       ◄── macro_observations, factor_returns, fundamentals_facts + security_master
06:00 UTC  dq_post_checks   ◄── đọc silver + gold + metadata
```

- Mỗi bước đọc **toàn bộ đầu vào từ file**, không gọi hàm của bước trước. Nhờ vậy chạy lại riêng một bước được (`alphadata build gold --table bars_1d`).
- Dựng lại là **tăng dần**: chỉ dựng lại phần có dữ liệu mới (mã có bar mới, mã có corporate action mới, series có vintage mới). Một corporate action mới làm `adj_factor` của toàn bộ lịch sử mã đó đổi, nên mã đó được dựng lại cả chuỗi.
- Bước trước lỗi thì bước sau không chạy (Airflow dependency), gold giữ nguyên bản hôm trước. Agent vẫn đọc được, chỉ thiếu dữ liệu mới nhất.

---

## 6. Luồng 4: check sau khi ghi và cảnh báo

```
dq_post_checks (06:00 UTC)
  ├─ freshness                  metadata.last_success(dataset) quá freshness_hours? → warn
  ├─ missing_bars_vs_calendar   silver equity bars vs reference.trading_calendar     → warn + danh sách phút thiếu
  ├─ cross_source_close         equity_bars_1d: Yahoo vs Alpaca lệch > 0.5%          → warn
  └─ adj_vs_source              gold.bars adj vs Yahoo adj_close lệch > 0.5%         → warn
        │
        └─ metadata.record_dq (run_id = null)
              │
orchestration.alerts ─── có warn mới / task fail hẳn / disk > 85% ──► Telegram
```

Team xem chi tiết bằng `alphadata dq report` hoặc `alphadata runs --status failed`.

---

## 7. Luồng 5: agent đọc dữ liệu

Ví dụ: agent đang backtest, đứng ở ngày 2024-06-03, gọi:

```python
get_bars(["AAPL", "NVDA"], "2024-01-01", "2024-06-03", freq="1d", as_of="2024-06-03")
get_macro(["fred:CPIAUCSL"], "2020-01-01", "2024-06-03", as_of="2024-06-03")
```

```
access.get_bars
  ├─ kiểm tra as_of có mặt (không có → lỗi); cắt end về as_of
  ├─ reference.security_master.resolve("AAPL", as_of), resolve("NVDA", as_of)   → security_id
  ├─ DuckDB (read_only) trên gold/bars_1d, lọc security_id + ngày   (bỏ qua file không liên quan nhờ partition)
  ├─ adjusted=True → close × adj_factor
  └─ trả DataFrame
access.get_macro
  └─ DuckDB trên gold/macro_pit:
       WHERE series_key = 'fred:CPIAUCSL'
         AND available_from <= '2024-06-03' AND '2024-06-03' <= available_to
     → CPI các tháng đến tháng 4/2024, mỗi tháng là giá trị đã công bố tính đến 2024-06-03
       (CPI tháng 5/2024 công bố ngày 12/6, nên chưa có)
```

Hai điểm agent cần biết (được ghi trong docstring của `access`):
- Giá điều chỉnh dùng `adj_factor` tính từ cả các sự kiện **sau** `as_of` (ví dụ NVDA split 10:1 ngày 2024-06-10). Nó đúng để tính **lợi nhuận**, nhưng không phải mức giá mà thị trường thấy lúc đó. Lọc theo mức giá thì dùng `adjusted=False`.
- `freq="1h"` trên dữ liệu minute được tính từ nến 1 phút khi query, không tự điền phút trống.

---

## 8. Một ngày vận hành

| Giờ UTC | Giờ VN | Việc |
|---|---|---|
| 01:00 | 08:00 | `binance_daily`: nến 1 phút + futures metrics của ngày UTC hôm trước |
| 02:00 | 09:00 | `listings_daily` (thứ Ba–Bảy), `sp500_weekly` (thứ Hai) |
| 03:00 | 10:00 | `maintenance_cleanup_logs` |
| 05:00 | 12:00 | `reference_rebuild` |
| 05:30 | 12:30 | `gold_rebuild` |
| 06:00 | 13:00 | `dq_post_checks` → alert nếu có |
| 07:00 | 14:00 | `sec_daily` |
| 14:00 | 21:00 | `nyfed_daily`, các nguồn P3 theo lịch công bố |
| 22:00 | 05:00 | `fred_daily` |
| 23:00 | 06:00 | `treasury_daily` |
| 23:30 | 06:30 | `alpaca_daily`, `yahoo_daily` (sau khi phiên after-hours ở Mỹ đóng lúc 20:00 ET) |

Dữ liệu US của ngày giao dịch `T` có trong silver khoảng 06:30 giờ VN ngày `T+1`, và có trong gold sau 12:30 giờ VN cùng ngày. Lịch đầy đủ ở [specs/11-orchestration.md](specs/11-orchestration.md).

---

## 9. Xử lý lỗi

| Tình huống | Nơi phát hiện | Hành vi | Dữ liệu |
|---|---|---|---|
| Nguồn chưa đăng dữ liệu | `sources.*` → `NotAvailableYet` | Retry sau 30 phút, tối đa 4 lần | Silver không đổi |
| Mạng chập chờn, 429, 5xx | `core.http` | Retry trong request (backoff mũ), rồi mới báo lỗi lên | |
| Nguồn đổi format | `sources.*` → `SourceFormatChanged` | Fail ngay, alert | Silver không đổi; raw lỗi vẫn được lưu để phân tích |
| Thiếu API key | `ingestion.pipeline` → `MissingCredential` | Fail ngay, alert | |
| Dữ liệu hỏng (rỗng, trùng khoá, sai đơn vị thời gian) | `quality` mức `error` → `DataQualityError` | Fail ngay, alert | **Silver không đổi**; raw và kết quả check được lưu |
| Dữ liệu đáng ngờ (thiếu nến, OHLC lạ) | `quality` mức `warn` | Vẫn ghi, báo trong DQ report | Silver có dữ liệu, `dq_results` có cờ |
| Disk sắp đầy | `storage.check_disk` → `DiskFullError` | Dừng mọi run mới, alert | Không ghi gì thêm |
| Process chết giữa lúc ghi | | File silver vẫn là bản cũ (ghi atomic); run kẹt ở `running` | Chạy lại partition là đủ (idempotent) |
| Backfill bị ngắt | | Resume theo checkpoint | |
| Reference hoặc gold dựng lỗi | Job rebuild | Bước sau không chạy, alert | Gold giữ bản hôm trước |

---

## 10. Theo dấu một con số

**Câu hỏi:** "CPI tháng 4/2024 mà agent thấy tại ngày 2024-06-03 lấy từ đâu?"

| Bước | Ở đâu | Tra thế nào |
|---|---|---|
| 1. Agent đọc | `gold/macro_pit`, `series_key = fred:CPIAUCSL`, `obs_date = 2024-04-01`, vintage có `available_from <= 2024-06-03` | `alphadata query "SELECT * FROM macro_pit WHERE ..."` |
| 2. Gold dựng từ | `silver/macro_observations/source=fred/series_id=CPIAUCSL/` | Catalog: `alphadata catalog --layer gold` cho biết bảng nguồn |
| 3. Silver ghi bởi | Run của dataset `fred_series`, có `ingested_at` | `alphadata runs --dataset fred_series` → `run_id` |
| 4. Raw | `raw/fred/fred_series/series_id=CPIAUCSL/<fetch_date>/*.json` + manifest | `raw_files` theo `run_id`: đường dẫn, URL (đã che key), SHA-256 |
| 5. Nguồn gốc | `api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&realtime_start=...` | URL trong manifest; đối chiếu trên ALFRED |

Cùng cách đó với một bar giá: `gold.bars_1d` (cột `source` cho biết Alpaca hay Yahoo) → `silver.equity_bars_1d` → run → raw → URL. `dq_flags` trên dòng gold cho biết dòng đó có cảnh báo nào.

---

## 11. Thêm một nguồn mới đi qua những module nào

1. Viết spec nguồn trong `docs/specs/07-ingestion/02-sources/`.
2. `schemas`: thêm bảng silver nếu chưa có bảng phù hợp.
3. `ingestion.sources.<nguồn>`: viết `Dataset` (fetch, normalize, checks), đăng ký vào registry.
4. `universe`: thêm file YAML trong `config/universe/`.
5. `orchestration.schedules`: thêm một dòng `Schedule`. DAG tự sinh.
6. Nếu dữ liệu cần lên gold: thêm vào job tương ứng ở `gold` hoặc `reference`.
7. Cập nhật bảng nguồn ở [02-architecture.md](02-architecture.md) mục 6.

Không cần sửa `pipeline`, `storage`, `metadata`, `quality` (trừ khi cần check mới), `access` hay Airflow.

Chi tiết ở [specs/07-ingestion/01-pipeline.md](specs/07-ingestion/01-pipeline.md).
