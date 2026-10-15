# Kiến trúc tầng data cho Alpha research agent

> Trạng thái: **DRAFT v0.2**, chờ leader duyệt.
> Cập nhật: 2026-10-15.
> Phạm vi: chỉ tầng data (thu thập → làm sạch → bảng chuẩn hoá → cấp cho agent đọc). Không gồm agent research, backtest, execution.
> Tài liệu này mô tả toàn cảnh. Chi tiết từng module nằm trong [specs/](specs/), mỗi module hoặc sub module có một file spec.

---

## 1. Mục tiêu và nguyên tắc

Agent research tìm alpha bằng cách thử rất nhiều ý tưởng rồi backtest trên lịch sử. Data sai thì agent sẽ tìm ra alpha **ảo**. Tầng data phải chặn được 5 lỗi sau:

| Lỗi | Ví dụ | Cách chặn |
|---|---|---|
| **Look-ahead bias** | Dùng GDP quý 3 từ ngày 1/10, trong khi số này cuối tháng 10 mới công bố | Mọi số macro và fundamentals có `available_from`. Agent đọc qua `as_of` bắt buộc |
| **Survivorship bias** | Universe chỉ gồm mã còn sống đến hôm nay | Lưu thành phần index theo ngày, giữ cả mã đã hủy niêm yết trong security master |
| **Corporate action chưa điều chỉnh** | Split 2:1, giá giảm 50%, agent tưởng là crash | Lưu giá gốc, tính giá điều chỉnh từ bảng corporate actions |
| **Lệch timezone, lệch phiên** | Nến 9:30 giờ New York bị ghi thành 9:30 UTC | Mọi timestamp lưu `ts_utc`, phiên tính theo lịch sàn |
| **Số liệu bị revise** | GDP công bố 2.1%, sau sửa còn 1.8% | Lưu theo vintage (ALFRED). Nguồn không có vintage thì ghi nhận theo ngày crawl |

**Nguyên tắc:**
1. **Raw trước, clean sau.** Lưu nguyên bản file tải về trước khi biến đổi. Chạy lại ETL không cần crawl lại.
2. **Mọi dòng đều biết nguồn gốc.** Mỗi dòng có `source`, `ingested_at`. Mỗi bảng có trong catalog.
3. **Point-in-time.** Thứ agent đọc tại thời điểm `t` chỉ được là thứ đã biết tại `t`.
4. **Không sửa tay.** Mọi thay đổi đi qua code và pipeline, có log.
5. **Idempotent.** Chạy lại cùng một partition cho cùng kết quả.
6. **Pipeline không phụ thuộc Airflow.** Mọi việc chạy được bằng CLI. Airflow chỉ lên lịch và retry.

---

## 2. Ràng buộc

| Hạng mục | Giá trị |
|---|---|
| Ngân sách data | **0 USD.** Chỉ dùng nguồn miễn phí. Không xét nguồn trả phí trong giai đoạn này |
| Server bee | Máy ảo KVM, AMD EPYC 4 core, 15 GB RAM (8.5 GB khả dụng), **16 GB disk trống** (đo 2026-10-13) |
| Horizon | Daily đến vài tuần trên universe rộng, cộng intraday (phút → giờ) trên universe pilot |
| Thời gian | ~4 tuần cho prototype |

**Disk là điểm nghẽn.** Pilot dùng ~11 GB trên 16 GB trống (data ~6 GB, Docker image và log ~5 GB). Bản đầy đủ cần ~70–80 GB, phải xin mở rộng disk thêm ~100 GB. Vì vậy mọi lần chạy đều qua **disk guard** (dừng nếu disk trống dưới 3 GB), và raw của minute bị xoá sau khi ghi silver (vẫn giữ manifest, vì tải lại được).

**Phân bổ RAM** trong 8.5 GB khả dụng: Airflow ~2.5 GB (LocalExecutor, parallelism 2), Postgres ~1 GB, task ETL ~2.5 GB (DuckDB `memory_limit='2GB'`, xử lý từng mã × từng tháng), dự phòng ~2.5 GB.

**Ước lượng dung lượng (Parquet zstd):**

| Dữ liệu | Pilot | Bản đầy đủ (sau khi có thêm disk) |
|---|---|---|
| Crypto minute | Top 10 cặp từ 2024: ~0.8 GB (đo: ~28 MB / cặp / năm) | Top 50 cặp từ 2018: ~12 GB |
| US minute | 20 cổ phiếu + 30 ETF từ 2024: ~1.1 GB (ước ~8 MB / mã / năm) | S&P 500 + 30 ETF từ 2016: ~40 GB |
| Daily, macro, factor | ~2.5 GB | ~4 GB |
| Fundamentals (SEC) | Chưa làm | 2–5 GB |
| Raw daily | ~1 GB | ~10 GB (kể cả bản tạm) |

Nến 5 phút, 15 phút, 1 giờ **không lưu riêng**. Chúng được tính từ nến 1 phút khi query.

---

## 3. Kiến trúc tầng

```
                 Nguồn miễn phí (API / file zip / CSV)
                                │
 ┌──────────────────────────────▼──────────────────────────────┐
 │ INGESTION   ingestion/sources/<nguồn>: fetch → normalize      │
 │             ingestion/pipeline: run_partition, backfill       │
 └──────────────────────────────┬──────────────────────────────┘
                                ▼
   [BRONZE] raw/<source>/<dataset>/<partition>/<fetch_date>/  + manifest (URL, SHA-256)
                                │  quality: check trước khi ghi
                                ▼
   [SILVER] silver/<table>/...  Parquet, đúng schema (schemas/), UTC, dedupe
                                │
            ┌───────────────────┴───────────────────┐
            ▼                                       ▼
   reference/: security master, lịch sàn,   gold/: bars điều chỉnh, universe
   corporate actions, thành phần index      theo ngày, macro PIT
            └───────────────────┬───────────────────┘
                                ▼
   [GOLD] gold/<table>/...  Parquet, khoá security_id, sẵn cho research
                                │
                                ▼
   access/: get_bars(..., as_of), get_macro(..., as_of)  (read-only)  →  Agent

 Xuyên suốt:  core (config, http, lỗi) · storage (lake, disk guard) · metadata (runs, catalog)
              quality · schemas · universe · orchestration (Airflow) · cli · deploy
```

**Vì sao Parquet + DuckDB mà không dồn vào Postgres:** minute data lớn (US minute đầy đủ hàng tỷ dòng), Parquet nén tốt hơn nhiều so với bảng Postgres, và DuckDB đọc thẳng Parquet rất nhanh cho kiểu truy vấn "lấy 2 năm minute của 500 mã". Postgres chỉ giữ metadata vận hành (runs, catalog, DQ) và DB của Airflow.

---

## 4. Module

Mỗi dòng là một module trong `src/alphadata/`, có một spec riêng.

| Module | Tầng | Trách nhiệm | Spec |
|---|---|---|---|
| `core` | Xuyên suốt | Settings, HTTP session có retry, lỗi chung, tiện ích thời gian | [core.md](specs/core.md) |
| `storage` | Xuyên suốt | Bố cục lake, ghi raw + manifest, ghi Parquet atomic, upsert, disk guard | [storage.md](specs/storage.md) |
| `metadata` | Xuyên suốt | `ingestion_runs`, `raw_files`, `dq_results`, `table_catalog`, checkpoint backfill | [metadata.md](specs/metadata.md) |
| `quality` | Xuyên suốt | Thư viện check (error chặn ghi, warn chỉ ghi nhận), check sau khi ghi (freshness, đối chiếu nguồn) | [quality.md](specs/quality.md) |
| `schemas` | Xuyên suốt | Hợp đồng cột và kiểu của mọi bảng silver và gold | [schemas.md](specs/schemas.md) |
| `universe` | Xuyên suốt | Danh sách mã/series cần crawl, đọc từ file YAML | [universe.md](specs/universe.md) |
| `ingestion.pipeline` | Ingestion | Hợp đồng `Dataset`, `run_partition`, backfill có checkpoint, registry | [ingestion/pipeline.md](specs/ingestion/pipeline.md) |
| `ingestion.sources.*` | Ingestion | Mỗi nguồn một sub module: fetch, normalize, check riêng | [ingestion/sources/](specs/ingestion/sources/) |
| `reference.security_master` | Reference | `security_id` và lịch sử ticker, sàn, niêm yết | [reference/security_master.md](specs/reference/security_master.md) |
| `reference.trading_calendar` | Reference | Ngày và giờ mở cửa của từng sàn | [reference/trading_calendar.md](specs/reference/trading_calendar.md) |
| `reference.corporate_actions` | Reference | Split, cổ tức, đổi ticker, đã đối chiếu giữa các nguồn | [reference/corporate_actions.md](specs/reference/corporate_actions.md) |
| `reference.index_membership` | Reference | Mã nào thuộc S&P 500 tại từng ngày | [reference/index_membership.md](specs/reference/index_membership.md) |
| `gold.bars` | Gold | Bars daily và minute theo `security_id`, có giá điều chỉnh | [gold/bars.md](specs/gold/bars.md) |
| `gold.universe_daily` | Gold | Mã nào hợp lệ để research tại ngày `t` | [gold/universe_daily.md](specs/gold/universe_daily.md) |
| `gold.macro_pit` | Gold | Macro và fundamentals point-in-time | [gold/macro_pit.md](specs/gold/macro_pit.md) |
| `access` | Access | API read-only cho agent, view DuckDB, `as_of` bắt buộc | [access.md](specs/access.md) |
| `orchestration` | Vận hành | Airflow DAG (sinh từ khai báo lịch), alert, bảo trì | [orchestration.md](specs/orchestration.md) |
| `cli` | Vận hành | Lệnh `alphadata run / backfill / catalog / runs / query / dq` | [cli.md](specs/cli.md) |
| `deploy/` (ngoài package) | Vận hành | Docker image, docker-compose Airflow 3 + Postgres, init DB | [deploy.md](specs/deploy.md) |

**Hướng phụ thuộc** (chỉ được import theo chiều mũi tên, không import ngược):

```
core ← storage ← metadata ← quality ← ingestion ← reference ← gold ← access
  ↑        ↑         ↑                    ↑           ↑          ↑       ↑
schemas, universe (chỉ phụ thuộc core)    └───────────┴──────────┴── orchestration, cli
```

`orchestration` và `cli` là lớp ngoài cùng: được import mọi module, nhưng không module nào import ngược lại chúng.

---

## 5. Cấu trúc thư mục

Code cũ để `dags/` ngoài package, ngang hàng với `src/`. Như vậy Airflow DAG nằm lệch khỏi kiến trúc, và DAG phải tự biết danh sách universe, lịch. Cấu trúc mới đặt **orchestration là một module trong package**, giống các tầng khác. Thư mục `dags/` của module này được mount vào Airflow.

```
alphadata/                              (repo root)
├── README.md
├── pyproject.toml
├── .env.example
├── config/
│   └── universe/                       YAML: danh sách mã/series theo nhóm (xem specs/universe.md)
├── src/alphadata/
│   ├── core/                           settings.py, http.py, errors.py, timeutil.py
│   ├── storage/                        layout.py, raw.py, parquet.py, disk.py
│   ├── metadata/                       store.py, tables.py, checkpoint.py
│   ├── quality/                        checks.py, runner.py, post_checks.py
│   ├── schemas/                        silver.py, gold.py (hợp đồng bảng)
│   ├── universe/                       loader.py
│   ├── ingestion/
│   │   ├── pipeline/                   dataset.py (hợp đồng), run.py, backfill.py, registry.py
│   │   └── sources/
│   │       ├── binance/                mỗi nguồn một sub package
│   │       ├── alpaca/
│   │       ├── yahoo/
│   │       ├── fred/
│   │       └── ...                     (danh sách đầy đủ ở mục 7)
│   ├── reference/
│   │   ├── security_master/
│   │   ├── trading_calendar/
│   │   ├── corporate_actions/
│   │   └── index_membership/
│   ├── gold/
│   │   ├── bars/
│   │   ├── universe_daily/
│   │   └── macro_pit/
│   ├── access/                         api.py (get_bars, get_macro), views.py
│   ├── orchestration/
│   │   ├── schedules.py                khai báo lịch: dataset nào, universe nào, cron nào
│   │   ├── factory.py                  sinh DAG từ schedules
│   │   └── dags/                       file entrypoint cho Airflow (mount vào /opt/airflow/dags)
│   └── cli/
├── deploy/                             Dockerfile, docker-compose.yml, init-db.sql
├── tests/                              cấu trúc giống src/alphadata/
└── docs/
    ├── architecture.md                 (file này)
    └── specs/                          cấu trúc giống src/alphadata/
        ├── core.md, storage.md, metadata.md, quality.md, schemas.md, universe.md
        ├── access.md, orchestration.md, cli.md, deploy.md
        ├── ingestion/
        │   ├── pipeline.md
        │   └── sources/<nguồn>.md
        ├── reference/<sub module>.md
        └── gold/<sub module>.md
```

**Quy ước:**
- Một sub package trong code tương ứng một file spec cùng tên. Thêm module mới thì thêm spec trước.
- `tests/` và `docs/specs/` có cùng cấu trúc với `src/alphadata/`, để tìm test và spec của một module là thấy ngay.
- `deploy/` ở ngoài package vì không phải code Python. Nó thay cho thư mục `infra/` cũ.
- Universe là dữ liệu cấu hình, không phải code, nên nằm trong `config/universe/` dạng YAML.

---

## 6. Luồng chạy một partition

Một **partition** là đơn vị nhỏ nhất của một lần chạy, ví dụ BTCUSDT ngày 2026-10-11, hoặc series DGS10. Chi tiết ở [ingestion/pipeline.md](specs/ingestion/pipeline.md).

1. **Disk guard**: dừng nếu disk trống dưới ngưỡng.
2. **fetch**: tải về, lưu nguyên bản vào bronze, kèm manifest (URL, SHA-256, thời điểm tải).
3. **normalize**: đưa về schema silver, thời gian UTC, thêm `source`, `ingested_at`.
4. **checks**: lỗi mức `error` (rỗng, trùng khoá, timestamp ngoài khoảng) thì **không ghi**. Mức `warn` (thiếu nến, OHLC bất thường) vẫn ghi nhưng được lưu lại.
5. **merge + ghi silver**: gộp với dữ liệu cũ, ghi Parquet atomic.
6. **metadata**: ghi `ingestion_runs`, `raw_files`, `dq_results`, `table_catalog`.

Gold được dựng lại từ silver và reference bằng job riêng, không nằm trong `run_partition`.

**Lưu trữ trên server:**

```
/data/alphadata/
├── raw/<source>/<dataset>/<k=v>/.../<fetch_date>/<file> + <file>.manifest.json
├── silver/<table>/<k=v>/.../part.parquet
├── gold/<table>/...
└── checkpoints/                        trạng thái backfill (cũng ghi vào metadata)
Postgres: DB `airflow` (của Airflow) và DB `alphadata_meta` (metadata của pipeline)
```

---

## 7. Nguồn dữ liệu đã chốt

**Cách chốt:** chỉ nhận nguồn miễn phí, gọi được bằng chương trình (không cần trình duyệt, không bị chặn bot), và điều khoản không cấm dùng nội bộ. Ngày 2026-10-15 đã gọi thử thật từng endpoint từ máy dev. Nguồn cần key mà chưa có key thì kiểm tra rằng endpoint phản hồi đúng (báo thiếu key) và đối chiếu tài liệu chính thức.

### 7.1 Dùng

| Ưu tiên | Nguồn | Dữ liệu | Key | Kết quả kiểm tra 2026-10-15 | Spec |
|---|---|---|---|---|---|
| P1 | **Binance public data** | Crypto spot 1 phút, funding rate, futures metrics | Không | ✅ Tải được file zip 1m, fundingRate, metrics | [binance.md](specs/ingestion/sources/binance.md) |
| P1 | **Alpaca Market Data** | US equity + ETF 1 phút và daily (SIP, từ 2016) | Miễn phí | ⚠️ Endpoint trả 401 khi thiếu key (đúng). Tài liệu chính thức: gói Basic miễn phí có lịch sử từ 2016, 200 request/phút. **Cần tạo key để tải thử** | [alpaca.md](specs/ingestion/sources/alpaca.md) |
| P1 | **Yahoo Finance** (chart API) | Daily US equity, ETF, index từ trước 2005; split, cổ tức | Không | ✅ Daily và 1m đều trả JSON. Không chính thức, có thể hỏng bất cứ lúc nào | [yahoo.md](specs/ingestion/sources/yahoo.md) |
| P1 | **FRED / ALFRED** | Macro Mỹ, lãi suất, FX, commodity, có vintage; lịch công bố | Miễn phí | ✅ API phản hồi, báo thiếu key (đúng). Endpoint CSV `fredgraph.csv` bị timeout, không dùng | [fred.md](specs/ingestion/sources/fred.md) |
| P2 | **SEC EDGAR** | Fundamentals (XBRL companyfacts), CIK ↔ ticker | Không (cần User-Agent có email) | ✅ companyfacts AAPL 3.8 MB, company_tickers.json | [sec_edgar.md](specs/ingestion/sources/sec_edgar.md) |
| P2 | **S&P 500 constituents** (Wikipedia + GitHub fja05680/sp500) | Thành phần hiện tại và lịch sử thay đổi | Không | ✅ Cả hai tải được | [sp500_constituents.md](specs/ingestion/sources/sp500_constituents.md) |
| P2 | **Nasdaq Trader symbol directory** | Danh sách mã đang niêm yết (Nasdaq + NYSE + khác) | Không | ✅ `nasdaqlisted.txt` tải được | [nasdaq_trader.md](specs/ingestion/sources/nasdaq_trader.md) |
| P2 | **Kenneth French Data Library** | Factor Fama-French, momentum | Không | ✅ Tải được zip FF3 daily | [french.md](specs/ingestion/sources/french.md) |
| P2 | **US Treasury** | Yield curve chính thức: par yield và real yield | Không | ✅ CSV par yield và real yield năm 2026 | [treasury.md](specs/ingestion/sources/treasury.md) |
| P2 | **NY Fed Markets API** | SOFR, EFFR, OBFR | Không | ✅ JSON SOFR | [nyfed.md](specs/ingestion/sources/nyfed.md) |
| P3 | **EIA** | Giá và tồn kho năng lượng | Miễn phí | ✅ API phản hồi, báo thiếu key (đúng) | [eia.md](specs/ingestion/sources/eia.md) |
| P3 | **BLS** | CPI chi tiết, việc làm | v1 không cần, v2 miễn phí | ✅ v1 trả dữ liệu CPI không cần key | [bls.md](specs/ingestion/sources/bls.md) |
| P3 | **BEA** | GDP, thu nhập (NIPA) | Miễn phí | ✅ API phản hồi, báo key sai (đúng) | [bea.md](specs/ingestion/sources/bea.md) |
| P3 | **ECB Data Portal** | Tỷ giá tham chiếu EUR, lãi suất euro | Không | ✅ CSV tỷ giá USD/EUR | [ecb.md](specs/ingestion/sources/ecb.md) |
| P4 | **World Bank** | Chỉ số phát triển, nhiều nước | Không | ✅ JSON GDP Mỹ | [worldbank.md](specs/ingestion/sources/worldbank.md) |
| P4 | **IMF** | WEO, IFS | Không | ✅ `api.imf.org` (SDMX) và DataMapper. Endpoint cũ `dataservices.imf.org` không còn phản hồi | [imf.md](specs/ingestion/sources/imf.md) |
| P4 | **OECD** | Macro OECD, leading indicators | Không | ✅ SDMX `sdmx.oecd.org` | [oecd.md](specs/ingestion/sources/oecd.md) |

Thư viện offline (không crawl): `exchange_calendars` cho lịch sàn, xem [reference/trading_calendar.md](specs/reference/trading_calendar.md).

**Hệ quả:** không có nguồn minute miễn phí cho FX, vàng, dầu và index (Dukascopy bị loại). Các nhóm này chỉ có daily (FRED, ECB, EIA, Yahoo).

### 7.2 Loại

| Nguồn | Lý do loại |
|---|---|
| Stooq | Đã chặn bot: mọi request CSV trả về trang JavaScript challenge, không tải được bằng chương trình |
| Dukascopy | Không kết nối được từ mạng dev (timeout cả datafeed lẫn trang chủ), và chưa tìm được điều khoản cho phép tải tự động. Có thể thử lại từ server bee sau prototype |
| CME daily settlements | Trả 403, chặn IP vì "suspected web scraping". Điều khoản cấm scraping |
| Nasdaq Data Link (dataset free) | Trả 403 (Incapsula). Các dataset miễn phí cũ như WIKI đã ngừng cập nhật |
| Cboe (VIX history CSV) | Lỗi TLS từ mạng dev. Không cần: VIX đã có từ FRED (`VIXCLS`) và Yahoo (`^VIX`) |
| CoinGecko | Bản miễn phí chỉ cho lịch sử 365 ngày (thử `days=max` bị 401). Binance đã đủ cho crypto |
| Polygon.io / Massive (gói free) | 5 request/phút, chỉ end-of-day, lịch sử ~2 năm. Không hơn gì Alpaca |
| Tiingo (gói free) | Giới hạn 50 request/giờ, và theo các nguồn bên thứ ba thì gói free chỉ cho dùng cá nhân. Dùng nội bộ công ty phải trả phí |
| Mọi nguồn trả phí | Norgate, Sharadar, CRSP, Databento, FirstRate, Kibot, Kaiko, Bloomberg, LSEG, FiinTrade, Fireant: ngoài ngân sách 0 USD, bỏ khỏi tài liệu |

### 7.3 Hạn chế của bộ nguồn miễn phí

| Hạn chế | Hậu quả | Cách giảm nhẹ |
|---|---|---|
| Giá của mã đã hủy niêm yết khó có (Yahoo và Alpaca thường không trả) | Survivorship bias trong backtest US equity | Ghi lại danh sách mã từng thuộc S&P 500 nhưng không tải được giá. Kết quả backtest phải kèm cảnh báo này |
| Yahoo không chính thức | Có thể hỏng bất cứ lúc nào | Đối chiếu với Alpaca daily (từ 2016). Connector Yahoo phải báo lỗi rõ khi format đổi |
| Minute pilot chỉ từ 2024 | Ít chế độ thị trường để kiểm chứng | Mở rộng về 2016 (Alpaca) và 2018 (Binance) khi có thêm disk |
| Không có minute cho FX, commodity, index | Không làm được alpha intraday cho các nhóm này | Chấp nhận ở prototype |
| Rate limit của API miễn phí | Backfill chậm | Backfill một lần, chạy qua đêm, có checkpoint |

---

## 8. Lộ trình

| Phase | Nội dung | Kết quả cần có |
|---|---|---|
| 0. Nền | `core`, `storage`, `metadata`, `quality`, `schemas`, `universe`, `ingestion.pipeline`, `cli`, `deploy` | `alphadata run` chạy được với một dataset giả; Docker stack lên được trên server bee |
| 1. Giá + macro chính | Nguồn P1: binance, alpaca, yahoo, fred. `orchestration` cho cả 4 | DAG hằng ngày chạy ổn định 1 tuần; backfill pilot xong |
| 2. Reference | Nguồn P2 và `reference.*` | Có `security_id`, lịch sàn, thành phần S&P 500 theo ngày, corporate actions đã đối chiếu |
| 3. Gold + access | `gold.*`, `access` | Agent gọi được `get_bars`, `get_macro` với `as_of`; có test PIT |
| 4. Mở rộng macro | Nguồn P3, P4 | Theo nhu cầu của agent |

Code cũ (commit `68636b0`) đã chạy được Binance, Yahoo, FRED nhưng theo cấu trúc cũ. Phần nào dùng lại được (disk guard, ghi Parquet atomic, MetaStore, parse timestamp Binance) sẽ chuyển vào module mới tương ứng.

---

## 9. Quyết định thiết kế

| Quyết định | Lý do |
|---|---|
| Parquet + DuckDB cho mọi dữ liệu giá, kể cả daily | Disk hạn chế, Parquet nén tốt (zstd mức 9 + byte_stream_split giảm ~20% so với mặc định, đo trên nến 1 phút), DuckDB đọc trực tiếp |
| Postgres chỉ giữ metadata | Ít dữ liệu, cần ghi đồng thời từ nhiều task |
| Airflow gọi pipeline qua virtualenv riêng (`@task.external_python`) | Dependency của pipeline không xung đột với Airflow |
| DAG sinh từ `orchestration/schedules.py` | Thêm dataset hoặc đổi lịch chỉ sửa một chỗ, không viết DAG tay |
| Raw minute xoá sau khi ghi silver (giữ manifest) | Tải lại được, và chiếm nhiều chỗ nhất. Raw daily, macro được giữ |
| Silver giữ khoá theo nguồn (`symbol`, `series_id`); `security_id` chỉ có từ gold | Silver phản ánh đúng nguồn. Ánh xạ sang `security_id` cần security master, làm một lần ở gold |
| Một bảng silver có thể có nhiều nguồn (cột `source` nằm trong khoá) | Ví dụ `equity_bars_1d` có cả Yahoo và Alpaca để đối chiếu |
| Giá lưu bản gốc (unadjusted) | Giá điều chỉnh tính ở gold từ corporate actions, kiểm tra chéo với giá điều chỉnh của nguồn |

---

## 10. Ngoài phạm vi prototype

- Tick data, order book
- FX, commodity, index minute (không có nguồn miễn phí dùng được)
- Futures minute thật
- Tin tức, mạng xã hội, sentiment
- Options
- Real-time streaming (chỉ batch theo lịch)
- Nguồn trả phí
- Thị trường Việt Nam (vnstock, TCTK, NHNN): làm sau prototype nếu leader muốn
- Backtest engine, execution

---


