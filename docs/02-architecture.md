# 02. Kiến trúc

> Đọc sau [01-problem.md](01-problem.md). Tài liệu này mô tả **tĩnh**: hệ thống gồm những tầng và module nào, mỗi module chịu trách nhiệm gì, đặt ở đâu, dùng nguồn dữ liệu nào.
> Hệ thống **chạy** thế nào (luồng dữ liệu, thứ tự gọi giữa các module, xử lý lỗi) ở [03-how-it-works.md](03-how-it-works.md). Thiết kế từng bảng ở [04-data-model.md](04-data-model.md). Chi tiết từng module ở [specs/](specs/).

---

## 1. Nguyên tắc thiết kế

Mỗi nguyên tắc trả lời một yêu cầu trong [01-problem.md](01-problem.md):

| Nguyên tắc | Giải quyết | Thể hiện ở |
|---|---|---|
| **Raw trước, clean sau.** Lưu nguyên bản file tải về trước khi biến đổi | Tái lập được; nguồn miễn phí có thể biến mất | `storage` lưu raw + manifest; `normalize` chỉ đọc raw, không gọi mạng |
| **Mọi dòng biết nguồn gốc** | Truy được nguồn gốc | Cột `source`, `ingested_at`; `metadata.table_catalog`; manifest có URL và SHA-256 |
| **Point-in-time từ silver đến access** | Look-ahead, revision | Macro lưu `available_from` / `available_to`; `access` bắt buộc `as_of` |
| **Lưu giá gốc, điều chỉnh khi dựng gold** | Corporate actions | Silver giữ giá gốc; `gold.bars` tính `adj_factor` từ `reference.corporate_actions` |
| **Định danh ổn định** | Survivorship, đổi mã | `reference.security_master` cấp `security_id`; `reference.index_membership` theo ngày |
| **Mọi timestamp là UTC, phiên tính theo lịch sàn** | Lệch timezone | `ts_utc` ở mọi bảng; `reference.trading_calendar` |
| **Chặn dữ liệu hỏng trước khi ghi** | Chất lượng | `quality`: lỗi `error` không ghi silver, `warn` ghi và lưu lại |
| **Idempotent** | Tái lập, retry an toàn | Merge theo khoá, ghi Parquet atomic |
| **Pipeline không phụ thuộc Airflow** | Dễ debug, test | Mọi việc chạy được bằng `cli`; `orchestration` chỉ lên lịch và retry |
| **Không bao giờ làm hỏng server** | Server dùng chung | Disk guard trước mỗi lần chạy, giới hạn RAM cho từng thành phần |

---

## 2. Kiến trúc tầng

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
   [SILVER] silver/<table>/...  Parquet, đúng schema (schemas/), UTC, dedupe, khoá theo nguồn
                                │
            ┌───────────────────┴───────────────────┐
            ▼                                       ▼
   reference/: lịch sàn, security master,   gold/: bars điều chỉnh, universe
   corporate actions, thành phần index      theo ngày, macro PIT
            └───────────────────┬───────────────────┘
                                ▼
   [GOLD] gold/<table>/...  Parquet, khoá security_id, sẵn cho research
                                │
                                ▼
   access/: get_bars(..., as_of), get_macro(..., as_of)  (read-only)  →  Agent

 Xuyên suốt:  core · storage · schemas · metadata · quality · universe
 Vận hành:    orchestration (Airflow) · cli · deploy
```

| Tầng | Chứa gì | Ai ghi | Ai đọc |
|---|---|---|---|
| Bronze (raw) | File nguyên bản từ nguồn + manifest | `ingestion` | `ingestion` (normalize, chạy lại ETL) |
| Silver | Bảng đã chuẩn hoá theo từng nguồn, khoá theo định danh của nguồn (`symbol`, `series_id`) | `ingestion` | `reference`, `gold`, `quality`, CLI |
| Gold | Bảng reference và bảng research, khoá `security_id`, có PIT | `reference`, `gold` | `access`, `quality` |
| Metadata | Lịch sử chạy, file raw, kết quả DQ, catalog, checkpoint | `metadata` (được gọi từ mọi tầng) | `cli`, `orchestration`, `quality` |

**Vì sao Parquet + DuckDB mà không dồn vào Postgres:** minute data lớn (US minute đầy đủ hàng tỷ dòng), Parquet nén tốt hơn nhiều so với bảng Postgres, và DuckDB đọc thẳng Parquet rất nhanh cho kiểu truy vấn "lấy 2 năm minute của 500 mã". Postgres chỉ giữ metadata vận hành và DB của Airflow.

---

## 3. Module

Mỗi dòng là một module trong `src/alphadata/`, có một spec riêng. Thứ tự trong bảng là thứ tự nên đọc spec.

| # | Module | Tầng | Trách nhiệm | Spec |
|---|---|---|---|---|
| 1 | `core` | Xuyên suốt | Settings, HTTP session có retry và rate limit, lỗi chung, tiện ích thời gian | [specs/01-core.md](specs/01-core.md) |
| 2 | `storage` | Xuyên suốt | Bố cục lake, ghi raw + manifest, ghi Parquet atomic, upsert, disk guard | [specs/02-storage.md](specs/02-storage.md) |
| 3 | `schemas` | Xuyên suốt | Đưa thiết kế bảng ở [04-data-model.md](04-data-model.md) vào code; kiểm tra DataFrame trước khi ghi | [specs/03-schemas.md](specs/03-schemas.md) |
| 4 | `metadata` | Xuyên suốt | `ingestion_runs`, `raw_files`, `dq_results`, `table_catalog`, checkpoint backfill | [specs/04-metadata.md](specs/04-metadata.md) |
| 5 | `quality` | Xuyên suốt | Check trước khi ghi (error chặn, warn ghi nhận), check sau khi ghi (freshness, thiếu nến, đối chiếu nguồn) | [specs/05-quality.md](specs/05-quality.md) |
| 6 | `universe` | Xuyên suốt | Danh sách mã/series cần crawl, đọc từ YAML | [specs/06-universe.md](specs/06-universe.md) |
| 7 | `ingestion.pipeline` | Ingestion | Hợp đồng `Dataset`, `run_partition`, backfill có checkpoint, registry | [specs/07-ingestion/01-pipeline.md](specs/07-ingestion/01-pipeline.md) |
| 7 | `ingestion.sources.*` | Ingestion | Mỗi nguồn một sub module: fetch, normalize, check riêng | [specs/07-ingestion/02-sources/](specs/07-ingestion/02-sources/) |
| 8 | `reference.trading_calendar` | Reference | Ngày và giờ mở cửa của từng sàn | [specs/08-reference/01-trading_calendar.md](specs/08-reference/01-trading_calendar.md) |
| 8 | `reference.security_master` | Reference | `security_id` và lịch sử ticker, sàn, niêm yết | [specs/08-reference/02-security_master.md](specs/08-reference/02-security_master.md) |
| 8 | `reference.corporate_actions` | Reference | Split, cổ tức, đổi mã, đã đối chiếu giữa các nguồn | [specs/08-reference/03-corporate_actions.md](specs/08-reference/03-corporate_actions.md) |
| 8 | `reference.index_membership` | Reference | Mã nào thuộc S&P 500 tại từng ngày | [specs/08-reference/04-index_membership.md](specs/08-reference/04-index_membership.md) |
| 9 | `gold.bars` | Gold | Bars daily và minute theo `security_id`, có hệ số điều chỉnh | [specs/09-gold/01-bars.md](specs/09-gold/01-bars.md) |
| 9 | `gold.universe_daily` | Gold | Mã nào hợp lệ để research tại ngày `t` | [specs/09-gold/02-universe_daily.md](specs/09-gold/02-universe_daily.md) |
| 9 | `gold.macro_pit` | Gold | Macro, factor, fundamentals point-in-time | [specs/09-gold/03-macro_pit.md](specs/09-gold/03-macro_pit.md) |
| 10 | `access` | Access | API read-only cho agent, view DuckDB, `as_of` bắt buộc | [specs/10-access.md](specs/10-access.md) |
| 11 | `orchestration` | Vận hành | Airflow DAG sinh từ khai báo lịch, alert, bảo trì | [specs/11-orchestration.md](specs/11-orchestration.md) |
| 12 | `cli` | Vận hành | Lệnh `alphadata run / backfill / build / catalog / runs / dq / query` | [specs/12-cli.md](specs/12-cli.md) |
| 13 | `deploy/` (ngoài package) | Vận hành | Docker image, docker-compose Airflow 3 + Postgres, init DB, backup | [specs/13-deploy.md](specs/13-deploy.md) |

**Hướng phụ thuộc.** Một module chỉ được import module đứng trước nó trong chuỗi, không import ngược:

```
core ◄─ storage ◄─ schemas ◄─ metadata ◄─ quality ◄─ universe ◄─ ingestion ◄─ reference ◄─ gold ◄─ access
                                                                                                    ▲
                                                           orchestration, cli (import mọi module) ──┘
```

`orchestration` và `cli` là lớp ngoài cùng: được import mọi module, nhưng không module nào import ngược lại chúng. Nhờ vậy toàn bộ pipeline chạy được mà không cần Airflow.

Cách các module gọi nhau khi chạy được mô tả theo từng luồng ở [03-how-it-works.md](03-how-it-works.md).

---

## 4. Cấu trúc thư mục

Cây thư mục bám theo các tầng ở mục 2. **Orchestration là một module trong package** như các tầng khác, không để `dags/` ngoài package: DAG chỉ đọc lịch từ `orchestration/schedules.py` và gọi vào module khác. Thư mục `dags/` của module này được mount vào Airflow.

```
alphadata/                              (repo root)
├── README.md
├── pyproject.toml
├── .env.example
├── config/
│   ├── universe/                       YAML: danh sách mã/series theo nhóm
│   └── reference/                      security_overrides.yaml (ngoại lệ sửa tay có review)
├── src/alphadata/
│   ├── core/                           settings.py, http.py, errors.py, timeutil.py
│   ├── storage/                        layout.py, raw.py, parquet.py, disk.py
│   ├── schemas/                        silver.py, gold.py
│   ├── metadata/                       tables.py, store.py, checkpoint.py
│   ├── quality/                        checks.py, runner.py, post_checks.py
│   ├── universe/                       loader.py
│   ├── ingestion/
│   │   ├── pipeline/                   dataset.py, run.py, backfill.py, registry.py
│   │   └── sources/
│   │       ├── binance/                mỗi nguồn một sub package
│   │       ├── alpaca/
│   │       └── ...                     (17 nguồn, danh sách ở mục 6)
│   ├── reference/
│   │   ├── trading_calendar/
│   │   ├── security_master/
│   │   ├── corporate_actions/
│   │   └── index_membership/
│   ├── gold/
│   │   ├── bars/
│   │   ├── universe_daily/
│   │   └── macro_pit/
│   ├── access/                         api.py, views.py
│   ├── orchestration/
│   │   ├── schedules.py                khai báo lịch
│   │   ├── factory.py                  sinh DAG từ schedules
│   │   ├── alerts.py
│   │   └── dags/                       entrypoint cho Airflow (mount vào /opt/airflow/dags)
│   └── cli/
├── deploy/                             Dockerfile, docker-compose.yml, init-db.sql, migrations/, backup.sh
├── tests/                              cấu trúc giống src/alphadata/
└── docs/
    ├── 01-problem.md
    ├── 02-architecture.md              (file này)
    ├── 03-how-it-works.md
    ├── 04-data-model.md
    └── specs/                          cấu trúc giống src/alphadata/, đánh số theo thứ tự đọc
        ├── 01-core.md … 06-universe.md
        ├── 07-ingestion/
        │   ├── 01-pipeline.md
        │   └── 02-sources/01-binance.md … 17-oecd.md
        ├── 08-reference/01-trading_calendar.md … 04-index_membership.md
        ├── 09-gold/01-bars.md … 03-macro_pit.md
        └── 10-access.md … 13-deploy.md
```

**Quy ước:**
- Một sub package trong code tương ứng một file spec cùng tên, có tiền tố số thứ tự đọc (`NN-<tên module>.md`). Thêm module mới thì thêm spec trước.
- Thứ tự đọc theo hướng phụ thuộc: module được dùng bởi module khác đứng trước.
- `tests/` và `docs/specs/` có cùng cấu trúc với `src/alphadata/`.
- `deploy/` ở ngoài package vì không phải code Python.
- Universe và override là dữ liệu cấu hình, nằm trong `config/` dạng YAML.

---

## 5. Lưu trữ và tài nguyên

### 5.1 Bố cục lưu trữ trên server

```
/data/alphadata/
├── raw/<source>/<dataset>/<k=v>/.../<fetch_date>/<file> + <file>.manifest.json
├── silver/<table>/<k=v>/.../part.parquet
├── gold/<table>/<k=v>/.../part.parquet
└── checkpoints/<dataset>/<backfill_id>.json
Postgres: DB `airflow` (của Airflow) và DB `alphadata_meta` (metadata của pipeline)
```

### 5.2 Dung lượng (Parquet zstd)

| Dữ liệu | Pilot | Bản đầy đủ (cần thêm ~100 GB disk) |
|---|---|---|
| Crypto minute | Top 10 cặp từ 2024: ~0.8 GB (~28 MB / cặp / năm) | Top 50 cặp từ 2018: ~12 GB |
| US minute | 20 cổ phiếu + 30 ETF từ 2024: ~1.1 GB (ước ~8 MB / mã / năm) | S&P 500 + 30 ETF từ 2016: ~40 GB |
| Daily, macro, factor, reference | ~2.5 GB | ~4 GB |
| Fundamentals (SEC) | Chưa làm | 2–5 GB |
| Raw daily | ~1 GB | ~10 GB (kể cả bản tạm) |
| Docker image + log Airflow (giữ 7 ngày) | ~5 GB | ~5 GB |
| **Tổng** | **~11 GB / 16 GB trống** | **~75–80 GB** |

Nến 5 phút, 15 phút, 1 giờ **không lưu riêng**, mà tính từ nến 1 phút khi query. Raw minute xoá sau khi ghi silver (giữ manifest) vì tải lại được.

### 5.3 RAM (8.5 GB khả dụng)

| Thành phần | Giới hạn |
|---|---|
| Airflow (LocalExecutor, parallelism 2) | ~2.5 GB |
| Postgres | ~1 GB |
| Task ETL (DuckDB `memory_limit='2GB'`, xử lý từng mã × từng tháng) | ~2.5 GB |
| Dự phòng cho service khác | ~2.5 GB |

---

## 6. Nguồn dữ liệu đã chốt

**Cách chốt:** chỉ nhận nguồn miễn phí, gọi được bằng chương trình (không cần trình duyệt, không bị chặn bot), và điều khoản không cấm dùng nội bộ. Ngày 2026-10-15 đã gọi thử thật từng endpoint. Nguồn cần key mà chưa có key thì kiểm tra rằng endpoint phản hồi đúng (báo thiếu key) và đối chiếu tài liệu chính thức.

### 6.1 Dùng

| Ưu tiên | Nguồn | Dữ liệu | Key | Kết quả kiểm tra 2026-10-15 | Spec |
|---|---|---|---|---|---|
| P1 | **Binance public data** | Crypto spot 1 phút, funding rate, futures metrics | Không | ✅ Tải được file zip 1m, fundingRate, metrics | [specs/07-ingestion/02-sources/01-binance.md](specs/07-ingestion/02-sources/01-binance.md) |
| P1 | **Alpaca Market Data** | US equity + ETF 1 phút và daily (SIP, từ 2016) | Miễn phí | ⚠️ Endpoint trả 401 khi thiếu key (đúng). Tài liệu chính thức: gói Basic miễn phí có lịch sử từ 2016, 200 request/phút. **Cần tạo key để tải thử** | [specs/07-ingestion/02-sources/02-alpaca.md](specs/07-ingestion/02-sources/02-alpaca.md) |
| P1 | **Yahoo Finance** (chart API) | Daily US equity, ETF, index từ trước 2005; split, cổ tức | Không | ✅ Daily và 1m đều trả JSON. Không chính thức, có thể hỏng bất cứ lúc nào | [specs/07-ingestion/02-sources/03-yahoo.md](specs/07-ingestion/02-sources/03-yahoo.md) |
| P1 | **FRED / ALFRED** | Macro Mỹ, lãi suất, FX, commodity, có vintage; lịch công bố | Miễn phí | ✅ API phản hồi, báo thiếu key (đúng). Endpoint CSV `fredgraph.csv` bị timeout, không dùng | [specs/07-ingestion/02-sources/04-fred.md](specs/07-ingestion/02-sources/04-fred.md) |
| P2 | **SEC EDGAR** | Fundamentals (XBRL companyfacts), CIK ↔ ticker | Không (cần User-Agent có email) | ✅ companyfacts AAPL 3.8 MB, company_tickers.json | [specs/07-ingestion/02-sources/05-sec_edgar.md](specs/07-ingestion/02-sources/05-sec_edgar.md) |
| P2 | **S&P 500 constituents** (Wikipedia + GitHub fja05680/sp500) | Thành phần hiện tại và lịch sử thay đổi | Không | ✅ Cả hai tải được | [specs/07-ingestion/02-sources/06-sp500_constituents.md](specs/07-ingestion/02-sources/06-sp500_constituents.md) |
| P2 | **Nasdaq Trader symbol directory** | Danh sách mã đang niêm yết (Nasdaq + NYSE + khác) | Không | ✅ `nasdaqlisted.txt` tải được | [specs/07-ingestion/02-sources/07-nasdaq_trader.md](specs/07-ingestion/02-sources/07-nasdaq_trader.md) |
| P2 | **Kenneth French Data Library** | Factor Fama-French, momentum | Không | ✅ Tải được zip FF3 daily | [specs/07-ingestion/02-sources/08-french.md](specs/07-ingestion/02-sources/08-french.md) |
| P2 | **US Treasury** | Yield curve chính thức: par yield và real yield | Không | ✅ CSV par yield và real yield năm 2026 | [specs/07-ingestion/02-sources/09-treasury.md](specs/07-ingestion/02-sources/09-treasury.md) |
| P2 | **NY Fed Markets API** | SOFR, EFFR, OBFR | Không | ✅ JSON SOFR, EFFR | [specs/07-ingestion/02-sources/10-nyfed.md](specs/07-ingestion/02-sources/10-nyfed.md) |
| P3 | **EIA** | Giá và tồn kho năng lượng | Miễn phí | ✅ API phản hồi, báo thiếu key (đúng) | [specs/07-ingestion/02-sources/11-eia.md](specs/07-ingestion/02-sources/11-eia.md) |
| P3 | **BLS** | CPI chi tiết, việc làm | v1 không cần, v2 miễn phí | ✅ v1 trả dữ liệu CPI không cần key | [specs/07-ingestion/02-sources/12-bls.md](specs/07-ingestion/02-sources/12-bls.md) |
| P3 | **BEA** | GDP, thu nhập (NIPA) | Miễn phí | ✅ API phản hồi, báo key sai (đúng) | [specs/07-ingestion/02-sources/13-bea.md](specs/07-ingestion/02-sources/13-bea.md) |
| P3 | **ECB Data Portal** | Tỷ giá tham chiếu EUR, lãi suất euro | Không | ✅ CSV tỷ giá USD/EUR | [specs/07-ingestion/02-sources/14-ecb.md](specs/07-ingestion/02-sources/14-ecb.md) |
| P4 | **World Bank** | Chỉ số phát triển, nhiều nước | Không | ✅ JSON GDP Mỹ | [specs/07-ingestion/02-sources/15-worldbank.md](specs/07-ingestion/02-sources/15-worldbank.md) |
| P4 | **IMF** | WEO, IFS | Không | ✅ `api.imf.org` (SDMX) và DataMapper. Endpoint `dataservices.imf.org` không còn phản hồi | [specs/07-ingestion/02-sources/16-imf.md](specs/07-ingestion/02-sources/16-imf.md) |
| P4 | **OECD** | Macro OECD, leading indicators | Không | ✅ SDMX `sdmx.oecd.org` | [specs/07-ingestion/02-sources/17-oecd.md](specs/07-ingestion/02-sources/17-oecd.md) |

Thư viện offline (không crawl): `exchange_calendars` cho lịch sàn, xem [specs/08-reference/01-trading_calendar.md](specs/08-reference/01-trading_calendar.md).

**Hệ quả:** không có nguồn minute miễn phí cho FX, vàng, dầu và index (Dukascopy bị loại). Các nhóm này chỉ có daily (FRED, ECB, EIA, Yahoo).

### 6.2 Loại

| Nguồn | Lý do loại |
|---|---|
| Stooq | Đã chặn bot: mọi request CSV trả về trang JavaScript challenge, không tải được bằng chương trình |
| Dukascopy | Không kết nối được từ mạng dev (timeout cả datafeed lẫn trang chủ), và chưa tìm được điều khoản cho phép tải tự động. Có thể thử lại từ server bee |
| CME daily settlements | Trả 403, chặn IP vì "suspected web scraping". Điều khoản cấm scraping |
| Nasdaq Data Link (dataset free) | Trả 403 (Incapsula). Các dataset miễn phí như WIKI đã ngừng cập nhật |
| Cboe (VIX history CSV) | Lỗi TLS từ mạng dev. Không cần: VIX đã có từ FRED (`VIXCLS`) và Yahoo (`^VIX`) |
| CoinGecko | Bản miễn phí chỉ cho lịch sử 365 ngày (thử `days=max` bị 401). Binance đã đủ cho crypto |
| Polygon.io / Massive (gói free) | 5 request/phút, chỉ end-of-day, lịch sử ~2 năm. Không hơn gì Alpaca |
| Tiingo (gói free) | Giới hạn 50 request/giờ, và theo các nguồn bên thứ ba thì gói free chỉ cho dùng cá nhân. Dùng nội bộ công ty phải trả phí |
| Mọi nguồn trả phí | Ngoài ngân sách 0 USD |

### 6.3 Hạn chế của bộ nguồn miễn phí

| Hạn chế | Hậu quả | Cách giảm nhẹ |
|---|---|---|
| Giá của mã đã hủy niêm yết khó có (Yahoo và Alpaca thường không trả) | Survivorship bias trong backtest US equity | `gold.universe_daily` xuất báo cáo mã thuộc universe nhưng không có giá. Kết quả backtest phải kèm con số này |
| Yahoo không chính thức | Có thể hỏng bất cứ lúc nào | Đối chiếu với Alpaca daily (từ 2016). Connector Yahoo báo lỗi rõ khi format đổi |
| Minute pilot chỉ từ 2024 | Ít chế độ thị trường để kiểm chứng | Mở rộng về 2016 (Alpaca) và 2018 (Binance) khi có thêm disk |
| Không có minute cho FX, commodity, index | Không làm được alpha intraday cho các nhóm này | Chấp nhận ở prototype |
| Nhiều nguồn macro không có vintage | Phải ước lượng `available_from` khi backfill | Ước lượng nghiêng về phía muộn; đánh dấu `vintage_kind = observed` |
| Rate limit của API miễn phí | Backfill chậm | Backfill một lần, chạy qua đêm, có checkpoint |

---

## 7. Quyết định thiết kế

| Quyết định | Lý do |
|---|---|
| Parquet + DuckDB cho mọi dữ liệu giá, kể cả daily | Disk hạn chế, Parquet nén tốt (zstd mức 9 + byte_stream_split giảm ~20% so với mặc định trên nến 1 phút), DuckDB đọc trực tiếp |
| Postgres chỉ giữ metadata | Ít dữ liệu, cần ghi đồng thời từ nhiều task |
| Airflow gọi pipeline qua virtualenv riêng (`@task.external_python`) | Dependency của pipeline không xung đột với Airflow |
| DAG sinh từ `orchestration/schedules.py` | Thêm dataset hoặc đổi lịch chỉ sửa một chỗ, không viết DAG tay |
| Mỗi dataset ghi đúng một bảng silver | Hợp đồng đơn giản, dễ test, dễ truy nguồn gốc |
| Raw minute xoá sau khi ghi silver (giữ manifest) | Tải lại được, và chiếm nhiều chỗ nhất. Raw daily, macro được giữ |
| Silver giữ khoá theo nguồn (`symbol`, `series_id`); `security_id` chỉ có từ gold | Silver phản ánh đúng nguồn. Ánh xạ sang `security_id` cần security master, làm một lần ở gold |
| Một bảng silver có thể có nhiều nguồn (cột `source` nằm trong khoá) | Ví dụ `equity_bars_1d` có cả Yahoo và Alpaca để đối chiếu |
| Giá lưu bản gốc, gold lưu hệ số điều chỉnh thay vì giá đã điều chỉnh | Một nguồn sự thật; giá điều chỉnh được tính lại khi có corporate action mới |

---

## 8. Lộ trình

| Phase | Nội dung | Kết quả cần có |
|---|---|---|
| 0. Nền | `core`, `storage`, `schemas`, `metadata`, `quality`, `universe`, `ingestion.pipeline`, `cli`, `deploy` | `alphadata run` chạy được với một dataset giả; Docker stack lên được trên server bee |
| 1. Giá + macro chính | Nguồn P1: binance, alpaca, yahoo, fred. `orchestration` cho cả 4 | Các DAG hằng ngày chạy ổn định 1 tuần; backfill pilot xong |
| 2. Reference | Nguồn P2 và `reference.*` | Có `security_id`, lịch sàn, thành phần S&P 500 theo ngày, corporate actions đã đối chiếu |
| 3. Gold + access | `gold.*`, `access` | Agent gọi được `get_bars`, `get_macro` với `as_of`; có test PIT |
| 4. Mở rộng macro | Nguồn P3, P4 | Theo nhu cầu của agent |
