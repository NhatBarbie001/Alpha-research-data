# Data Layer Spec: nền tảng data cho Alpha Research Agent

> Trạng thái: **DRAFT v0.1**, chờ leader review.
> Phạm vi: chỉ tầng data (thu thập → làm sạch → bảng chuẩn hoá → cấp cho agent đọc). Chưa bao gồm agent research, backtest, hay execution.

---

## 0. Đọc trước: vì sao data quan trọng hơn model

Agent research sinh ra "alpha" (tín hiệu có lợi nhuận vượt thị trường) bằng cách thử rất nhiều ý tưởng rồi backtest trên lịch sử. Nếu data sai, agent sẽ tìm ra alpha **ảo**, thường gặp các lỗi sau:

| Lỗi | Ý nghĩa | Ví dụ | Cách tầng data chặn |
|---|---|---|---|
| **Look-ahead bias** | Dùng thông tin chưa được công bố tại thời điểm đó | Dùng GDP quý 3 ngay từ ngày 1/10 trong khi số này chỉ công bố cuối tháng 10 | Lưu `release_time` / `available_at` cho mọi số macro và báo cáo tài chính |
| **Survivorship bias** | Chỉ có các cổ phiếu còn sống đến hôm nay | Bộ dữ liệu không có các công ty đã phá sản → backtest luôn đẹp | Giữ cả mã đã hủy niêm yết trong `security_master` |
| **Corporate action chưa điều chỉnh** | Giá bị "nhảy" khi chia tách (split) hoặc trả cổ tức | Cổ phiếu 2:1 split, giá giảm đột ngột 50% → agent tưởng là crash | Lưu cả giá gốc (`close`) lẫn giá đã điều chỉnh (`adj_close`) và bảng `corporate_actions` |
| **Lệch timezone / lệch phiên** | Nến 1 phút ghi giờ local, giờ UTC, hoặc gộp nhầm phiên | Nến "9:30" của NY bị ghi là 9:30 UTC | Chuẩn hoá toàn bộ về `ts_utc`, kèm `exchange` và `timezone` |
| **Dữ liệu bị revise** | Số macro thường được sửa sau khi công bố | GDP ban đầu 2.1%, sau revise còn 1.8% | Lưu theo `vintage` (ALFRED của FRED làm đúng việc này) |

**Nguyên tắc chung của spec này:**
1. **Raw trước, clean sau.** Lưu file gốc từ nguồn trước khi sửa. Luôn có thể chạy lại ETL mà không cần crawl lại.
2. **Mọi dòng đều biết nguồn gốc.** Mỗi bảng có cột `source` và `ingested_at`. Mỗi bảng được ghi vào data catalog.
3. **Point-in-time (PIT).** Mọi thứ agent đọc ở thời điểm `t` chỉ được phép là thứ đã biết tại `t`.
4. **Không sửa tay.** Mọi thay đổi đi qua code và DAG, có log.

---

## 1. Kiến trúc tổng thể

```
   Nguồn (API / file / web)
          │
          ▼
 ┌─────────────────────┐
 │ Airflow (bee server)│  điều phối lịch crawl, retry, alert
 └─────────┬───────────┘
           ▼
 [BRONZE] raw files gốc (Parquet/CSV/JSON) + checksum + manifest
           │   lưu trên disk server: /data/raw/<source>/<dataset>/<yyyy-mm-dd>/
           ▼
 [SILVER] bảng đã chuẩn hoá: đúng schema, đúng timezone, đã dedupe, đã kiểm tra
           │   lưu dạng Parquet partition + catalog trong Postgres
           ▼
 [GOLD]   bảng sẵn dùng cho research: adjusted bars, macro PIT, universe theo ngày
           │
           ▼
 Harness API (read-only) → Alpha research agent
```

**Vì sao không dồn hết vào một Postgres?**
- Minute data của US stock rất lớn. Ví dụ ~8.000 mã × 390 nến/ngày × 252 ngày × 20 năm ≈ **15 tỷ dòng**. Postgres chịu được nhưng chậm và tốn disk.
- Parquet + **DuckDB** đọc cực nhanh cho kiểu truy vấn "lấy 2 năm minute của 500 mã".
- **Postgres** vẫn giữ vai trò: metadata, catalog, security master, lịch sử chạy DAG, kết quả data-quality, bảng daily nhỏ.

**Quyết định mặc định (có thể đổi sau khi leader duyệt):**
- Metadata + daily bars: Postgres 16 (có thể bật TimescaleDB nếu cần)
- Minute + tick: Parquet partition theo `asset_class/exchange/year/month`, query bằng DuckDB
- Orchestration: Airflow 2.x chạy bằng Docker Compose trên bee server, executor `LocalExecutor`
- Ngôn ngữ ETL: Python 3.11+, thư viện chính: `pandas` / `polars`, `duckdb`, `pyarrow`, `requests`, `sqlalchemy`

---

## 2. Nguồn data đề xuất

> **Giai đoạn prototype chỉ dùng nguồn miễn phí (0 USD).** Danh sách nguồn được chọn và thứ tự làm nằm ở [scope.md](scope.md) mục 3.2. Bảng bên dưới là toàn cảnh, gồm cả nguồn trả phí để tham khảo khi mở rộng.
>
> Ghi chú: giá, giới hạn rate, và điều khoản sử dụng thay đổi thường xuyên. Mọi thông tin trong bảng cần được **xác minh lại** ở T1 trước khi cam kết dùng. Cột "Rủi ro" ghi những điểm đã biết cần cẩn thận.

### 2.1 Cổ phiếu Mỹ (US equities)

| Nguồn | Granularity | Lịch sử | Chi phí (ước) | Điểm mạnh | Rủi ro / lưu ý |
|---|---|---|---|---|---|
| **Norgate Data** | Daily (có intraday ở gói cao) | Dài, có delisted | Trả phí, gói vài trăm USD/năm | Survivorship-free, có index membership theo ngày | Chủ yếu chạy Windows; kiểm tra license redistribution |
| **Sharadar (Nasdaq Data Link)** | Daily + fundamentals | Dài | Trả phí | Có fundamentals PIT, có delisted | Chỉ daily; đắt hơn nếu cần nhiều bảng |
| **CRSP (qua WRDS)** | Daily, monthly | Rất dài | Cần tài khoản học thuật/tổ chức | Chuẩn vàng cho nghiên cứu | Thường không cho phép tự host, cần xem license |
| **Polygon.io (nay là Massive)** | Minute, daily, trades | Free: 2 năm. Trả phí: 5–10+ năm | Free: 5 request/phút. Starter ~29 USD/tháng | API tốt, có corporate actions | Free tier chỉ đủ để đối chiếu vài mã |
| **Alpaca Market Data** | Minute, daily | Từ ~2016 (chưa xác nhận chính thức) | **Miễn phí**: lịch sử full-market (SIP) cũ hơn 15 phút. Real-time SIP ~99 USD/tháng | **Nguồn US minute chính cho prototype** | Real-time free chỉ có IEX (không ảnh hưởng vì ta chỉ tải lịch sử). Kiểm tra rate limit và điều khoản lưu trữ |
| **Tiingo** | Daily, IEX intraday | Dài (EOD) | Có gói miễn phí + trả phí | Dữ liệu EOD khá sạch | Điều khoản redistribution cần đọc kỹ |
| **Databento** | Tick, minute | Từ 2017+ | Pay-as-you-go, tương đối đắt | Chất lượng cao, tick đầy đủ | Chi phí tăng nhanh nếu tải cả universe |
| **FirstRate Data / Kibot** | Minute, daily | Vài chục năm | Mua một lần theo gói | Mua lịch sử minute rẻ, dùng một lần | Cập nhật mới thì phải mua tiếp |
| **Stooq** | Daily | Dài | Miễn phí (CSV) | Dễ lấy, tốt để cross-check | Không có minute, chất lượng không đảm bảo |
| **Yahoo Finance (yfinance)** | Daily; minute chỉ ~60 ngày gần nhất | Dài (daily) | Miễn phí | Nhanh để prototype | **Không chính thức**, ToS không cho redistribution, hay đổi API. Chỉ dùng để prototype / cross-check |

**Đề xuất tạm:** daily dài hạn từ **Norgate hoặc Sharadar** (cần leader quyết chi phí), minute từ **Polygon** hoặc **Databento** cho một universe nhỏ trước, daily từ **Stooq/Yahoo** để đối chiếu chéo.

### 2.2 Chỉ số (Global index)

| Nguồn | Granularity | Ghi chú |
|---|---|---|
| **Stooq** | Daily | Có S&P 500, Nikkei, DAX, FTSE, VN-Index tuỳ mã |
| **Yahoo (^GSPC, ^N225, ^VNINDEX...)** | Daily | Dùng để cross-check |
| **Dukascopy** | Minute, tick (indices CFD) | Miễn phí tải, có các index toàn cầu; kiểm tra điều khoản |
| **FRED** | Daily (một số index) | Ít index nhưng sạch, có S&P 500 daily |
| Nguồn trả phí (Refinitiv/LSEG, Bloomberg) | Đầy đủ | Chỉ đề cập để biết; cần ngân sách |

### 2.3 Commodity / Futures

| Nguồn | Granularity | Ghi chú |
|---|---|---|
| **Nasdaq Data Link (một số dataset free)** | Daily | Có dataset miễn phí cho futures và spot |
| **EIA (US Energy Information Administration)** | Daily/weekly | Giá dầu, khí đốt, công khai, miễn phí, có API key |
| **CME daily settlements** | Daily | Settlement chính thức, file công khai |
| **Stooq** | Daily | Continuous futures, cần kiểm tra roll |
| **FirstRate Data (futures)** | Minute | Mua lịch sử một lần |
| **LME / ICE** | Daily | Thường trả phí cho dữ liệu đầy đủ |

**Rủi ro lớn của futures:** **roll** (chuyển hợp đồng). Phải lưu cả contract-level và continuous series, và ghi lại quy tắc roll.

### 2.4 Crypto

| Nguồn | Granularity | Ghi chú |
|---|---|---|
| **Binance public data (data.binance.vision)** | Minute, daily, trades | Miễn phí, file zip theo tháng, khá đầy đủ từ 2017 |
| **CoinGecko API** | Daily | Miễn phí có giới hạn; tốt cho market cap |
| **Kaiko** | Minute, tick | Trả phí, nhiều sàn |

### 2.5 Forex và lãi suất

| Nguồn | Granularity | Ghi chú |
|---|---|---|
| **Dukascopy** | Tick, minute | Miễn phí tải, dùng để có FX lịch sử dài |
| **ECB Statistical Data Warehouse** | Daily | Tỷ giá tham chiếu, miễn phí |
| **FRED** | Daily | Treasury yields, tỷ giá nhiều cặp, miễn phí |
| **US Treasury (fiscaldata.treasury.gov)** | Daily | Yield curve chính thức |

### 2.6 Macro kinh tế

| Nguồn | Dữ liệu | Ghi chú |
|---|---|---|
| **FRED** (+ **ALFRED** cho vintage) | CPI, GDP, unemployment, PMI, rates, hàng nghìn series | **Nguồn trung tâm.** Miễn phí, cần API key. ALFRED cho lịch sử revise |
| **BLS API** | Việc làm, CPI chi tiết | Miễn phí, có giới hạn request |
| **BEA API** | GDP, personal income | Miễn phí, cần key |
| **World Bank API** | Chỉ số phát triển, nhiều nước | Miễn phí, tần suất năm/quý |
| **IMF Data API** | IFS, WEO | Miễn phí |
| **OECD, BIS, Eurostat** | Macro châu Âu, credit, property | Miễn phí, API khác nhau |
| **Các ngân hàng trung ương** | Ví dụ ECB, BoJ | Từng nguồn có API riêng |
| **Việt Nam**: TCTK (GSO), NHNN (SBV) | CPI, GDP, tín dụng, tỷ giá, lãi suất VN | Thường không có API ổn định, có thể phải scrape |

### 2.7 Fundamentals và factor

| Nguồn | Dữ liệu | Ghi chú |
|---|---|---|
| **SEC EDGAR (XBRL)** | Báo cáo tài chính công ty Mỹ | Miễn phí. Dùng **filing date** làm thời điểm available, đây là chìa khoá PIT |
| **Sharadar** | Fundamentals đã chuẩn hoá, PIT | Trả phí, tiện nhất |
| **Kenneth French Data Library** | Factor (Fama-French), momentum | Miễn phí, daily/monthly, dùng để benchmark |

### 2.8 Thị trường Việt Nam (đề xuất bổ sung)

Vì team ở Việt Nam, nên cân nhắc thêm nhóm này:
- **HOSE / HNX / UPCoM**: cổ phiếu, VN-Index, VN30. Có thể dùng thư viện **vnstock** (wrapper của dữ liệu công khai, cần kiểm tra điều khoản) hoặc nhà cung cấp trả phí (FiinTrade, Fireant, v.v.).
- Cần quyết định: có đưa VN market vào universe không, và độ ưu tiên so với US.

---

## 3. Schema đề xuất

Quy ước chung cho mọi bảng:
- Thời gian lưu dạng **UTC**. Cột ngày (`date`) là ngày giao dịch của sàn tương ứng.
- Mỗi bảng có: `source` (ví dụ `polygon`, `stooq`, `fred`), `ingested_at` (UTC), `source_file` hoặc `source_run_id`.
- Khoá định danh là `security_id` (số nguyên nội bộ), không dùng ticker làm khoá vì ticker đổi theo thời gian.

### 3.1 Schema `ref` (tham chiếu)

**`ref.security_master`**: một dòng cho mỗi (security, sàn, ticker) theo khoảng thời gian.
| Cột | Kiểu | Ghi chú |
|---|---|---|
| security_id | bigint PK | ID nội bộ |
| ticker | text | Ticker tại thời điểm đó |
| exchange | text | NYSE, NASDAQ, HOSE, ... |
| asset_class | text | equity, index, future, fx, crypto, macro |
| currency | text | USD, VND, ... |
| country | text | |
| name | text | |
| sector / industry | text | Theo nguồn nào (GICS, ICB...) |
| listed_from / listed_to | date | `listed_to` null nếu còn niêm yết |
| delist_reason | text | Phá sản, sáp nhập, ... |
| source | text | |

**`ref.trading_calendar`**: `exchange`, `date`, `is_open`, `open_utc`, `close_utc`, `source`. Cần để biết nến nào "thiếu" là do nghỉ lễ, không phải lỗi.

**`ref.corporate_actions`**: `security_id`, `ex_date`, `action_type` (split, dividend, spinoff, ticker_change), `ratio`, `amount`, `source`.

**`ref.index_membership`**: `index_code`, `security_id`, `valid_from`, `valid_to`, `source`. Cho biết mã nào thuộc S&P 500 tại từng ngày, cần cho universe không bị survivorship.

### 3.2 Schema `px` (giá)

**`px.bars_daily`**
| Cột | Ghi chú |
|---|---|
| security_id, date | PK |
| open, high, low, close | Giá **gốc** (unadjusted) |
| adj_close | Đã điều chỉnh split + dividend |
| volume | |
| source, ingested_at | |

**`px.bars_minute`** (lưu Parquet, không phải bảng Postgres)
| Cột | Ghi chú |
|---|---|
| security_id, ts_utc | |
| open, high, low, close, volume | |
| source | |

Partition: `asset_class=…/exchange=…/year=…/month=…`.

**`px.futures_contract_daily`** và **`px.futures_continuous_daily`**: contract-level và chuỗi continuous, kèm `roll_rule` và `roll_date`.

**`px.fx_daily`**, **`px.crypto_bars_*`**: cùng cấu trúc với bars nhưng thêm `pair` và `quote_ccy`.

### 3.3 Schema `macro`

**`macro.series`**: `series_id`, `source`, `name`, `frequency` (D/W/M/Q/A), `unit`, `country`, `seasonal_adj`, `notes`.

**`macro.observations`** (quan trọng nhất để tránh look-ahead):
| Cột | Ghi chú |
|---|---|
| series_id, obs_date | Ngày của số liệu (ví dụ quý 3: 2026-09-30) |
| value | |
| vintage_date | Ngày số này được công bố/sửa (từ ALFRED hoặc ghi nhận lúc crawl) |
| available_at | Thời điểm agent được phép dùng. Thường = release time + độ trễ an toàn |
| source | |

Quy tắc: query PIT luôn lấy bản ghi có `available_at <= t`.

### 3.4 Schema `fund` (báo cáo tài chính)

**`fund.financials`**: `security_id`, `period_end`, `filing_date` (dùng làm available_at), `metric` (revenue, net_income, ...), `value`, `currency`, `form` (10-K, 10-Q), `source`.

### 3.5 Schema `meta` (vận hành)

- **`meta.source_catalog`**: `source_name`, `url`, `license`, `rate_limit`, `cost`, `contact`, `status`, `last_verified`.
- **`meta.table_catalog`**: `table_name`, `description`, `sources` (danh sách nguồn), `owner`, `refresh_schedule`, `last_updated`. Đây là "note nguồn từng bảng" mà leader yêu cầu.
- **`meta.ingestion_runs`**: `run_id`, `dag_id`, `source`, `dataset`, `started_at`, `finished_at`, `status`, `rows_in`, `rows_out`, `error`.
- **`meta.dq_results`**: `check_name`, `table_name`, `run_at`, `passed`, `detail`.

---

## 4. Quy tắc làm sạch (ETL silver)

1. **Dedupe** theo khoá (security_id, ts) và giữ bản mới nhất theo `ingested_at`.
2. **Chuẩn timezone**: mọi thứ về UTC. Với bar daily, giữ `date` theo lịch sàn.
3. **Kiểm tra OHLC**: `low <= min(open, close) <= max(open, close) <= high`, `volume >= 0`. Dòng vi phạm → đánh cờ, không xoá âm thầm.
4. **Thiếu nến**: so với `trading_calendar`. Nến thiếu trong phiên mở cửa phải được ghi vào bảng lỗi, không tự nội suy.
5. **Outlier giá**: đánh cờ thay đổi > X% trong 1 bar (X ~ 50% cho daily, cần xác nhận với corporate actions) để phân biệt lỗi data với sự kiện thật.
6. **Điều chỉnh**: tính `adj_close` từ `corporate_actions`, kiểm tra chéo với `adj_close` của nguồn.
7. **Đối chiếu chéo**: với cùng một mã, so sánh giữa 2 nguồn (ví dụ Stooq vs Polygon). Chênh lệch lớn hơn ngưỡng → báo cáo.
8. **Đơn vị**: volume (cổ phiếu vs lot), giá (cent vs dollar), tiền tệ (USD vs VND). Ghi rõ trong `unit` hoặc `currency`.

---

## 5. Danh sách task

Mỗi task có: **mục tiêu**, **đầu ra**, **tiêu chí hoàn thành (Done when)**, và **phụ thuộc**. Thứ tự gợi ý từ trên xuống. Các task có thể làm song song nếu không có mũi tên phụ thuộc.

### T0. Chốt phạm vi và ràng buộc
- **Mục tiêu:** trả lời các câu hỏi ở mục 7 để không làm thừa.
- **Đầu ra:** file `docs/scope.md` ghi quyết định.
- **Done when:** leader ký duyệt universe ban đầu, budget data, disk server, tần suất cập nhật.
- **Phụ thuộc:** không.

### T1. Lập source catalog
- **Mục tiêu:** với mỗi nguồn ở mục 2, xác minh: giá/gói hiện tại, rate limit, lịch sử có bao nhiêu năm, license cho phép lưu nội bộ không, cần key gì.
- **Đầu ra:** `docs/sources.md` (hoặc bảng `meta.source_catalog`), mỗi nguồn có `last_verified`.
- **Done when:** có ít nhất 2 nguồn cho mỗi nhóm (US equity, index, commodity, macro) để đối chiếu chéo. Mỗi nguồn ghi rõ "đã kiểm tra ToS: có/không".
- **Phụ thuộc:** T0.

### T2. Chuẩn bị hạ tầng trên server bee
- **Mục tiêu:** Docker, Postgres 16, thư mục `/data/raw`, `/data/silver`, `/data/gold` với quota disk, backup Postgres định kỳ.
- **Đầu ra:** `infra/docker-compose.yml`, `infra/README.md` mô tả cách chạy lại từ đầu.
- **Done when:** `docker compose up` trên bee server chạy được, Postgres kết nối được từ máy dev, backup thử nghiệm restore thành công.
- **Phụ thuộc:** T0 (cần biết disk và RAM).

### T3. Thiết kế và tạo schema
- **Mục tiêu:** tạo các schema `ref`, `px`, `macro`, `fund`, `meta` theo mục 3, kèm migration (Alembic hoặc SQL file có version).
- **Đầu ra:** `db/migrations/`, script tạo bảng.
- **Done when:** migration chạy được từ DB rỗng, có thể rollback, các cột bắt buộc (`source`, `ingested_at`) có constraint NOT NULL.
- **Phụ thuộc:** T2.

### T4. Dựng Airflow
- **Mục tiêu:** Airflow chạy bằng Docker Compose trên bee server, dùng chung Postgres hoặc DB riêng, có DAG mẫu.
- **Đầu ra:** `airflow/dags/`, template DAG gồm 3 bước: `fetch_raw → normalize_silver → dq_check`.
- **Done when:** DAG mẫu chạy thành công end-to-end, retry và alert (email hoặc Telegram/Slack) hoạt động khi task fail.
- **Phụ thuộc:** T2.

### T5. Lớp ingest raw (bronze)
- **Mục tiêu:** mỗi nguồn có một hàm `fetch` chỉ làm một việc: tải về và lưu nguyên file gốc, không biến đổi.
- **Đầu ra:** thư mục `ingest/<source>/`, file raw trong `/data/raw/<source>/<dataset>/<date>/`, file `manifest.json` (URL, thời gian tải, checksum SHA-256, số dòng).
- **Done when:** chạy lại cùng một ngày cho ra cùng checksum (idempotent), không ghi đè raw cũ.
- **Phụ thuộc:** T1, T4.

### T6. Ingest daily price
- **Mục tiêu:** lấy daily cho US equity, index, ETF, futures continuous, FX, crypto.
- **Đầu ra:** raw cho từng nguồn, rồi silver `px.bars_daily`, `px.futures_continuous_daily`, `px.fx_daily`.
- **Done when:** với 50 mã mẫu, so sánh 2 nguồn cho chênh lệch close < 0.5% (hoặc giải thích được), và `adj_close` khớp với nguồn đã điều chỉnh.
- **Phụ thuộc:** T3, T5.

### T7. Ingest minute price (giai đoạn 1: nhỏ)
- **Mục tiêu:** chỉ làm 1 universe nhỏ, ví dụ 50–100 mã US lớn + vài index, lịch sử 2 năm, để kiểm chứng pipeline trước khi scale.
- **Đầu ra:** Parquet trong `/data/silver/px/bars_minute/`, query thử bằng DuckDB.
- **Done when:** đếm số nến/ngày so với `trading_calendar`, số thiếu được ghi vào `meta.dq_results`.
- **Phụ thuộc:** T6.

### T8. Ingest minute price (giai đoạn 2: scale)
- **Mục tiêu:** mở rộng universe theo ngân sách, backfill theo tháng, có checkpoint để resume khi crash.
- **Done when:** có ước lượng thời gian và dung lượng đầy đủ, backfill chạy được qua đêm, không có tháng nào thiếu mà không được ghi nhận.
- **Phụ thuộc:** T7 và quyết định ngân sách.

### T9. Ingest macro (có PIT)
- **Mục tiêu:** FRED + ALFRED (vintage), BLS, BEA, World Bank, IMF, ECB, sau đó nguồn VN.
- **Đầu ra:** `macro.series`, `macro.observations` với `vintage_date` và `available_at`.
- **Done when:** với CPI và GDP, query tại 1 ngày quá khứ trả về đúng giá trị đã công bố lúc đó, không phải giá trị đã revise sau này. Có test cụ thể cho việc này.
- **Phụ thuộc:** T3, T5.

### T10. Reference data: security master, lịch, corporate actions, index membership
- **Mục tiêu:** dựng `ref.*`. Đây là task dễ bị bỏ qua nhưng quan trọng nhất cho chống survivorship.
- **Done when:** có mã đã delisted trong `security_master`, có index membership theo ngày cho ít nhất 1 index (ví dụ S&P 500), `trading_calendar` phủ đủ các sàn đang dùng.
- **Phụ thuộc:** T1, T3.

### T11. Fundamentals (SEC EDGAR, tuỳ chọn ở giai đoạn đầu)
- **Mục tiêu:** parse XBRL, lưu `fund.financials` với `filing_date`.
- **Done when:** với một công ty mẫu, số liệu khớp với báo cáo gốc và `available_at` = filing date.
- **Phụ thuộc:** T10.

### T12. Cleaning và chuẩn hoá (silver → gold)
- **Mục tiêu:** áp dụng các quy tắc mục 4, tạo gold: `gold.adj_bars_daily`, `gold.universe_daily` (mã nào hợp lệ tại ngày t), `gold.macro_pit` (view PIT).
- **Done when:** mỗi quy tắc có test, và mỗi dòng bị loại hoặc đánh cờ đều có lý do trong log.
- **Phụ thuộc:** T6, T9, T10.

### T13. Data quality checks và alert
- **Mục tiêu:** tự động chạy sau mỗi DAG: thiếu nến, OHLC sai, outlier, chênh lệch giữa nguồn, freshness (dữ liệu có cập nhật đúng hạn không).
- **Đầu ra:** bảng `meta.dq_results`, dashboard đơn giản (Grafana hoặc notebook).
- **Done when:** cố tình làm hỏng một bảng (ví dụ xoá vài nến), hệ thống phải báo lỗi.
- **Phụ thuộc:** T4, T12.

### T14. Data catalog và lineage
- **Mục tiêu:** điền `meta.table_catalog` cho mọi bảng: mô tả, nguồn, owner, lịch refresh. Với mỗi cột gold, ghi cột silver/raw nào tạo ra nó.
- **Done when:** không còn bảng nào thiếu thông tin nguồn, và leader có thể trả lời "số này đến từ đâu" trong 1 phút.
- **Phụ thuộc:** T12.

### T15. Harness API cho agent (read-only)
- **Mục tiêu:** một lớp Python duy nhất để agent đọc data, ví dụ `get_bars(universe, start, end, as_of)`, `get_macro(series, as_of)`. Tham số `as_of` bắt buộc, để không ai vô tình lấy data tương lai.
- **Done when:** có test chứng minh `get_macro(..., as_of=t)` không bao giờ trả dữ liệu có `available_at > t`.
- **Phụ thuộc:** T12, T9.

### T16. Tài liệu và runbook
- **Mục tiêu:** README cho người mới: cách thêm nguồn mới, cách backfill, cách xử lý khi DAG fail, cách restore DB.
- **Done when:** một người mới đọc xong có thể thêm một nguồn daily đơn giản mà không hỏi lại.
- **Phụ thuộc:** toàn bộ.

**Sơ đồ phụ thuộc rút gọn:**
```
T0 → T1 ──────────────┐
T0 → T2 → T3 → T4 ────┤
                 T5 ←─┤
                 T6 → T7 → T8
                 T9 (macro)
                 T10 → T11
                 T12 (cần T6, T9, T10) → T13 → T14 → T15 → T16
```

---

## 6. Thứ tự làm để có kết quả sớm

1. **Tuần 1:** T0, T1, T2, T3. Mục tiêu: có DB chạy trên bee server, có danh sách nguồn đã kiểm tra.
2. **Tuần 2:** T4, T5, T6. Mục tiêu: daily price chạy tự động, đối chiếu được 2 nguồn.
3. **Tuần 3:** T7, T9, T10. Mục tiêu: minute cho universe nhỏ, macro PIT, reference data.
4. **Tuần 4:** T12, T13. Mục tiêu: gold tables và cảnh báo lỗi.
5. **Sau đó:** T8 (scale), T11 (fundamentals), T14, T15, T16.

Lý do đẩy T15 (harness API) xuống cuối: agent chỉ cần một API ổn định. Làm sớm sẽ phải sửa nhiều lần khi schema đổi.

---

## 7. Câu hỏi mở cho leader

1. **Ngân sách data** cho năm đầu là bao nhiêu? Quyết định này quyết định nên dùng Norgate/Sharadar hay Yahoo + Stooq.
2. **Universe**: chỉ US, hay có VN và crypto? Bao nhiêu mã (50, 500, 3000)?
3. **Tần suất**: alpha nhắm đến horizon nào (intraday, daily, weekly)? Điều này quyết định có cần minute không.
4. **Lịch sử**: cần bao nhiêu năm? (Gợi ý: daily ≥ 15–20 năm, minute ≥ 2–3 năm.)
5. **Server bee**: CPU, RAM, disk còn trống bao nhiêu? (Minute US đầy đủ có thể cần hàng trăm GB đến vài TB.)
6. **Có được phép lưu và dùng lại data bên thứ ba** trong nội bộ không? Nhiều nhà cung cấp cấm redistribution, cần thoả thuận sớm.
7. **Agent** sẽ đọc data trực tiếp từ DuckDB/Parquet hay qua API service? (Ảnh hưởng T15.)
8. **Người chịu trách nhiệm** từng nhóm data (owner) là ai?

---

## 8. Tiêu chí thành công của tầng data (Definition of Done)

- [ ] Mọi bảng đều có `source` và được ghi trong `meta.table_catalog`.
- [ ] Mọi số liệu macro và fundamentals có `available_at`, và có test PIT.
- [ ] Universe có mã đã delisted (chống survivorship).
- [ ] Giá gốc và giá đã điều chỉnh đều có, và có kiểm tra corporate actions.
- [ ] DAG chạy lại được từ raw mà không cần crawl lại.
- [ ] Có dashboard hoặc báo cáo DQ hằng ngày.
- [ ] `get_*` của harness không bao giờ trả dữ liệu tương lai.

---

## Phụ lục A. Thuật ngữ nhanh

| Thuật ngữ | Giải thích |
|---|---|
| OHLCV | Open, High, Low, Close, Volume của một nến (bar) |
| Bar / nến | Gom giá trong một khoảng (1 phút, 1 ngày) |
| Adjusted price | Giá đã điều chỉnh cho split và cổ tức, để so sánh lịch sử liền mạch |
| Split | Chia tách cổ phiếu (2:1 nghĩa là số lượng gấp đôi, giá giảm một nửa) |
| Survivorship-free | Dữ liệu có cả các mã đã chết |
| PIT (point-in-time) | Dữ liệu đúng như đã biết tại thời điểm đó |
| Vintage | Một phiên bản của số liệu tại một thời điểm công bố |
| Roll (futures) | Chuyển sang hợp đồng tháng sau khi hợp đồng cũ sắp hết hạn |
| Universe | Tập mã được xét trong nghiên cứu |
| Lineage | Truy vết một số liệu đến nguồn gốc |
| Backfill | Tải lịch sử quá khứ một lần |
| Idempotent | Chạy lại nhiều lần vẫn cho cùng kết quả |
| Bronze / Silver / Gold | Raw / đã làm sạch / sẵn dùng cho nghiên cứu |
