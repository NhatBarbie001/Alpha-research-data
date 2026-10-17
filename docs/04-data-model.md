# 04. Thiết kế dữ liệu (database)

> Đọc sau [03-how-it-works.md](03-how-it-works.md). Đây là **bản thiết kế duy nhất** của mọi bảng trong hệ thống: cột, kiểu, khoá, quan hệ, cách lưu vật lý, phân quyền, lưu giữ. Spec của từng module chỉ mô tả logic và trỏ về đây khi nói đến bảng.
> Code định nghĩa bảng nằm ở `src/alphadata/schemas/` (Parquet) và `src/alphadata/metadata/tables.py` (Postgres). Có test so sánh code với tài liệu này, xem [specs/03-schemas.md](specs/03-schemas.md).

---

## 1. Tổng quan

Hệ thống có **hai loại lưu trữ**, mỗi loại cho một kiểu dữ liệu:

| | Lake (Parquet + DuckDB) | Postgres |
|---|---|---|
| Chứa | Toàn bộ dữ liệu thị trường, macro, reference | Metadata vận hành của pipeline |
| Tầng | Bronze (file gốc), silver, gold | `alphadata_meta` |
| Số bảng | 14 silver, 5 reference, 5 gold | 5 |
| Kích thước | ~6 GB (pilot) đến ~70 GB (đầy đủ) | Vài trăm MB |
| Kiểu ghi | Batch, ghi đè cả file một cách atomic | Nhiều task ghi đồng thời, từng dòng |
| Kiểu đọc | Quét cột trên nhiều triệu dòng (phân tích) | Tra cứu theo khoá |
| Truy vấn bằng | DuckDB (đọc thẳng file, không cần nạp) | SQLAlchemy Core |

Lake **không phải một database server**: mỗi bảng là một thư mục file Parquet, DuckDB tạo view đọc thẳng các file đó khi cần. Không có process database nào chạy thường trực cho lake, nên không tốn RAM khi không ai query.

Postgres còn chứa DB `airflow` của Airflow; DB đó do Airflow tự quản lý, không thiết kế ở đây.

---

## 2. Quy ước

### 2.1 Kiểu dữ liệu

| Kiểu logic | Arrow / Parquet | DuckDB | Postgres | Dùng cho |
|---|---|---|---|---|
| `string` | `string` (dictionary encoding) | `VARCHAR` | `text` | Mã, tên, enum |
| `int32` / `int64` | `int32` / `int64` | `INTEGER` / `BIGINT` | `integer` / `bigint` | Đếm, id |
| `float64` | `double` (byte_stream_split) | `DOUBLE` | `double precision` | Giá, volume, giá trị macro |
| `bool` | `bool` | `BOOLEAN` | `boolean` | Cờ |
| `date` | `date32` | `DATE` | `date` | Ngày giao dịch, ngày quan sát |
| `timestamp` | `timestamp[us, tz=UTC]` | `TIMESTAMPTZ` | `timestamptz` | Mọi thời điểm. **Luôn UTC** |
| `list<string>` | `list<string>` | `VARCHAR[]` | | Cờ DQ, danh sách nguồn |

- Giá dùng `float64`, không dùng decimal: dữ liệu nguồn vốn là số thực và DuckDB tính nhanh hơn nhiều trên `double`.
- Volume dùng `float64` vì crypto có volume lẻ.
- Enum lưu dạng `string` (dictionary encoding của Parquet đã nén tốt). Giá trị hợp lệ liệt kê ở mục 8 và được `quality.schema_match` kiểm tra.

### 2.2 Tên

| Quy ước | Ví dụ |
|---|---|
| Tên bảng: `snake_case`, danh từ số nhiều hoặc mô tả nội dung, hậu tố tần suất nếu có | `equity_bars_1d`, `crypto_bars_1m` |
| Thời điểm: `ts_utc` cho bars, hậu tố `_at` cho sự kiện | `ingested_at`, `checked_at` |
| Ngày: `date` (ngày giao dịch theo lịch sàn), `obs_date` (ngày của quan sát macro), hậu tố `_date` cho các ngày khác | `ex_date`, `filed_date` |
| Khoảng hiệu lực: cặp `valid_from` / `valid_to` (thực thể) hoặc `available_from` / `available_to` (giá trị PIT), **cả hai đầu đều tính**, đầu mở là `9999-12-31` | |
| Định danh nguồn: `symbol`, `series_id`, `cik` | |
| Định danh nội bộ: `security_id` | |

### 2.3 Cột bắt buộc

| Tầng | Cột bắt buộc |
|---|---|
| Silver | `source` (`string`), `ingested_at` (`timestamp`) |
| Reference, gold | `built_at` (`timestamp`): thời điểm dựng bảng |

Các cột này không lặp lại trong bảng chi tiết ở mục 4, 5, 6.

---

## 3. Sơ đồ quan hệ

**Nhóm chứng khoán** (xoay quanh `security_id`):

```
                         ┌───────────────────────────┐
                         │ security_master           │
                         │ PK security_id            │
                         │    cik, share_class       │
                         └─────────────┬─────────────┘
                                       │ 1 : N  (security_id)
     ┌──────────────────┬──────────────┼───────────────┬──────────────────┬──────────────────┐
     ▼                  ▼              ▼               ▼                  ▼                  ▼
 security_symbols   corporate_     index_          bars_1d            universe_daily     fundamentals_pit
 (ticker theo       actions        membership      bars_1m            (hợp lệ tại        (theo filing)
  thời gian,        (split,        (SP500 theo     (giá gốc +          ngày t)
  theo nguồn)        cổ tức)        ngày)           hệ số điều chỉnh)
     ▲                                                  │
     │ symbol + ngày → security_id                      │ date phải là ngày mở cửa
     │ (dùng khi dựng gold)                             ▼
 SILVER: equity_bars_1d / 1m, crypto_bars_1m,      trading_calendar
         equity_corporate_actions,                 PK (exchange, date)
         index_constituent_*, listing_snapshots
```

**Nhóm macro và fundamentals:**

```
 macro_series ──1:N── macro_observations ──────────────► macro_pit
 PK (source, series_id)  PK (source, series_id,            series_key = "<source>:<series_id>"
                             obs_date, available_from)

 factor_returns ────────────────────────────────────────► macro_pit
                                                           series_key = "french:<dataset>:<factor>"

 fundamentals_facts ── cik ──► security_master.cik ─────► fundamentals_pit (security_id)
```

**Không có khoá ngoại thật** trên Parquet. Quan hệ được kiểm tra bằng check `referential` của `quality` sau khi dựng gold (ví dụ: mọi `security_id` trong `bars_1d` phải có trong `security_master`).

**Hai hệ định danh:**
- Silver dùng định danh của nguồn (`symbol`, `series_id`, `cik`), vì silver phản ánh đúng nguồn.
- Gold dùng `security_id`. Ánh xạ làm **một lần** khi dựng gold, qua `security_symbols` (theo `symbol_source` và ngày) hoặc `security_master.cik`.

---

## 4. Bảng silver

Do `ingestion` ghi, mỗi dataset ghi đúng một bảng. Đường dẫn: `silver/<bảng>/<partition k=v>/part.parquet`. Cột partition (ví dụ `year`, `month`) chỉ nằm trong đường dẫn, DuckDB đọc ra bằng `hive_partitioning`.

### 4.1 Tóm tắt

| Bảng | Khoá | Partition | Sắp xếp trong file | Nguồn | Dòng / năm (pilot) |
|---|---|---|---|---|---|
| `crypto_bars_1m` | symbol, ts_utc | symbol, year, month | ts_utc | binance | ~5.3 triệu |
| `crypto_funding_rates` | symbol, ts_utc | symbol | ts_utc | binance | ~11 nghìn |
| `crypto_futures_metrics_5m` | symbol, ts_utc | symbol, year | ts_utc | binance | ~1 triệu |
| `equity_bars_1m` | symbol, ts_utc | symbol, year, month | ts_utc | alpaca | ~10 triệu |
| `equity_bars_1d` | source, symbol, date | source, symbol | date | yahoo, alpaca | ~400 nghìn |
| `equity_corporate_actions` | source, symbol, ex_date, action_type | source | symbol, ex_date | yahoo, alpaca | nhỏ |
| `macro_series` | source, series_id | source | series_id | macro (10 nguồn) | ~1 nghìn dòng tổng |
| `macro_observations` | source, series_id, obs_date, available_from | source, series_id | obs_date, available_from | macro (10 nguồn) | vài triệu tổng (kể cả vintage) |
| `macro_release_dates` | source, release_id, release_date | source | release_id, release_date | fred | nhỏ |
| `fundamentals_facts` | cik, taxonomy, concept, unit, period_start, period_end, accession | cik | concept, period_end | sec_edgar | sau khi có disk |
| `factor_returns` | dataset, factor, date, vintage_date | dataset | factor, date | french | nhỏ |
| `index_constituent_snapshots` | source, index_code, snapshot_date, symbol | index_code | snapshot_date, symbol | sp500_constituents | nhỏ |
| `index_constituent_changes` | source, index_code, date, symbol, action | index_code | date | sp500_constituents | nhỏ |
| `listing_snapshots` | source, snapshot_date, symbol | source, year | snapshot_date, symbol | nasdaq_trader, sec_edgar | ~3 triệu |

### 4.2 Giá

**`crypto_bars_1m`**: một dòng cho mỗi cặp mỗi phút.

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| symbol | string | | Tên cặp Binance, ví dụ `BTCUSDT` |
| ts_utc | timestamp | | Thời điểm mở nến |
| open, high, low, close | float64 | | Theo đồng quote (USDT) |
| volume | float64 | | Khối lượng đồng base |
| quote_volume | float64 | | Khối lượng đồng quote |
| trades | int64 | | Số giao dịch |
| taker_buy_base, taker_buy_quote | float64 | | Khối lượng mua chủ động |

**`crypto_funding_rates`**: symbol (string), ts_utc (timestamp, thời điểm tính funding), funding_interval_hours (int32), funding_rate (float64, dạng tỷ lệ, `0.0001` = 0.01%).

**`crypto_futures_metrics_5m`**: một dòng mỗi 5 phút, futures USDⓈ-M.

| Cột | Kiểu | Từ cột nguồn |
|---|---|---|
| symbol, ts_utc | string, timestamp | `symbol`, `create_time` |
| open_interest | float64 | `sum_open_interest` (đơn vị base) |
| open_interest_value | float64 | `sum_open_interest_value` (USDT) |
| top_trader_ls_ratio_accounts | float64 | `count_toptrader_long_short_ratio` |
| top_trader_ls_ratio_positions | float64 | `sum_toptrader_long_short_ratio` |
| ls_ratio_accounts | float64 | `count_long_short_ratio` |
| taker_buy_sell_vol_ratio | float64 | `sum_taker_long_short_vol_ratio` |

**`equity_bars_1m`**: giá gốc, không điều chỉnh.

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| symbol | string | | Ticker theo định dạng Alpaca |
| ts_utc | timestamp | | Thời điểm mở nến |
| open, high, low, close | float64 | | USD |
| volume | float64 | | Số cổ phiếu |
| trades | int64 | ✓ | |
| vwap | float64 | ✓ | |
| session | string | | `pre` / `regular` / `post` |
| feed | string | | `sip` |

**`equity_bars_1d`**: cả Yahoo và Alpaca, phân biệt bằng `source`.

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| source | string | | `yahoo` / `alpaca` |
| symbol | string | | Ticker theo định dạng của nguồn (`BRK-B` ở Yahoo, `BRK.B` ở Alpaca) |
| date | date | | Ngày giao dịch theo giờ sàn |
| open, high, low, close | float64 | | Yahoo: **đã điều chỉnh split, chưa điều chỉnh cổ tức**. Alpaca: giá gốc |
| volume | float64 | | |
| adj_close | float64 | ✓ | Chỉ Yahoo có (điều chỉnh split + cổ tức) |
| currency | string | | |
| exchange_tz | string | | Ví dụ `America/New_York` |

**`equity_corporate_actions`**

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| source | string | | `yahoo` / `alpaca` |
| symbol | string | | |
| ex_date | date | | Ngày không hưởng quyền |
| action_type | string | | `split` / `dividend` / `spinoff` / `symbol_change` |
| ratio | float64 | ✓ | Split: số cổ phiếu mới trên một cổ phiếu cũ (2:1 → 2.0, gộp 1:10 → 0.1) |
| amount | float64 | ✓ | Cổ tức tiền mặt trên một cổ phiếu |
| currency | string | ✓ | |
| new_symbol | string | ✓ | Với `symbol_change`, `spinoff` |
| raw_json | string | ✓ | Bản ghi gốc của nguồn, để đối chiếu |

### 4.3 Macro

**`macro_series`**: một dòng cho mỗi series.

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| source | string | | `fred`, `treasury`, `nyfed`, `eia`, `bls`, `bea`, `ecb`, `worldbank`, `imf`, `oecd` |
| series_id | string | | Định danh của nguồn |
| title | string | | |
| frequency | string | | `D` / `W` / `M` / `Q` / `A` |
| units | string | ✓ | |
| seasonal_adjustment | string | ✓ | |
| country | string | ✓ | ISO3 |
| release_id | string | ✓ | Nối với `macro_release_dates` nếu biết |
| default_release_lag_days | int32 | ✓ | Độ trễ công bố mặc định, dùng khi ước lượng `available_from` |
| notes | string | ✓ | Bản quyền bên thứ ba, ghi chú dự báo, ... |
| last_updated | timestamp | ✓ | Theo nguồn |

**`macro_observations`**: một dòng cho mỗi **vintage** của mỗi quan sát.

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| source, series_id | string | | |
| obs_date | date | | Ngày của số liệu (quý 3 → `YYYY-07-01`, tháng → ngày 1, năm → `YYYY-01-01`) |
| value | float64 | ✓ | Null khi nguồn ghi thiếu (FRED ghi `.`) |
| available_from | date | | Ngày đầu tiên giá trị này được biết |
| available_to | date | | Ngày cuối cùng giá trị này còn hiệu lực; `9999-12-31` nếu vẫn hiện hành |
| vintage_kind | string | | `official` (nguồn có vintage, ví dụ ALFRED) / `observed` (ghi nhận theo ngày crawl hoặc ước lượng khi backfill) |

Ràng buộc: với cùng (`source`, `series_id`, `obs_date`), các khoảng `[available_from, available_to]` không chồng nhau và nối liền nhau (check `vintage_order`). Cách tính `available_from` cho từng nguồn nằm trong spec nguồn.

**`macro_release_dates`**: source (string), release_id (string), release_name (string), release_date (date). Lịch công bố, dùng để ước lượng `available_from` cho nguồn không có vintage.

### 4.4 Fundamentals và factor

**`fundamentals_facts`**: một dòng cho mỗi số liệu trong mỗi filing (giữ cả bản gốc lẫn bản sửa).

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| cik | string | | 10 chữ số, có số 0 ở đầu |
| taxonomy | string | | `us-gaap` / `dei` |
| concept | string | | Ví dụ `Revenues` |
| unit | string | | Ví dụ `USD`, `shares` |
| value | float64 | | |
| period_start | date | ✓ | Null với số tại một thời điểm (bảng cân đối) |
| period_end | date | | |
| fiscal_year | int32 | ✓ | |
| fiscal_period | string | ✓ | `FY`, `Q1`–`Q4` |
| form | string | | `10-K`, `10-Q`, `10-K/A`, ... |
| filed_date | date | | Ngày nộp, là gốc của PIT |
| accession | string | | Số hiệu filing |
| frame | string | ✓ | Ví dụ `CY2024Q1` |

**`factor_returns`**

| Cột | Kiểu | Ghi chú |
|---|---|---|
| dataset | string | `ff3_daily` / `ff5_daily` / `mom_daily` |
| factor | string | `Mkt-RF`, `SMB`, `HML`, `RMW`, `CMA`, `RF`, `Mom` |
| date | date | |
| value | float64 | **Phần trăm** mỗi ngày (`0.5` = 0.5%), giữ nguyên đơn vị của nguồn |
| vintage_date | date | Ngày tải file |

### 4.5 Danh sách mã và thành phần index

**`index_constituent_snapshots`**: source (string: `fja05680` / `wikipedia`), index_code (string: `SP500`), snapshot_date (date), symbol (string, giữ nguyên dạng của nguồn), security_name (string, null).

**`index_constituent_changes`**: source (string), index_code (string), date (date), symbol (string), action (string: `add` / `remove`), security_name (string, null), reason (string, null).

**`listing_snapshots`**

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| source | string | | `nasdaq_trader` / `sec_edgar` |
| snapshot_date | date | | Nasdaq Trader: ngày trong dòng `File Creation Time` |
| symbol | string | | |
| name | string | | |
| exchange | string | ✓ | Mã sàn của nguồn |
| is_etf | bool | ✓ | |
| is_test_issue | bool | ✓ | |
| cik | string | ✓ | Chỉ SEC có |

---

## 5. Bảng reference

Do `reference` ghi, dựng lại từ silver mỗi ngày. Bảng nhỏ, mỗi bảng một file (`gold/<bảng>/part.parquet`), không partition.

**`trading_calendar`**

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| exchange | string | | `XNYS` (mọi equity và ETF Mỹ) / `CRYPTO` (24/7) |
| date | date | | |
| is_open | bool | | |
| open_utc, close_utc | timestamp | ✓ | Phiên chính. Null khi đóng cửa |
| early_close | bool | | |
| pre_open_utc, post_close_utc | timestamp | ✓ | Mỹ: 04:00 và 20:00 giờ New York, quy đổi theo đúng ngày |
| calendar_version | string | | `exchange_calendars==<phiên bản>` |

Khoá (`exchange`, `date`). Phủ từ 2000-01-01 đến hết năm sau năm hiện tại.

**`security_master`**

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| security_id | int64 | | Khoá. Tăng dần, **không bao giờ tái sử dụng**, không đổi khi dựng lại |
| asset_class | string | | `equity` / `etf` / `index` / `crypto` |
| name | string | | Tên gần nhất |
| cik | string | ✓ | Equity Mỹ |
| share_class | string | ✓ | Ví dụ `B` của `BRK.B` |
| first_seen, last_seen | date | | Khoảng có giá hoặc có trong snapshot |
| status | string | | `active` / `delisted` / `unknown` |
| delist_reason | string | ✓ | |

Ràng buộc: (`cik`, `share_class`) duy nhất khi `cik` có giá trị.

`security_id` phải ổn định qua các lần dựng lại: job dựng đọc bảng cũ, giữ nguyên id của chứng khoán đã có, chỉ cấp id mới (lớn hơn id lớn nhất) cho chứng khoán mới.

**`security_symbols`**: ánh xạ ticker theo thời gian và theo nguồn.

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| security_id | int64 | | |
| symbol_source | string | | `canonical` / `yahoo` / `alpaca` / `binance` |
| symbol | string | | |
| exchange | string | ✓ | |
| valid_from, valid_to | date | | `valid_to = 9999-12-31` nếu còn hiệu lực |

Khoá (`symbol_source`, `symbol`, `valid_from`). Ràng buộc: với cùng (`symbol_source`, `symbol`), các khoảng hiệu lực không chồng nhau, nên một ticker tại một ngày chỉ ra đúng một `security_id`.

**`corporate_actions`**: đã đối chiếu giữa các nguồn.

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| security_id | int64 | | |
| ex_date | date | | |
| action_type | string | | `split` / `dividend` / `spinoff` / `symbol_change` |
| ratio | float64 | ✓ | |
| amount | float64 | ✓ | |
| currency | string | ✓ | |
| sources | list<string> | | Các nguồn có sự kiện này |
| status | string | | `confirmed` / `single_source` / `conflict` |
| detail | string | ✓ | Ghi chú khi `conflict` |

Khoá (`security_id`, `ex_date`, `action_type`).

**`index_membership`**

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| index_code | string | | `SP500` |
| security_id | int64 | ✓ | Null nếu không ánh xạ được (có báo cáo) |
| symbol | string | | Ticker lúc vào index |
| valid_from, valid_to | date | | Khoảng thuộc index |
| sources | list<string> | | `fja05680`, `wikipedia` |

Khoá (`index_code`, `symbol`, `valid_from`).

---

## 6. Bảng gold

Do `gold` ghi, `access` đọc. Mọi bảng khoá theo `security_id` (trừ `macro_pit`).

### 6.1 Tóm tắt

| Bảng | Khoá | Partition | Sắp xếp trong file |
|---|---|---|---|
| `bars_1d` | security_id, date | year | security_id, date |
| `bars_1m` | security_id, ts_utc | asset_class, year, month | security_id, ts_utc |
| `universe_daily` | universe_code, date, security_id | universe_code, year | date, security_id |
| `macro_pit` | series_key, obs_date, available_from | source | series_key, obs_date, available_from |
| `fundamentals_pit` | security_id, concept, unit, period_start, period_end, accession | (không) | security_id, concept, period_end |

### 6.2 Chi tiết

**`bars_1d`**

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| security_id | int64 | | |
| date | date | | |
| open, high, low, close | float64 | | **Giá gốc**, chưa điều chỉnh gì (phần Yahoo đã được bỏ điều chỉnh split) |
| volume | float64 | | |
| split_factor | float64 | | Nhân với giá để ra giá chỉ điều chỉnh split |
| adj_factor | float64 | | Nhân với giá để ra giá điều chỉnh split + cổ tức |
| source | string | | `alpaca` (từ 2016) / `yahoo` (trước 2016) / `binance` |
| dq_flags | list<string> | | Ví dụ `price_jump`, `cross_source_diff`. Rỗng nếu không có |

Không lưu cột giá đã điều chỉnh: giá điều chỉnh = giá gốc × hệ số, để chỉ có một nguồn sự thật.

**`bars_1m`**: security_id (int64), ts_utc (timestamp), open, high, low, close, volume (float64), trades (int64, null), vwap (float64, null), session (string), split_factor (float64), source (string). Crypto: `session = regular`, `split_factor = 1`. Partition `asset_class` (`equity` / `crypto`) giúp query một loại không đọc file của loại kia.

**`universe_daily`**

| Cột | Kiểu | Ghi chú |
|---|---|---|
| universe_code | string | `SP500` / `US_ETF_CORE` / `US_MINUTE_PILOT` / `CRYPTO_PILOT` |
| date | date | Ngày giao dịch |
| security_id | int64 | |
| has_price | bool | Có bar daily ngày `date`. Mã thiếu giá vẫn có dòng với `false`, không bị bỏ |
| adv_20d | float64 (null) | Giá trị giao dịch trung bình 20 ngày, **tính đến `date - 1`**, USD |
| tradable | bool | `has_price` và `adv_20d` trên ngưỡng |

**`macro_pit`**

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| series_key | string | | `<source>:<series_id>`, ví dụ `fred:CPIAUCSL`; factor: `french:<dataset>:<factor>` |
| obs_date | date | | |
| value | float64 | ✓ | |
| available_from, available_to | date | | |
| vintage_kind | string | | `official` / `observed` |
| frequency | string | | |
| units | string | ✓ | |

**`fundamentals_pit`**

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| security_id | int64 | | Từ `cik` qua `security_master` |
| concept, unit | string | | |
| value | float64 | | |
| period_start | date | ✓ | |
| period_end | date | | |
| fiscal_period | string | ✓ | |
| form, accession | string | | |
| available_from | date | | `filed_date + 1 ngày` |
| available_to | date | | Ngày trước filing kế tiếp sửa cùng số liệu, hoặc `9999-12-31` |

### 6.3 Lineage: gold dựng từ đâu

| Bảng gold / reference | Dựng từ | Module |
|---|---|---|
| `trading_calendar` | Thư viện `exchange_calendars` | `reference.trading_calendar` |
| `security_master`, `security_symbols` | `listing_snapshots`, `index_constituent_*`, `equity_corporate_actions` (symbol_change), `equity_bars_1d`, `crypto_bars_1m`, `config/reference/security_overrides.yaml` | `reference.security_master` |
| `corporate_actions` | `equity_corporate_actions`, `equity_bars_1d` (kiểm tra bằng giá), `security_symbols` | `reference.corporate_actions` |
| `index_membership` | `index_constituent_snapshots`, `index_constituent_changes`, `security_symbols` | `reference.index_membership` |
| `bars_1d` | `equity_bars_1d`, `crypto_bars_1m` (gộp ngày UTC), `security_symbols`, `corporate_actions` | `gold.bars` |
| `bars_1m` | `equity_bars_1m`, `crypto_bars_1m`, `security_symbols`, `bars_1d` (split_factor) | `gold.bars` |
| `universe_daily` | `index_membership`, `bars_1d`, `config/universe/*.yaml` | `gold.universe_daily` |
| `macro_pit` | `macro_observations`, `macro_series`, `factor_returns` | `gold.macro_pit` |
| `fundamentals_pit` | `fundamentals_facts`, `security_master` | `gold.macro_pit` |

Bảng này cũng được ghi vào `table_catalog` (cột `notes`), để `alphadata catalog` trả lời được "bảng này lấy từ đâu".

---

## 7. Bảng metadata (Postgres `alphadata_meta`)

Do `metadata` ghi. Định nghĩa bằng SQLAlchemy Core, chạy được trên SQLite (máy dev) và Postgres (server).

```
ingestion_runs 1 ──── N raw_files
       │ 1
       └──────── N dq_results (run_id null với check sau khi ghi)
backfill_checkpoints (theo backfill_id)        table_catalog (theo table_name, source)
```

**`ingestion_runs`**: một dòng cho mỗi lần `run_partition`.

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| run_id | uuid | | PK |
| source, dataset | text | | |
| partition | jsonb | | Ví dụ `{"symbol": "BTCUSDT", "date": "2026-10-11"}` |
| trigger | text | | `cli` / `airflow` / `backfill` |
| airflow_run_id | text | ✓ | |
| backfill_id | uuid | ✓ | |
| started_at | timestamptz | | |
| finished_at | timestamptz | ✓ | |
| status | text | | `running` / `success` / `failed` / `skipped_not_available` |
| rows_in, rows_out | bigint | ✓ | Dòng mới / tổng dòng file silver sau merge |
| silver_path | text | ✓ | |
| error | text | ✓ | Tối đa 4000 ký tự |

Index: (`dataset`, `started_at` desc), (`status`, `started_at` desc).

**`raw_files`**

| Cột | Kiểu | Ghi chú |
|---|---|---|
| id | bigserial | PK |
| run_id | uuid | FK → `ingestion_runs` |
| path | text | Đường dẫn trong lake |
| url | text | **Đã che API key** |
| sha256 | char(64) | |
| bytes | bigint | |
| fetched_at | timestamptz | |
| deleted | boolean | `true` khi raw đã bị xoá (manifest vẫn còn) |

Index: (`run_id`), (`sha256`).

**`dq_results`**

| Cột | Kiểu | Null | Ghi chú |
|---|---|---|---|
| id | bigserial | | PK |
| run_id | uuid | ✓ | FK → `ingestion_runs`. Null với check sau khi ghi |
| table_name | text | | |
| check_name | text | | |
| severity | text | | `error` / `warn` |
| passed | boolean | | |
| detail | text | ✓ | Ví dụ `bad_rows=3` |
| checked_at | timestamptz | | |

Index: (`table_name`, `checked_at` desc), (`passed`, `checked_at` desc), (`run_id`).

**`table_catalog`**: PK (`table_name`, `source`).

| Cột | Kiểu | Ghi chú |
|---|---|---|
| table_name, source | text | Một bảng có thể có nhiều nguồn |
| layer | text | `silver` / `reference` / `gold` |
| dataset | text | Null với reference, gold |
| description | text | |
| source_url | text | |
| license_note | text | Tóm tắt điều khoản |
| key_cols | text | |
| path_pattern | text | |
| refresh_schedule | text | |
| schema_version | integer | Xem mục 9.2 |
| notes | text | Lineage, lưu ý (ví dụ Yahoo `close` đã điều chỉnh split) |
| updated_at | timestamptz | |

**`backfill_checkpoints`**: PK (`backfill_id`, `partition_hash`).

| Cột | Kiểu | Ghi chú |
|---|---|---|
| backfill_id | uuid | |
| partition_hash | char(64) | SHA-256 của `partition` đã sort key |
| dataset | text | |
| partition | jsonb | |
| status | text | `pending` / `done` / `failed` |
| attempts | integer | |
| last_error | text | |
| updated_at | timestamptz | |

Index: (`backfill_id`, `status`).

Trên SQLite, `uuid` và `jsonb` lưu dạng `text`.

---

## 8. Giá trị enum

| Cột | Giá trị |
|---|---|
| `equity_bars_1m.session`, `bars_1m.session` | `pre`, `regular`, `post` |
| `*.action_type` | `split`, `dividend`, `spinoff`, `symbol_change` |
| `macro_*.frequency` | `D`, `W`, `M`, `Q`, `A` |
| `*.vintage_kind` | `official`, `observed` |
| `security_master.asset_class` | `equity`, `etf`, `index`, `crypto` |
| `security_master.status` | `active`, `delisted`, `unknown` |
| `security_symbols.symbol_source` | `canonical`, `yahoo`, `alpaca`, `binance` |
| `corporate_actions.status` | `confirmed`, `single_source`, `conflict` |
| `universe_daily.universe_code` | `SP500`, `US_ETF_CORE`, `US_MINUTE_PILOT`, `CRYPTO_PILOT` |
| `ingestion_runs.status` | `running`, `success`, `failed`, `skipped_not_available` |
| `ingestion_runs.trigger` | `cli`, `airflow`, `backfill` |
| `dq_results.severity` | `error`, `warn` |

---

## 9. Thiết kế vật lý

### 9.1 File Parquet

| Thông số | Giá trị | Lý do |
|---|---|---|
| Nén | zstd mức 9 | Disk là ràng buộc chặt nhất |
| Cột số thực | `byte_stream_split` | Nhỏ hơn ~20% so với mặc định trên nến 1 phút |
| Cột string | Dictionary encoding (mặc định) | Mã, enum lặp lại nhiều |
| Row group | ~1 triệu dòng | Đủ lớn để nén tốt, đủ nhỏ để DuckDB bỏ qua theo thống kê min/max |
| Thống kê | Bật min/max cho mọi cột | DuckDB dùng để bỏ qua row group không khớp điều kiện |
| Kích thước file | 1–200 MB | Minute: một file mỗi mã mỗi tháng (~2–3 MB); tránh hàng chục nghìn file quá nhỏ |
| Ghi | File tạm rồi `os.replace` | Không bao giờ có file ghi dở |

**Partition và thứ tự sắp xếp được chọn theo cách query chính:**

| Câu hỏi thường gặp | Nhờ đâu mà nhanh |
|---|---|
| "2 năm nến 1 phút của 50 mã" | Partition `symbol/year/month` (silver) hoặc `asset_class/year/month` + sắp xếp theo `security_id` (gold): chỉ đọc đúng file và row group cần |
| "Toàn bộ S&P 500 ngày `t`" (cross-section) | `bars_1d` partition theo `year`: một năm chỉ ~1 file, đọc nhanh dù quét cả năm |
| "Chuỗi CPI tại `as_of`" | `macro_pit` sắp xếp theo `series_key`: thống kê min/max loại hết row group của series khác |

### 9.2 Thay đổi schema

| Thay đổi | Cách làm |
|---|---|
| Thêm cột có thể null | Sửa `schemas`, tăng `schema_version` trong catalog. File cũ thiếu cột thì DuckDB đọc ra null (`union_by_name = true`). Không cần ghi lại file cũ |
| Đổi kiểu, đổi tên, xoá cột, đổi khoá | Tạo bảng mới (ví dụ `equity_bars_1d_v2`), dựng lại từ raw (silver) hoặc từ silver (gold), chuyển `access` sang bảng mới, rồi xoá bảng cũ |
| Đổi partition | Như trên |
| Bảng Postgres | File migration SQL có số thứ tự trong `deploy/migrations/`, chạy khi deploy. Không sửa bảng tay |

Gold và reference luôn dựng lại được hoàn toàn từ silver, nên thay đổi ở gold rẻ. Silver dựng lại được từ raw với các nguồn còn giữ raw; với raw đã xoá (minute) thì phải tải lại từ nguồn.

### 9.3 Lưu giữ

| Dữ liệu | Giữ |
|---|---|
| Raw minute (Binance klines, Alpaca 1m) | Xoá ngay sau khi ghi silver; giữ manifest và dòng `raw_files` |
| Raw daily, macro, reference | Mọi lần tải trong 30 ngày gần nhất; sau đó chỉ giữ lần tải đầu tiên (backfill) và lần tải mới nhất của mỗi partition |
| Silver, reference, gold | Giữ toàn bộ |
| `ingestion_runs`, `raw_files`, `table_catalog`, `backfill_checkpoints` | Giữ toàn bộ (nhỏ) |
| `dq_results` | 1 năm |
| Log Airflow | 7 ngày |
| Backup Postgres | 7 bản gần nhất |

---

## 10. Phân quyền

| Đối tượng | Lake (`/data/alphadata`) | Postgres `alphadata_meta` |
|---|---|---|
| Pipeline (Airflow, CLI trên server) | Đọc + ghi raw, silver, gold (user `50000`) | Role `alphadata_writer`: đọc + ghi mọi bảng |
| Agent | Chỉ đọc `gold/`, qua DuckDB `read_only` | Không cần quyền |
| Team research | Chỉ đọc silver và gold | Role `alphadata_reader`: chỉ `SELECT` |
| Airflow | | Không có quyền trên `alphadata_meta` (dùng DB `airflow` riêng) |

API key không bao giờ nằm trong bất kỳ bảng hay file nào của lake; URL trong `raw_files` và manifest đã được che key.
