# Spec: `schemas`

> Code: `src/alphadata/schemas/` · Tầng: xuyên suốt · Phase 0 (silver), Phase 3 (gold)
> Phụ thuộc: `core`

## Mục đích

Một chỗ duy nhất định nghĩa cột, kiểu, khoá và partition của mọi bảng silver và gold. Connector, `storage`, `quality`, `access` đều đọc từ đây. Sửa schema một bảng thì sửa ở đây và cập nhật file này.

## Quy ước chung

- Timestamp: `timestamp[us, UTC]`, tên cột `ts_utc` (bars) hoặc hậu tố `_at` (thời điểm sự kiện).
- Ngày: `date32`, tên `date` (ngày giao dịch theo lịch sàn) hoặc `obs_date` (ngày của quan sát macro).
- Giá: `float64`. Volume: `float64` (crypto có volume lẻ).
- Mọi bảng silver có `source` (text) và `ingested_at` (timestamp UTC). `source` nằm trong khoá nếu bảng nhận nhiều nguồn.
- Silver dùng định danh của nguồn (`symbol`, `series_id`). Gold dùng `security_id`.

```python
@dataclass(frozen=True)
class TableSchema:
    name: str
    layer: str                 # "silver" | "gold"
    columns: dict[str, pa.DataType]
    key: list[str]
    partition: list[str]       # các cột tạo thư mục k=v
    nullable: set[str]
```

## Bảng silver

| Bảng | Khoá | Partition | Nguồn |
|---|---|---|---|
| `crypto_bars_1m` | symbol, ts_utc | symbol, year, month | binance |
| `crypto_funding_rates` | symbol, ts_utc | symbol | binance |
| `crypto_futures_metrics_5m` | symbol, ts_utc | symbol, year | binance |
| `equity_bars_1m` | symbol, ts_utc | symbol, year, month | alpaca |
| `equity_bars_1d` | source, symbol, date | source, symbol | yahoo, alpaca |
| `equity_corporate_actions` | source, symbol, ex_date, action_type | source | yahoo, alpaca |
| `macro_observations` | source, series_id, obs_date, available_from | source, series_id | fred, treasury, nyfed, eia, bls, bea, ecb, worldbank, imf, oecd |
| `macro_series` | source, series_id | source | như trên |
| `macro_release_dates` | source, release_id, release_date | source | fred |
| `fundamentals_facts` | cik, taxonomy, concept, unit, period_end, accession | cik | sec_edgar |
| `factor_returns` | dataset, factor, date, vintage_date | dataset | french |
| `index_constituent_changes` | index_code, date, symbol, action | index_code | sp500_constituents |
| `index_constituent_snapshots` | index_code, snapshot_date, symbol | index_code | sp500_constituents |
| `listing_snapshots` | source, snapshot_date, symbol | source, year | nasdaq_trader, sec_edgar |

### Cột chính

**`crypto_bars_1m`**: symbol, ts_utc (open time), open, high, low, close, volume (base), quote_volume, trades, taker_buy_base, taker_buy_quote.

**`equity_bars_1m`**: symbol, ts_utc, open, high, low, close, volume, trades, vwap, session (`pre` / `regular` / `post`), feed (`sip`). Giá gốc, không điều chỉnh.

**`equity_bars_1d`**: source, symbol, date, open, high, low, close, volume, adj_close (null nếu nguồn không có), currency, exchange_tz.
Với Yahoo: `close` đã điều chỉnh split nhưng chưa điều chỉnh cổ tức, `adj_close` điều chỉnh cả hai. Ghi rõ trong catalog. Với Alpaca (`adjustment=raw`): giá gốc hoàn toàn.

**`equity_corporate_actions`**: source, symbol, ex_date, action_type (`split` / `dividend` / `spinoff` / `symbol_change`), ratio (split: số cổ phiếu mới trên một cổ phiếu cũ), amount (cổ tức, theo `currency`), currency, new_symbol, raw_json.

**`macro_observations`**:

| Cột | Ghi chú |
|---|---|
| source, series_id | `series_id` theo định danh của nguồn |
| obs_date | Ngày của số liệu (quý 3 → 2026-07-01 theo quy ước của FRED) |
| value | float64, null nếu nguồn ghi "." hoặc thiếu |
| available_from | Ngày đầu tiên giá trị này được biết |
| available_to | Ngày cuối cùng giá trị này còn hiệu lực. `9999-12-31` nếu vẫn là bản hiện hành |
| vintage_kind | `official` (nguồn có vintage, ví dụ ALFRED) hoặc `observed` (ghi nhận theo ngày crawl) |

Với `observed`: lần crawl đầu tiên của một quan sát có `available_from = obs_date + độ trễ công bố mặc định của series` khi backfill lịch sử, và `= ngày crawl` khi chạy hằng ngày. Quy tắc cụ thể nằm trong spec của từng nguồn.

**`macro_series`**: source, series_id, title, frequency (`D` / `W` / `M` / `Q` / `A`), units, seasonal_adjustment, country, notes, last_updated.

**`fundamentals_facts`**: cik, taxonomy (`us-gaap`, `dei`, ...), concept, unit, value, period_start (null cho số tại thời điểm), period_end, fiscal_year, fiscal_period, form, filed_date, accession, frame.

**`factor_returns`**: dataset (`ff3_daily`, `ff5_daily`, `mom_daily`, ...), factor, date, value (%/ngày), vintage_date (ngày tải file).

**`index_constituent_changes`**: index_code (`SP500`), date, symbol, action (`add` / `remove`), security_name, reason.

**`listing_snapshots`**: source, snapshot_date, symbol, name, exchange, is_etf, is_test_issue, cik (null nếu nguồn không có).

## Bảng gold

| Bảng | Khoá | Spec |
|---|---|---|
| `bars_1d` | security_id, date | [gold/bars.md](gold/bars.md) |
| `bars_1m` | security_id, ts_utc | [gold/bars.md](gold/bars.md) |
| `universe_daily` | universe_code, date, security_id | [gold/universe_daily.md](gold/universe_daily.md) |
| `macro_pit` | series_key, obs_date, available_from | [gold/macro_pit.md](gold/macro_pit.md) |
| `fundamentals_pit` | security_id, concept, period_end, available_from | [gold/macro_pit.md](gold/macro_pit.md) |

Bảng reference (`security_master`, `trading_calendar`, `corporate_actions`, `index_membership`) cũng là gold, định nghĩa trong spec của từng sub module ở [reference/](reference/).

## Done when

- Mọi bảng ở trên có `TableSchema` trong code.
- `storage.write_parquet_atomic` từ chối DataFrame không khớp schema.
- Có test sinh bảng markdown từ code, so với file này để phát hiện lệch.
