# Spec: `universe`

> Code: `src/alphadata/universe/`, dữ liệu trong `config/universe/` · Tầng: xuyên suốt · Phase 0
> Phụ thuộc: `core`, `storage` (chỉ để đọc bảng gold cho universe sinh ra)

## Mục đích

Khai báo **cần crawl những mã và series nào**. CLI, backfill và orchestration đều đọc từ đây, không ai tự viết danh sách mã trong code.

Phân biệt với [09-gold/02-universe_daily.md](09-gold/02-universe_daily.md): `universe` là phạm vi crawl (cấu hình tĩnh), còn `universe_daily` là tập mã hợp lệ để research tại từng ngày (tính từ dữ liệu, có xét survivorship).

## Định dạng

Mỗi file YAML là một nhóm:

```yaml
# config/universe/crypto_pilot.yaml
name: crypto_pilot
dataset: binance_spot_klines_1m
key: symbol
start: 2024-01-01
members:
  - BTCUSDT
  - ETHUSDT
```

```yaml
# config/universe/fred_core.yaml
name: fred_core
dataset: fred_series
key: series_id
members:
  - {id: DGS10, note: "Lợi suất trái phiếu Mỹ 10 năm, %, daily"}
  - {id: CPIAUCSL, note: "CPI, đã điều chỉnh mùa vụ, monthly"}
```

`members` nhận chuỗi hoặc object có `id`. Trường tuỳ chọn mỗi member: `start`, `end`, `note`.

## API

```python
load(name) -> Universe            # Universe(name, dataset, key, start, members: list[Member])
list_universes() -> list[str]
partitions(name, **extra) -> list[dict]   # ví dụ [{"symbol": "BTCUSDT", "date": "2026-10-14"}, ...]
```

Lỗi khi: file thiếu trường bắt buộc, `dataset` không có trong registry, member trùng.

## Universe pilot (đề xuất, chờ leader duyệt)

| File | Dataset | Thành viên | Từ |
|---|---|---|---|
| `crypto_pilot.yaml` | `binance_spot_klines_1m` | Top 10 cặp USDT theo volume: BTC, ETH, SOL, BNB, XRP, DOGE, ADA, TRX, LINK, AVAX | 2024-01-01 |
| `crypto_futures_pilot.yaml` | `binance_funding_rate`, `binance_futures_metrics` | 10 cặp như trên | 2024-01-01 |
| `us_etf_core.yaml` | `alpaca_bars_1m`, `alpaca_bars_1d`, `yahoo_daily` | ~30 ETF: SPY, QQQ, IWM, DIA, XLK, XLF, XLE, XLV, XLI, XLY, XLP, XLU, XLB, XLRE, XLC, TLT, IEF, SHY, LQD, HYG, GLD, SLV, USO, UNG, UUP, EEM, EFA, VNQ, TIP, AGG | Minute 2024-01-01, daily 2005-01-01 |
| `us_stock_pilot.yaml` | `alpaca_bars_1m`, `alpaca_bars_1d`, `yahoo_daily` | 20 cổ phiếu S&P 500 có giá trị giao dịch trung bình lớn nhất (chốt cố định khi bắt đầu Phase 1, ghi ngày chốt vào file) | Như trên |
| `us_index.yaml` | `yahoo_daily` | ^GSPC, ^NDX, ^DJI, ^RUT, ^VIX, ^N225, ^GDAXI, ^FTSE, ^HSI, ^STOXX50E | 2000-01-01 |
| `sp500_daily.yaml` | `yahoo_daily`, `alpaca_bars_1d` | Sinh tự động từ `reference.index_membership`: mọi mã từng thuộc S&P 500 từ 2005 | 2005-01-01 |
| `fred_core.yaml` | `fred_series` | ~30 series chính có vintage (lãi suất, CPI, PCE, việc làm, GDP, dầu, USD index, VIX, spread tín dụng) | Dài nhất có thể |
| `fred_wide.yaml` | `fred_series_current` | ~500 series mở rộng (không lấy vintage, chỉ bản hiện hành + ghi nhận hằng ngày) | Dài nhất có thể |

`sp500_daily.yaml` là universe sinh ra: file chỉ khai báo `generated_from: gold/index_membership`. Loader đọc thẳng file Parquet của bảng đó qua `storage` (không import module `reference`, để giữ đúng hướng phụ thuộc). Bảng chưa được dựng thì loader báo lỗi rõ ràng.

## Done when

- Mọi file YAML pilot có trong repo và load được.
- Test lỗi với file thiếu trường, member trùng.
