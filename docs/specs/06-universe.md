# Spec: `universe`

> Code: `src/alphadata/universe/`, dữ liệu trong `config/universe/` · Tầng: xuyên suốt · Phase 0
> Phụ thuộc: `core`, `storage` (chỉ để đọc bảng gold cho universe sinh ra)

## Mục đích

Khai báo **cần crawl những mã và series nào**. CLI, backfill và orchestration đều đọc từ đây, không ai tự viết danh sách mã trong code.

Phân biệt với [09-gold/02-universe_daily.md](09-gold/02-universe_daily.md): `universe` là phạm vi crawl (cấu hình tĩnh), còn `universe_daily` là tập mã hợp lệ để research tại từng ngày (tính từ dữ liệu, có xét survivorship).

## Định dạng

Mỗi file YAML là một nhóm, tên file trùng `name`:

```yaml
# config/universe/crypto_pilot.yaml
name: crypto_pilot
datasets: [binance_spot_klines_1m]
key: symbol
start: 2024-01-01
members:
  - BTCUSDT
  - ETHUSDT
```

```yaml
# config/universe/fred_core.yaml
name: fred_core
datasets: [fred_series]
key: series_id
members:
  - {id: DGS10, note: "Lợi suất trái phiếu Mỹ 10 năm, %, daily"}
  - {id: CPIAUCSL, note: "CPI, đã điều chỉnh mùa vụ, monthly"}
```

| Trường | Bắt buộc | Ý nghĩa |
|---|---|---|
| `name` | ✓ | `snake_case`, trùng tên file |
| `datasets` | ✓ | Danh sách dataset dùng universe này (một universe có thể phục vụ nhiều dataset, ví dụ `us_etf_core` cho Alpaca 1m, Alpaca 1d, Yahoo) |
| `key` | ✓ | Khoá partition mà member điền vào, ví dụ `symbol`, `series_id` |
| `members` | ✓ | Chuỗi, hoặc object có `id` và tuỳ chọn `start`, `end`, `note` |
| `start` | | Ngày sớm nhất cần backfill |
| `note` | | Ghi chú |

`start`, `end` của member: cả hai đầu đều tính (quy ước của data model). Dùng cho cặp mới niêm yết hoặc mã đã hủy niêm yết.

**Mã phải là chuỗi.** YAML tự đổi `ON` thành `True`, `0700` thành số, `2024-01-01` thành ngày. Loader báo lỗi thay vì crawl sai mã; viết trong ngoặc kép: `"ON"`.

## API

```python
load(name, directory=None) -> Universe
list_universes(directory=None) -> list[str]
partitions(name, extra=None, *, on=None, directory=None) -> list[dict[str, str]]

@dataclass(frozen=True)
class Universe:
    name: str
    datasets: tuple[str, ...]
    key: str
    members: tuple[Member, ...]     # Member(id, start, end, note)
    start: date | None
    def member_ids(self, on=None) -> list[str]
    def partitions(self, extra=None, *, on=None) -> list[dict[str, str]]
```

- `directory` mặc định là `settings.universe_dir` (`ALPHADATA_UNIVERSE_DIR`, mặc định `./config/universe`).
- `partitions(extra={"period": "2026-10-14"}, on=date(2026, 10, 14))` → `[{"symbol": "BTCUSDT", "period": "2026-10-14"}, ...]`. Có `on` thì bỏ member không hoạt động vào ngày đó. `extra` chứa chính `key` thì `ValueError`.

**Lỗi** (`InvalidUniverse`, message bắt đầu bằng tên file): không phải mapping; thiếu trường bắt buộc; **trường lạ** (bắt lỗi gõ sai như `member:`); `name` sai hoặc khác tên file; `datasets` rỗng, trùng hoặc không phải `snake_case`; `members` rỗng; member trùng, `id` rỗng hoặc có khoảng trắng hai đầu, `id` bị YAML đổi kiểu; ngày sai; `start > end`; YAML hỏng. Tên universe không phải `snake_case` (ví dụ `../x`) thì `ValueError`; không có file thì `KeyError` kèm danh sách universe có sẵn.

**Kiểm với registry nằm ở test, không ở loader.** `universe` đứng trước `ingestion` trong hướng phụ thuộc nên không import registry. `tests/universe/test_repo_universes.py` đối chiếu mọi file trong `config/universe/`: dataset có trong registry, `key` thuộc `partition_keys` của dataset, mọi member qua `dataset.validate`. File sai thì CI fail. Người dùng universe (CLI, backfill) kiểm dataset được chọn có trong `datasets`.

## Universe pilot (đề xuất, chờ leader duyệt)

Có trong repo: `crypto_pilot.yaml` (P0-08). `crypto_futures_pilot.yaml` thêm ở P1-01; `us_etf_core`, `us_stock_pilot`, `us_index`, `fred_core`, `fred_wide` ở P1-05; `sp500_daily` ở P2-06.

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

- File YAML pilot của mỗi task có trong repo khi task xong, load được và qua test đối chiếu registry.
- Test lỗi với file thiếu trường, member trùng.
