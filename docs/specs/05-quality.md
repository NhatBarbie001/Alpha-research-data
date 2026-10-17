# Spec: `quality`

> Code: `src/alphadata/quality/` · Tầng: xuyên suốt · Phase 0 (check trước khi ghi), Phase 2 (check sau khi ghi)
> Phụ thuộc: `core`, `metadata`, `schemas`

## Mục đích

Kiểm tra chất lượng dữ liệu ở hai thời điểm:
1. **Trước khi ghi silver** (trong `run_partition`): chặn dữ liệu hỏng.
2. **Sau khi ghi** (job riêng, theo lịch): freshness, đối chiếu giữa nguồn, thiếu nến so với lịch sàn.

Không bao giờ âm thầm xoá dòng lỗi.

## File

| File | Nội dung |
|---|---|
| `checks.py` | Hàm check thuần (nhận DataFrame, trả `CheckResult`) |
| `runner.py` | Chạy danh sách check, ghi `dq_results`, quyết định chặn hay không |
| `post_checks.py` | Check sau khi ghi, chạy trên silver/gold bằng DuckDB |

## Mức độ

| severity | Ý nghĩa |
|---|---|
| `error` | Không ghi silver, run fail với `DataQualityError` |
| `warn` | Vẫn ghi, kết quả được lưu và hiện trong báo cáo DQ |

```python
@dataclass
class CheckResult:
    name: str
    severity: str   # "error" | "warn"
    passed: bool
    detail: str     # ví dụ "bad_rows=3"
```

## Check trước khi ghi

| Check | Mức | Áp dụng |
|---|---|---|
| `not_empty` | error | Mọi dataset (trừ khi dataset khai báo partition có thể rỗng) |
| `no_null_keys` | error | Mọi dataset |
| `unique_keys` | error | Mọi dataset |
| `schema_match` | error | Mọi dataset: đủ cột, đúng kiểu theo `schemas` |
| `ts_in_range` | error | Bars: timestamp nằm trong khoảng của partition. Bắt lỗi sai đơn vị epoch |
| `ohlc_consistent` | warn | Bars: `low <= min(open, close)`, `max(open, close) <= high` |
| `non_negative` | warn | Volume, giá |
| `row_count` | warn | Crypto 1m: đúng 1440 nến/ngày |
| `price_jump` | warn | Daily: thay đổi > 50% trong một bar (có thể là split chưa điều chỉnh hoặc lỗi) |
| `vintage_order` | error | Macro: `available_from <= available_to`, không có hai vintage chồng nhau cho cùng một quan sát |

Mỗi connector chọn check của mình bằng cách override `Dataset.checks()`.

## Check sau khi ghi

| Check | Mức | Mô tả |
|---|---|---|
| `freshness` | warn | Dataset không có run thành công trong N giờ (N khai báo trong `orchestration/schedules.py`) |
| `missing_bars_vs_calendar` | warn | Equity minute và daily: so số nến với `reference.trading_calendar`, liệt kê ngày/phút thiếu |
| `cross_source_close` | warn | `equity_bars_1d`: chênh lệch close giữa Yahoo và Alpaca > 0.5% |
| `adj_vs_source` | warn | Giá điều chỉnh tự tính ở gold lệch giá điều chỉnh của Yahoo > 0.5% |
| `referential` | error | Thay cho khoá ngoại (Parquet không có): mọi `security_id` trong bảng gold và reference phải có trong `security_master`. Danh sách quan hệ ở [04-data-model.md](../04-data-model.md) mục 3 |

Kết quả ghi vào `dq_results` với `run_id` null. Có lệnh `alphadata dq report` in tóm tắt (xem [12-cli.md](12-cli.md)).

## Done when

- Mỗi check có test với dữ liệu đúng và dữ liệu sai.
- Cố tình xoá vài nến trong silver, `missing_bars_vs_calendar` phải báo.
- Một check `error` fail thì file silver không thay đổi.
