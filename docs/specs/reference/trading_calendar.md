# Spec: `reference.trading_calendar`

> Code: `src/alphadata/reference/trading_calendar/` · Tầng: reference (bảng gold) · Phase 2
> Phụ thuộc: `storage`, `schemas`, thư viện `exchange_calendars`

## Mục đích

Biết ngày nào sàn mở cửa và mở mấy giờ, để:
- Phân biệt nến **thiếu do nghỉ lễ** với nến thiếu do lỗi dữ liệu.
- Gán phiên `pre` / `regular` / `post` cho minute US, kể cả ngày đóng cửa sớm.
- Xác định ngày giao dịch trước/sau khi tính lợi nhuận.

## Nguồn

Thư viện Python `exchange_calendars` (mã nguồn mở, chạy offline, không crawl). Lịch được cập nhật theo phiên bản thư viện: ghi phiên bản đang dùng vào catalog, nâng cấp thư viện thì dựng lại bảng.

## Bảng `trading_calendar`

| Cột | Ghi chú |
|---|---|
| exchange | `XNYS` (dùng cho mọi equity và ETF Mỹ), `CRYPTO` (24/7) |
| date | |
| is_open | |
| open_utc, close_utc | Giờ phiên chính, UTC. Null nếu đóng cửa |
| early_close | bool |
| pre_open_utc, post_close_utc | Mỹ: 04:00 và 20:00 giờ New York, đổi sang UTC theo ngày |
| source | `exchange_calendars==<phiên bản>` |

Phủ từ 2000-01-01 đến cuối năm sau năm hiện tại. Dựng lại mỗi năm một lần hoặc khi nâng cấp thư viện.

## API

```python
is_open(exchange, date) -> bool
sessions(exchange, start, end) -> DataFrame
session_of(ts_utc, exchange="XNYS") -> str     # "pre" | "regular" | "post" | "closed"
expected_minutes(exchange, date, session="regular") -> int
```

## Done when

- Ngày 2024-11-29 (sau Lễ Tạ ơn) có `early_close = true`, `close_utc = 18:00`.
- `session_of` đúng ở cả hai phía của ngày đổi giờ mùa hè/mùa đông.
- `missing_bars_vs_calendar` trong `quality` dùng được bảng này.
