# Spec: `reference.corporate_actions`

> Code: `src/alphadata/reference/corporate_actions/` · Tầng: reference (bảng gold) · Phase 2
> Phụ thuộc: `reference.security_master`, silver `equity_corporate_actions`, `equity_bars_1d`

## Mục đích

Một bảng split và cổ tức **đã đối chiếu giữa các nguồn**, theo `security_id`, để `gold.bars` tính giá điều chỉnh.

## Đầu vào

| Nguồn | Có | Không có |
|---|---|---|
| Yahoo (`yahoo_corporate_actions`) | Split, cổ tức tiền mặt, lịch sử dài | Spin-off, đổi mã |
| Alpaca (`alpaca_corporate_actions`) | Split, cổ tức, spin-off, đổi mã (tuỳ endpoint, xác nhận khi có key) | Có thể không có lịch sử dài |

## Bảng `corporate_actions`

| Cột | Ghi chú |
|---|---|
| security_id, ex_date, action_type | Khoá |
| ratio | Split: số cổ phiếu mới trên một cổ phiếu cũ (2:1 → 2.0, reverse 1:10 → 0.1) |
| amount, currency | Cổ tức tiền mặt trên một cổ phiếu |
| sources | Danh sách nguồn có sự kiện này |
| status | `confirmed` (≥ 2 nguồn khớp, hoặc 1 nguồn và giá xác nhận), `single_source`, `conflict` |
| detail | Ghi chú khi `conflict` |

## Quy tắc đối chiếu

1. Ghép sự kiện giữa nguồn theo (`security_id`, `action_type`, `ex_date` lệch ≤ 1 ngày làm việc).
2. Split: tỷ lệ phải khớp. Cổ tức: số tiền lệch ≤ 1%.
3. **Kiểm tra bằng giá**: split có `ratio = r` thì giá gốc (Alpaca `raw`) ngày `ex_date` so với ngày trước phải xấp xỉ `1/r` (sai số ±20% để chừa biến động thật). Không khớp → `conflict`.
4. `price_jump` warn ở daily mà không có sự kiện nào tương ứng → ghi vào báo cáo "nhảy giá chưa giải thích".
5. `conflict` không được dùng tự động trong `gold.bars`; cần quyết định qua `config/reference/security_overrides.yaml`.

## Done when

- Các split lớn đã biết (AAPL 4:1 2020-08-31, NVDA 10:1 2024-06-10, TSLA 3:1 2022-08-25) có `status = confirmed`.
- Báo cáo "nhảy giá chưa giải thích" chạy được và rỗng với universe pilot (hoặc mọi dòng đều có giải thích).
