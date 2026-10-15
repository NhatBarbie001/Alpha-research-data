# Spec: `reference.index_membership`

> Code: `src/alphadata/reference/index_membership/` · Tầng: reference (bảng gold) · Phase 2
> Phụ thuộc: `reference.security_master`, silver `index_constituent_snapshots`, `index_constituent_changes`

## Mục đích

Cho biết **mã nào thuộc S&P 500 tại từng ngày** trong quá khứ. Đây là thứ giúp universe research không bị survivorship bias: backtest tại ngày `t` chỉ chọn trong các mã thuộc index tại `t`, kể cả mã sau này bị loại hoặc phá sản.

## Đầu vào

- `index_constituent_snapshots` (GitHub fja05680, từ 1996, cập nhật thủ công bởi tác giả).
- `index_constituent_changes` và snapshot hiện tại (Wikipedia).

Xem [ingestion/sources/sp500_constituents.md](../ingestion/sources/sp500_constituents.md).

## Bảng `index_membership`

| Cột | Ghi chú |
|---|---|
| index_code | `SP500` |
| security_id | |
| symbol | Ticker tại thời điểm vào index (để tra cứu) |
| valid_from | Ngày đầu tiên thuộc index |
| valid_to | Ngày cuối cùng thuộc index, `9999-12-31` nếu còn |
| source | `fja05680`, `wikipedia`, hoặc cả hai |

## Quy tắc dựng

1. Từ các snapshot GitHub liên tiếp, suy ra khoảng `[valid_from, valid_to]` của từng ticker.
2. Sau ngày snapshot cuối của GitHub, áp các thay đổi từ Wikipedia.
3. So snapshot hiện tại của Wikipedia với kết quả dựng được. Lệch → warn, ghi danh sách lệch.
4. Map ticker sang `security_id` qua `security_master.resolve(symbol, valid_from)`. Ticker không map được vẫn giữ dòng với `security_id` null và ghi vào báo cáo.
5. Sinh danh sách cho universe `sp500_daily` (mọi mã từng thuộc index từ 2005).

## API

```python
members(index_code, on_date) -> list[int]                  # security_id
membership_history(index_code, security_id) -> DataFrame
```

## Done when

- Mỗi ngày giao dịch từ 2005 có 495–510 thành viên (S&P 500 có hơn 500 mã vì một số công ty có nhiều class).
- Lehman Brothers thuộc S&P 500 đến tháng 9/2008.
- Tỷ lệ ticker không map được `security_id` được báo cáo và dưới 5%.
