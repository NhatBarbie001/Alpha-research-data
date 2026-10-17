# Spec: `reference.index_membership`

> Code: `src/alphadata/reference/index_membership/` · Tầng: reference (bảng gold) · Phase 2
> Phụ thuộc: `reference.security_master`, silver `index_constituent_snapshots`, `index_constituent_changes`

## Mục đích

Cho biết **mã nào thuộc S&P 500 tại từng ngày** trong quá khứ. Đây là thứ giúp universe research không bị survivorship bias: backtest tại ngày `t` chỉ chọn trong các mã thuộc index tại `t`, kể cả mã sau này bị loại hoặc phá sản.

## Đầu vào

- `index_constituent_snapshots` (GitHub fja05680, từ 1996, cập nhật thủ công bởi tác giả).
- `index_constituent_changes` và snapshot hiện tại (Wikipedia).

Xem [07-ingestion/02-sources/06-sp500_constituents.md](../07-ingestion/02-sources/06-sp500_constituents.md).

## Bảng

Ghi bảng `index_membership`: một dòng cho mỗi khoảng thời gian một ticker thuộc index (`valid_from` đến `valid_to`, cả hai đầu đều tính). Cột và kiểu ở [04-data-model.md](../../04-data-model.md) mục 5.

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
