# Spec: `gold.bars`

> Code: `src/alphadata/gold/bars/` · Tầng: gold · Phase 3
> Phụ thuộc: `reference.*`, silver `equity_bars_1d`, `equity_bars_1m`, `crypto_bars_1m`

## Mục đích

Bars sẵn dùng cho research: theo `security_id`, đã chọn nguồn chính, có giá điều chỉnh tính từ `reference.corporate_actions`.

## Bảng

**`bars_1d`**: khoá (`security_id`, `date`).

| Cột | Ghi chú |
|---|---|
| security_id, date | |
| open, high, low, close, volume | **Giá gốc** (chưa điều chỉnh gì) |
| adj_factor | Hệ số nhân để ra giá điều chỉnh split + cổ tức, tính tại thời điểm dựng |
| split_factor | Hệ số chỉ cho split |
| source | Nguồn của dòng này (`alpaca` hoặc `yahoo`) |
| dq_flags | Danh sách cờ warn của dòng (ví dụ `price_jump`, `cross_source_diff`) |

Giá điều chỉnh = giá gốc × `adj_factor`. Không lưu sẵn cột giá điều chỉnh để tránh hai nguồn sự thật.

**`bars_1m`**: khoá (`security_id`, `ts_utc`). Cột: open, high, low, close, volume, trades, vwap, session, source. Có `split_factor` theo ngày (join từ `bars_1d`), không có cổ tức (không cần cho intraday).

Crypto: `bars_1m` cũng chứa crypto (từ `crypto_bars_1m`), `session = "regular"`, `split_factor = 1`.

## Chọn nguồn cho daily

| Khoảng thời gian | Nguồn chính | Lý do |
|---|---|---|
| Từ 2016 | Alpaca (`raw`) | Giá gốc thật, nguồn chính thức hơn |
| Trước 2016 | Yahoo | Alpaca không có |

Yahoo `close` đã điều chỉnh split (xem [yahoo.md](../ingestion/sources/yahoo.md)). Để ghép với Alpaca, phải **bỏ điều chỉnh split** cho phần Yahoo bằng `reference.corporate_actions` trước khi ghép. Tại điểm nối (đầu 2016), close của hai nguồn phải lệch < 0.5%, nếu không ghi `cross_source_diff`.

## Tính `adj_factor`

Theo cách CRSP: đi ngược từ ngày gần nhất, tại mỗi `ex_date`:
- Split tỷ lệ `r`: nhân hệ số các ngày trước đó với `1/r`.
- Cổ tức `d`: nhân với `(1 - d / close_ngày_trước_ex_date)`.

Chỉ dùng sự kiện `confirmed` và `single_source` (có cảnh báo). Kiểm tra chéo với `adj_close` của Yahoo bằng check `adj_vs_source`.

**Lưu ý PIT:** `adj_factor` phụ thuộc các sự kiện xảy ra **sau** ngày `t`, nên giá điều chỉnh không phải thông tin point-in-time. Dùng giá điều chỉnh để tính lợi nhuận là đúng; dùng làm mức giá (ví dụ "giá dưới 5 USD") phải dùng giá gốc. `access` ghi rõ điều này.

## Dựng lại

Job `gold_bars_rebuild` chạy sau khi các DAG ingest daily xong: dựng lại `bars_1d` cho mã có dữ liệu mới hoặc corporate action mới, `bars_1m` cho các tháng có dữ liệu mới.

## Done when

- Lợi nhuận tổng (có cổ tức) của SPY 2016–2025 tính từ `bars_1d` lệch < 0.1%/năm so với `adj_close` của Yahoo.
- Không có ngày nhảy giá tại các ngày split đã biết sau khi điều chỉnh.
