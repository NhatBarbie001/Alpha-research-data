# Spec: `gold.bars`

> Code: `src/alphadata/gold/bars/` · Tầng: gold · Phase 3
> Phụ thuộc: `reference.*`, silver `equity_bars_1d`, `equity_bars_1m`, `crypto_bars_1m`

## Mục đích

Bars sẵn dùng cho research: theo `security_id`, đã chọn nguồn chính, có giá điều chỉnh tính từ `reference.corporate_actions`.

## Bảng

| Bảng | Một dòng là | Nội dung |
|---|---|---|
| `bars_1d` | Một chứng khoán một ngày | Giá gốc, `split_factor`, `adj_factor`, nguồn, cờ DQ. Equity/ETF từ `equity_bars_1d`; crypto gộp từ `crypto_bars_1m` theo ngày UTC |
| `bars_1m` | Một chứng khoán một phút | Giá gốc, `split_factor` theo ngày (lấy từ `bars_1d`), không có cổ tức (không cần cho intraday). Crypto: `session = regular`, `split_factor = 1` |

Cột, kiểu, partition, thứ tự sắp xếp ở [04-data-model.md](../../04-data-model.md) mục 6.

Giá điều chỉnh = giá gốc × `adj_factor`. Không lưu sẵn cột giá điều chỉnh để tránh hai nguồn sự thật.

## Chọn nguồn cho daily

| Khoảng thời gian | Nguồn chính | Lý do |
|---|---|---|
| Từ 2016 | Alpaca (`raw`) | Giá gốc thật, nguồn chính thức hơn |
| Trước 2016 | Yahoo | Alpaca không có |
| Crypto (mọi thời điểm) | Binance, gộp từ nến 1 phút theo ngày UTC | Một nguồn duy nhất |

Yahoo `close` đã điều chỉnh split (xem [07-ingestion/02-sources/03-yahoo.md](../07-ingestion/02-sources/03-yahoo.md)). Để ghép với Alpaca, phải **bỏ điều chỉnh split** cho phần Yahoo bằng `reference.corporate_actions` trước khi ghép. Tại điểm nối (đầu 2016), close của hai nguồn phải lệch < 0.5%, nếu không ghi `cross_source_diff`.

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
