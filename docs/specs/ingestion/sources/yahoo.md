# Spec nguồn: Yahoo Finance

> Code: `src/alphadata/ingestion/sources/yahoo/` · Ưu tiên: **P1** · Trạng thái: **Chốt dùng (nguồn không chính thức)**
> Kiểm tra 2026-10-15: ✅ `query1.finance.yahoo.com/v8/finance/chart/SPY` trả JSON cho cả `interval=1d` và `interval=1m`.

## Vai trò

Nguồn **daily chính** cho US equity, ETF và index toàn cầu trước 2016 (Alpaca chỉ có từ 2016), và nguồn split/cổ tức. Từ khi Stooq bị loại, Yahoo là nguồn daily dài hạn miễn phí duy nhất; Alpaca daily dùng để đối chiếu.

## Truy cập

| Mục | Giá trị |
|---|---|
| Endpoint | `https://query1.finance.yahoo.com/v8/finance/chart/<SYMBOL>` |
| Thư viện | Gọi trực tiếp chart API bằng `core.http` (không phụ thuộc yfinance để kiểm soát raw). yfinance chỉ dùng tham khảo cách parse |
| Key | Không cần. Cần User-Agent trình duyệt |
| Rate limit | Không công bố. Tự giới hạn 1 request/giây |
| Điều khoản | **Không chính thức**, Yahoo không cho phép phân phối lại. Chỉ dùng nội bộ cho nghiên cứu |

Tham số: `period1=<epoch>`, `period2=<epoch>`, `interval=1d`, `events=div,split`, `includeAdjustedClose=true`.

## Datasets

| Dataset | Bảng silver | Partition | keep_raw |
|---|---|---|---|
| `yahoo_daily` | `equity_bars_1d` | symbol | True |
| `yahoo_corporate_actions` | `equity_corporate_actions` | symbol | True |

Hai dataset dùng cùng một request: `yahoo_corporate_actions` đọc lại raw mới nhất của `yahoo_daily` nếu cùng `fetch_date`, không gọi lại API.

## Normalize

JSON: `chart.result[0]` có `timestamp[]`, `indicators.quote[0].{open,high,low,close,volume}`, `indicators.adjclose[0].adjclose`, `events.dividends`, `events.splits`, `meta.exchangeTimezoneName`.
- `date` = ngày của `timestamp` theo `exchangeTimezoneName` (không theo UTC).
- `close` của Yahoo **đã điều chỉnh split nhưng chưa điều chỉnh cổ tức**. `adj_close` điều chỉnh cả hai. Ghi rõ điều này trong catalog.
- Dòng có giá null (ngày nghỉ Yahoo vẫn trả) thì bỏ, ghi số dòng bỏ vào detail của check.
- Splits: `ratio = numerator / denominator`. Dividends: `amount`, `currency = meta.currency`.

## Hành vi đặc biệt

- Mỗi lần chạy **tải lại toàn bộ lịch sử** của mã (một request), vì Yahoo tính lại `close` khi có split mới. Merge giữ bản mới.
- Cấu trúc JSON khác mong đợi → `SourceFormatChanged`, alert ngay. Yahoo đổi API khá thường xuyên.
- Mã đã hủy niêm yết thường trả lỗi "No data found": ghi vào danh sách thiếu giá (phục vụ cảnh báo survivorship), không fail cả DAG.

## Checks riêng

| Check | Mức |
|---|---|
| `ohlc_consistent`, `non_negative` | warn |
| `price_jump` > 50% | warn |
| `cross_source_close` với Alpaca (sau khi ghi) | warn |

## Lịch

23:30 UTC thứ Hai–thứ Sáu (06:30 giờ VN).

## Done when

- Toàn bộ universe daily (ETF, index, mọi mã từng thuộc S&P 500 từ 2005) có dữ liệu hoặc nằm trong danh sách thiếu giá.
- Connector có test với JSON mẫu thật và với JSON thiếu trường.
