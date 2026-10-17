# Spec nguồn: Alpaca Market Data

> Code: `src/alphadata/ingestion/sources/alpaca/` · Ưu tiên: **P1** · Trạng thái: **Chốt dùng, chờ tạo key để tải thử**
> Kiểm tra 2026-10-15: ⚠️ `data.alpaca.markets/v2/stocks/bars` trả 401 khi không có key (đúng như mong đợi). Thông tin gói miễn phí lấy từ [tài liệu chính thức](https://docs.alpaca.markets/docs/about-market-data-api) và [forum Alpaca](https://forum.alpaca.markets/t/cant-read-spy-minutes/14293).

## Vai trò

**Nguồn US minute chính**, và nguồn daily thứ hai để đối chiếu với Yahoo (từ 2016).

## Truy cập

| Mục | Giá trị |
|---|---|
| Base URL | `https://data.alpaca.markets` |
| Key | Miễn phí: tạo tài khoản Alpaca (không cần nạp tiền). Header `APCA-API-KEY-ID`, `APCA-API-SECRET-KEY` |
| Gói | Basic (miễn phí): lịch sử từ 2016, dữ liệu full-market (SIP) **nếu cũ hơn 15 phút**, giống hệt dữ liệu gói trả phí |
| Rate limit | 200 request/phút |
| Điều khoản | Cần đọc mục lưu trữ dữ liệu trong Market Data Agreement khi tạo tài khoản. Chỉ dùng nội bộ, không phân phối lại |

## Datasets

| Dataset | Bảng silver | Partition | Endpoint | keep_raw |
|---|---|---|---|---|
| `alpaca_bars_1m` | `equity_bars_1m` | symbol, month (backfill) / symbol, date (hằng ngày) | `GET /v2/stocks/bars?timeframe=1Min` | False |
| `alpaca_bars_1d` | `equity_bars_1d` | symbol | `GET /v2/stocks/bars?timeframe=1Day` | True |
| `alpaca_corporate_actions` | `equity_corporate_actions` | symbol | Endpoint corporate actions của Market Data API (xác nhận đường dẫn và tham số khi có key) | True |

Tham số chung cho bars:

| Tham số | Giá trị | Lý do |
|---|---|---|
| `feed` | `sip` | Luôn ghi rõ, không dựa vào mặc định |
| `adjustment` | `raw` | Lưu giá gốc, điều chỉnh ở gold |
| `asof` | `-` | Không ánh xạ đổi tên mã. Đổi tên xử lý ở `reference.security_master` |
| `limit` | `10000` | Tối đa mỗi trang |
| `start`, `end` | RFC-3339 UTC | `end` phải cũ hơn hiện tại ít nhất 15 phút, nếu không API báo lỗi subscription |

Phân trang bằng `next_page_token`. Mỗi trang lưu thành một file raw JSON riêng (`page-0001.json`, ...).

## Normalize

Mỗi bar: `t` (RFC-3339, nano-giây), `o`, `h`, `l`, `c`, `v`, `n`, `vw`.
- `ts_utc = t`, `trades = n`, `vwap = vw`, `feed = "sip"`.
- `session`: đổi `ts_utc` sang giờ `America/New_York` **theo đúng ngày** (có DST), rồi gán `pre` (04:00–09:30), `regular` (09:30–16:00), `post` (16:00–20:00). Ngày đóng cửa sớm lấy giờ đóng từ `reference.trading_calendar` (khi Phase 2 xong; trước đó dùng 16:00 và ghi chú trong catalog).
- Daily: `date` = ngày giao dịch theo giờ New York của `t`, `adj_close` = null.

## Checks riêng

| Check | Mức |
|---|---|
| `ts_in_range` | error |
| `ohlc_consistent`, `non_negative` | warn |
| `missing_bars_vs_calendar` (sau khi ghi) | warn |

Phút không có giao dịch thì Alpaca không trả nến. **Không tự điền**, để agent quyết định khi resample.

## Lịch

23:30 UTC thứ Hai–thứ Sáu (06:30 giờ VN), sau khi after-hours kết thúc lúc 20:00 ET. Tải ngày giao dịch vừa xong.

## Ước lượng backfill

Mỗi trang 10.000 nến. Một mã một năm có ~100–250 nghìn nến minute → 10–25 request. 50 mã × 3 năm ≈ 3.750 request ≈ 20 phút ở 200 request/phút. Bản đầy đủ (530 mã từ 2016) ≈ 100 nghìn request ≈ 9 giờ, chạy qua đêm.

## Việc cần làm trước khi viết connector

1. Tạo tài khoản và key, ghi vào `.env` trên server.
2. Tải thử SPY 1 phút ngày 2016-01-04 để xác nhận lịch sử thật sự bắt đầu từ 2016.
3. Đọc điều khoản lưu trữ dữ liệu của tài khoản free.
4. Xác nhận đường dẫn endpoint corporate actions và độ dài lịch sử của nó.

## Done when

- Backfill pilot (50 mã từ 2024-01) xong, số nến regular session mỗi ngày khớp lịch sàn (trừ mã ít thanh khoản).
- Close daily của Alpaca và Yahoo cho cùng mã chênh < 0.5% (sau khi quy về cùng kiểu điều chỉnh).
