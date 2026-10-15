# Spec nguồn: Binance public data

> Code: `src/alphadata/ingestion/sources/binance/` · Ưu tiên: **P1** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ tải được `BTCUSDT-1m-2026-10-10.zip` (63 KB), `BTCUSDT-fundingRate-2026-08.zip`, `BTCUSDT-metrics-2026-10-10.zip`. REST `api.binance.com/api/v3/klines` cũng trả dữ liệu.

## Truy cập

| Mục | Giá trị |
|---|---|
| Base URL | `https://data.binance.vision/data/` |
| Key | Không cần |
| Định dạng | File zip chứa một CSV, kèm file `<tên>.zip.CHECKSUM` (SHA-256) |
| Rate limit | Không công bố cho data.binance.vision. Tự giới hạn 5 request/giây |
| Điều khoản | Dữ liệu công khai do Binance phát hành để tải về. Chỉ dùng nội bộ |
| Độ trễ | File ngày được đăng sau 00:00 UTC hôm sau, thường trễ vài giờ. File tháng đăng đầu tháng sau |

REST API (`api.binance.com`) chỉ dùng để vá lỗ hổng khi file zip thiếu, không dùng cho backfill.

## Datasets

| Dataset | Bảng silver | Partition | File nguồn | keep_raw |
|---|---|---|---|---|
| `binance_spot_klines_1m` | `crypto_bars_1m` | symbol, date (hằng ngày) hoặc symbol, month (backfill) | `spot/daily/klines/<SYM>/1m/<SYM>-1m-<YYYY-MM-DD>.zip`, `spot/monthly/klines/<SYM>/1m/<SYM>-1m-<YYYY-MM>.zip` | False |
| `binance_funding_rate` | `crypto_funding_rates` | symbol, month | `futures/um/monthly/fundingRate/<SYM>/<SYM>-fundingRate-<YYYY-MM>.zip` | True |
| `binance_futures_metrics` | `crypto_futures_metrics_5m` | symbol, date | `futures/um/daily/metrics/<SYM>/<SYM>-metrics-<YYYY-MM-DD>.zip` | True |

## Normalize

**Klines 1m.** CSV không có header, 12 cột: open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_base, taker_buy_quote, ignore.
- `ts_utc = open_time`. **Từ 2025-01-01, timestamp của spot là micro-giây**, trước đó là mili-giây. Dùng `core.timeutil.epoch_to_utc` tự nhận đơn vị theo độ lớn, không hard-code theo ngày.
- Bỏ `close_time`, `ignore`. `count` → `trades`.
- Một số file có dòng header, một số không: phát hiện bằng cách thử parse dòng đầu.

**Funding rate.** Cột: calc_time, funding_interval_hours, last_funding_rate → `ts_utc`, `funding_interval_hours`, `funding_rate`.

**Futures metrics** (5 phút): create_time, symbol, sum_open_interest, sum_open_interest_value, count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio, count_long_short_ratio, sum_taker_long_short_vol_ratio. `create_time` là chuỗi thời gian UTC.

Kiểm tra lại tên cột của funding và metrics khi viết connector, vì đây là ghi nhận từ tài liệu, chưa đối chiếu với file đã tải.

## Checks riêng

| Check | Mức |
|---|---|
| SHA-256 khớp file `.CHECKSUM` | error (raise trước khi lưu raw) |
| `ts_in_range` trong ngày/tháng của partition | error |
| `row_count` = 1440 × số ngày (klines) | warn |
| `ohlc_consistent`, `non_negative` | warn |

## Hành vi đặc biệt

- File ngày chưa có (404) → `NotAvailableYet`.
- Backfill dùng file tháng. Tháng hiện tại chưa có file tháng thì dùng file ngày.
- Silver `crypto_bars_1m` một file mỗi `symbol/year/month`. Run hằng ngày merge ngày mới vào file tháng.
- Cặp đã bị hủy niêm yết vẫn còn file lịch sử trên data.binance.vision, tải được bình thường.

## Lịch

01:00 UTC hằng ngày (08:00 giờ VN), tải ngày UTC hôm trước. Retry 4 lần, cách 30 phút.

## Dung lượng

Đo thực tế: ~28 MB / cặp / năm (Parquet, BTCUSDT 2024). Funding và metrics không đáng kể.

## Done when

- Backfill 10 cặp pilot từ 2024-01 xong, `row_count` warn được giải thích (Binance có vài lần bảo trì).
- Parse đúng cả file trước và sau 2025-01-01 (test với mẫu thật cắt ngắn).
