# Danh sách nguồn dữ liệu

> Cập nhật: 2026-10-14. Giá, giới hạn và điều khoản thay đổi thường xuyên: kiểm tra lại trước khi đưa nguồn vào pipeline.
>
> **Trạng thái:**
> ✅ **Đã chạy**: có connector trong `src/alphadata/sources/`, đã chạy thử với dữ liệu thật.
> 🔜 **Tiếp theo**: làm sau khi pipeline pilot ổn định.
> 📋 **Dự trữ**: có thể dùng sau, chưa lên kế hoạch.
> 💰 **Trả phí**: chỉ xét khi có ngân sách.

## Đang dùng trong pipeline

| Nguồn | Dataset | Bảng silver | Granularity | Truy cập | Ghi chú đã kiểm chứng |
|---|---|---|---|---|---|
| ✅ **Binance public data** | `binance_spot_klines_1m` | `crypto_bars_1m` | 1 phút | Tải file zip, không cần key | File ngày được đăng sau 00:00 UTC hôm sau, có độ trễ vài giờ. Có checksum SHA-256. **Từ 2025-01-01 timestamp là micro-giây**, trước đó là mili-giây. Đo thực tế: ~28 MB / cặp / năm (Parquet) |
| ✅ **Yahoo Finance** (yfinance) | `yahoo_daily` | `equity_bars_1d` | Daily | Thư viện, không cần key | Không chính thức. `close` đã điều chỉnh split nhưng chưa điều chỉnh cổ tức, `adj_close` điều chỉnh cả hai. Lịch sử bị tính lại khi có cổ tức/split |
| ✅ **FRED / ALFRED** | `fred_series` | `macro_observations` | Daily → năm | API, **cần key miễn phí** | Dùng `realtime_start/realtime_end` để lấy toàn bộ vintage, cho phép query point-in-time. Endpoint CSV công khai (`fredgraph.csv`) chặn client lập trình, nên không dùng |

## Theo nhóm thị trường

### Cổ phiếu và ETF Mỹ

| Nguồn | Granularity | Truy cập / chi phí | Trạng thái | Ghi chú |
|---|---|---|---|---|
| Alpaca Market Data | 1 phút, daily | Key miễn phí | 🔜 | Bản free có lịch sử full-market (SIP) nếu cũ hơn 15 phút. Lịch sử từ ~2016 (chưa xác nhận chính thức). **Nguồn US minute chính** |
| Stooq | Daily | CSV, miễn phí | 🔜 | Đối chiếu chéo với Yahoo |
| Polygon.io (nay là Massive) | 1 phút, daily | Free: 2 năm, 5 request/phút | 📋 | Đủ để đối chiếu vài mã |
| SEC EDGAR (XBRL companyfacts) | Theo quý | API miễn phí, cần khai User-Agent có email | 📋 | Fundamentals, `filing_date` dùng làm thời điểm công bố |
| Tiingo | Daily | Free có giới hạn | 📋 | |
| Norgate Data | Daily | 💰 vài trăm USD/năm | 📋 | Có mã đã hủy niêm yết, có lịch sử thành phần index |
| Sharadar (Nasdaq Data Link) | Daily, fundamentals | 💰 | 📋 | Point-in-time, có mã đã hủy niêm yết |
| Databento | Tick, 1 phút | 💰 pay-as-you-go | 📋 | Chất lượng cao |
| FirstRate Data, Kibot | 1 phút | 💰 mua một lần | 📋 | |

### Chỉ số toàn cầu

| Nguồn | Granularity | Truy cập / chi phí | Trạng thái | Ghi chú |
|---|---|---|---|---|
| Yahoo (^GSPC, ^N225, ^GDAXI, ^VNINDEX...) | Daily | Miễn phí | ✅ (^GSPC, ^VIX) | Thêm mã vào `universe.py` |
| Stooq | Daily | Miễn phí | 🔜 | |
| Dukascopy | 1 phút, tick (CFD) | Miễn phí, giới hạn tải | 📋 | **Chưa tìm được điều khoản chính thức**, đọc kỹ trước khi dùng |

### Commodity

| Nguồn | Granularity | Truy cập / chi phí | Trạng thái | Ghi chú |
|---|---|---|---|---|
| FRED (DCOILWTICO, DCOILBRENTEU, gold...) | Daily | Key miễn phí | ✅ (WTI) | Thêm series vào `universe.py` |
| EIA | Daily, tuần | Key miễn phí | 🔜 | Dầu, khí, tồn kho |
| CME daily settlements | Daily | Miễn phí | 📋 | |
| Nasdaq Data Link (dataset free) | Daily | Key miễn phí | 📋 | |
| Dukascopy (XAUUSD, Brent, WTI CFD) | 1 phút | Miễn phí | 📋 | Như trên |

### Crypto

| Nguồn | Granularity | Truy cập / chi phí | Trạng thái | Ghi chú |
|---|---|---|---|---|
| Binance public data | 1 phút, daily, trades | Miễn phí | ✅ | Có thêm futures (`/data/futures/um/`), funding rate |
| CoinGecko | Daily | Free có giới hạn | 📋 | Market cap |
| Kaiko | Tick | 💰 | 📋 | |

### FX và lãi suất

| Nguồn | Granularity | Truy cập / chi phí | Trạng thái | Ghi chú |
|---|---|---|---|---|
| FRED (DGS*, T10Y2Y, DEX*, DTWEXBGS) | Daily | Key miễn phí | ✅ (một phần) | |
| ECB Data Portal | Daily | Miễn phí | 📋 | Tỷ giá tham chiếu EUR |
| US Treasury fiscaldata | Daily | Miễn phí | 📋 | Yield curve chính thức |
| Dukascopy | 1 phút, tick | Miễn phí | 📋 | Volume là tick volume, không phải khối lượng thật |

### Macro

| Nguồn | Dữ liệu | Truy cập / chi phí | Trạng thái | Ghi chú |
|---|---|---|---|---|
| FRED / ALFRED | Hàng trăm nghìn series Mỹ và quốc tế | Key miễn phí | ✅ | Nguồn trung tâm |
| BLS | Việc làm, CPI chi tiết | Key miễn phí | 📋 | |
| BEA | GDP, thu nhập | Key miễn phí | 📋 | |
| World Bank, IMF, OECD, BIS, Eurostat | Macro toàn cầu | Miễn phí | 📋 | |
| Kenneth French Data Library | Factor Fama-French, momentum | Miễn phí | 🔜 | Benchmark cho alpha |
| TCTK (GSO), NHNN (SBV) | Macro Việt Nam | Web, thường phải scrape | 📋 | |

### Việt Nam

| Nguồn | Granularity | Truy cập / chi phí | Trạng thái | Ghi chú |
|---|---|---|---|---|
| vnstock | Daily, intraday | Thư viện miễn phí | 📋 | Kiểm tra điều khoản của nguồn gốc |
| FiinTrade, Fireant | Daily, fundamentals | 💰 | 📋 | |

## Thêm một nguồn mới

1. Viết class con của `Dataset` trong `src/alphadata/sources/<nguồn>.py` (xem `binance.py` làm mẫu), gồm `fetch`, `normalize`, `silver_path`, và nếu cần thì `checks` và `merge`.
2. Đăng ký trong `src/alphadata/sources/__init__.py`.
3. Thêm test với dữ liệu giả trong `tests/`.
4. Chạy thử: `uv run alphadata run <dataset> key=value`.
5. Thêm task vào `dags/alphadata_dags.py`.
6. Cập nhật bảng ở đầu file này.
