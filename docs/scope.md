# T0. Phạm vi và ràng buộc của tầng data (giai đoạn Prototype)

> Trạng thái: **DRAFT, đã điền phương án tạm**, chờ leader xác nhận.
> Nguyên tắc của giai đoạn này: **0 USD, quy mô vừa phải, có cả daily và minute, ra prototype chạy được trước**. Khi pipeline đã chứng minh được giá trị thì mới mở rộng hoặc mua data.
> Mọi mục ghi "Quyết định (tạm)" là do team tự chọn. Leader chỉ cần gạch và sửa mục nào không đồng ý.

---

## 1. Tóm tắt quyết định

| Hạng mục | Quyết định (tạm) |
|---|---|
| Ngân sách | **0 USD**, chỉ dùng nguồn miễn phí |
| Horizon alpha | Daily đến vài tuần là chính, **cộng intraday (phút → giờ)** trên universe nhỏ |
| Thị trường | US equity + ETF + global index, macro, commodity, FX, lãi suất, crypto |
| Universe US daily | S&P 500 (thành phần hiện tại + các mã từng thuộc, nếu tải được giá) + ~30 ETF |
| Universe minute | **Pilot:** ~20 cổ phiếu US thanh khoản cao + 30 ETF, top 10 cặp crypto, từ 2024. FX/CFD chờ kiểm tra điều khoản Dukascopy |
| Nguồn minute | US: **Alpaca** (free, full SIP). Crypto: **Binance**. FX/commodity/index: **Dukascopy** (cần kiểm tra điều khoản) |
| Lịch sử daily | Từ 2005 |
| Dung lượng | Pilot: **~11 GB** trên 16 GB trống (đã tính theo số đo thực tế). Bản đầy đủ: ~70 GB, cần mở rộng disk |
| Server bee | 4 core, 8.5 GB RAM khả dụng, 16 GB disk trống. **Cần xin thêm ~100 GB disk** để mở rộng minute |
| Thời gian | ~4 tuần cho prototype |

---

## 2. Server bee

### 2.1 Yêu cầu tối thiểu cho prototype

| Mục | Tối thiểu | Thoải mái |
|---|---|---|
| CPU | 4 core | 8 core |
| RAM | 16 GB (Airflow + Postgres + DuckDB) | 32 GB |
| Disk trống | 100 GB | 200 GB SSD |
| Mạng | Ra Internet được | |
| Phần mềm | Docker + Docker Compose, có sudo | |

### 2.2 Thông tin thực tế

Lệnh dùng để đo:

```bash
lscpu | grep -E 'Model name|^CPU\(s\)'   # CPU
free -h                                  # RAM
df -h                                    # Disk, chú ý phân vùng sẽ chứa /data
lsblk -d -o NAME,ROTA,SIZE               # ROTA=0 là SSD, 1 là HDD
cat /etc/os-release | head -2            # Hệ điều hành
docker --version; docker compose version # Đã có Docker chưa
```

| Mục | Kết quả (đo ngày 2026-10-13) | So với tối thiểu |
|---|---|---|
| Loại máy | Máy ảo KVM | Dễ nâng cấp disk và RAM |
| CPU | AMD EPYC-Milan, 4 core | ✅ Đủ |
| RAM | 15 GB tổng, **8.5 GB khả dụng** (6.7 GB đang bị service khác dùng) | ⚠️ Vừa đủ, phải cấu hình nhẹ |
| Disk | 48 GB tổng, 33 GB đã dùng, **16 GB trống** | ⚠️ Đủ cho pilot, thiếu cho bản đầy đủ |
| GPU | Không có (chỉ VGA ảo) | ✅ Không cần cho tầng data |
| OS | Linux 6.8.0 (kernel) | ✅ |
| SSD / HDD | Chưa kiểm tra | |
| Docker / Compose | Chưa kiểm tra | |
| Có sudo | Chưa kiểm tra | |
| Ra Internet | Chưa kiểm tra | |
| Service khác đang chạy | Có (đang dùng 6.7 GB RAM và 33 GB disk), chưa rõ là gì | |

### 2.2.1 Đánh giá

**Disk là điểm nghẽn.** 16 GB trống chỉ đủ cho bản pilot (mục 2.3). Docker image cũng chiếm chỗ: Airflow khoảng 1.5–2 GB, Postgres khoảng 0.4 GB, cộng với log của Airflow nếu không dọn. Nếu disk đầy, cả server sẽ lỗi, kể cả các service khác đang chạy trên đó. Vì vậy pipeline bắt buộc phải có **disk guard**: mọi DAG kiểm tra disk trống trước khi chạy và dừng nếu còn dưới 3 GB.

**RAM đủ nếu cấu hình nhẹ.** Phân bổ dự kiến trong 8.5 GB khả dụng:

| Thành phần | RAM giới hạn |
|---|---|
| Airflow (LocalExecutor, không Celery/Redis, parallelism = 2) | ~2.5 GB |
| Postgres (metadata Airflow + catalog + daily) | ~1 GB |
| Task ETL (polars + DuckDB, đặt `memory_limit='2GB'`, xử lý minute theo từng tháng × từng mã) | ~2.5 GB |
| Dự phòng cho service khác | ~2.5 GB |

**CPU 4 core đủ** cho daily, và đủ cho backfill minute nếu chạy qua đêm.

### 2.2.2 Phương án

| Phương án | Mô tả | Đánh giá |
|---|---|---|
| **A. Xin mở rộng disk (khuyến nghị)** | Máy ảo KVM nên thêm disk ảo khá dễ. Xin thêm **100 GB** (tối thiểu 50 GB), mount vào `/data` | Làm được bản minute đầy đủ (mục 2.3.2). Chỉ tốn chi phí hạ tầng, không tốn tiền mua data |
| **B. Pilot vừa 16 GB (làm ngay)** | Daily đầy đủ + minute cho universe nhỏ từ 2024. Raw minute xoá sau khi đã kiểm tra (vì tải lại được), log Airflow giữ 7 ngày | Bắt đầu được ngay, đủ để thử pipeline minute và cho agent thử alpha intraday |
| C. Lưu data ở chỗ khác | Object storage hoặc NAS của công ty, server bee chỉ chạy pipeline | Phức tạp hơn, chậm hơn khi query. Chỉ dùng nếu không mở rộng được disk |

**Quyết định (tạm):** làm ngay **B**, đồng thời xin leader **A**. Khi có disk mới thì mở rộng minute theo mục 2.3.2 và thêm fundamentals.

### 2.2.3 Việc cần làm trước khi cài gì lên server

1. Kiểm tra 33 GB đang dùng là gì, có dọn được không:
   ```bash
   sudo du -xh --max-depth=1 / 2>/dev/null | sort -h | tail -15
   docker system df        # nếu đã có Docker: xem image/volume cũ có xoá được không
   ```
2. Hỏi leader các service khác đang chạy trên server là gì, để không làm ảnh hưởng chúng.
3. Kiểm tra các mục còn trống ở bảng trên (SSD, Docker, sudo, Internet).
4. Đặt cảnh báo khi disk dùng quá 85%.

### 2.3 Ước lượng dung lượng

Số gần đúng cho Parquet nén zstd, tính trên mỗi mã mỗi năm:

| Loại minute | Số nến/năm | Dung lượng / mã / năm |
|---|---|---|
| Cổ phiếu US (cả pre-market và after-hours) | ~100–250 nghìn | ~8 MB (ước tính, chưa đo) |
| Crypto (giao dịch 24/7) | ~525 nghìn | **~28 MB (đo thực tế, BTCUSDT 2024)** |
| FX / CFD (24/5) | ~375 nghìn | ~15 MB (ước tính, chưa đo) |

Nến 5 phút, 15 phút, 1 giờ **không lưu riêng**. Chúng được tính từ nến 1 phút khi query (DuckDB đủ nhanh).

#### 2.3.1 Phương án B: pilot trên 16 GB hiện tại

| Dữ liệu | Quy mô | Dung lượng |
|---|---|---|
| US daily, index, commodity, FX, lãi suất | ~800 mã + ~130 series, từ 2005 | ~1.2 GB |
| Macro | ~500 series FRED, vintage cho ~30 series chính | ~1–2 GB |
| Crypto daily + factor | Top 100 cặp, Kenneth French | < 0.2 GB |
| **US minute** | ~20 cổ phiếu + 30 ETF, từ 2024-01-01 | ~1.1 GB |
| **Crypto minute** | Top 10 cặp USDT, từ 2024-01-01 | ~0.8 GB |
| **FX / vàng / dầu / index minute** | Chờ kiểm tra điều khoản Dukascopy | 0 |
| Raw daily (nén, giữ 30 ngày gần nhất + bản backfill đầu tiên) | | ~1 GB |
| Raw minute | Xoá sau khi kiểm tra xong, chỉ giữ manifest + checksum | ~0 |
| **Tổng data** | | **~6 GB** |
| Docker image (đo thực tế: Airflow + venv 3.5 GB, Postgres 0.6 GB) + log Airflow (giữ 7 ngày) | | ~5 GB |
| **Tổng cộng trên disk** | | **~11 GB / 16 GB trống**, còn ~5 GB dự phòng. Sau khi build image nên chạy `docker builder prune` để xoá cache build |

Fundamentals chưa làm ở phương án B.

#### 2.3.2 Bản đầy đủ: sau khi mở rộng disk +100 GB

| Dữ liệu | Quy mô | Dung lượng |
|---|---|---|
| Toàn bộ phần daily + macro ở trên | | ~4 GB |
| US minute | Toàn bộ S&P 500 + 30 ETF (~530 mã), từ 2016 | ~40 GB |
| Crypto minute | Top 50 cặp, từ 2018 | ~12 GB |
| FX / commodity / index minute | ~30 instrument, từ 2010 | ~7 GB |
| Fundamentals (SEC EDGAR) | S&P 500, 10 năm | 2–5 GB |
| Raw + bản tạm khi xử lý | | ~10 GB |
| **Tổng** | | **~70–80 GB**, vừa với disk +100 GB |

---

## 3. Mục tiêu research

### 3.1 Horizon
**Quyết định (tạm):**
- **Daily đến vài tuần** (giữ lệnh 1–20 ngày) trên universe rộng: S&P 500, index, commodity, FX, crypto.
- **Intraday** (phút đến giờ) trên universe pilot ở mục 2.3.1.

### 3.2 Thị trường, thứ tự làm và nguồn miễn phí

| Ưu tiên | Thị trường | Nguồn chính | Nguồn đối chiếu | Granularity |
|---|---|---|---|---|
| 1 | US equity + ETF | Stooq | Yahoo (yfinance) | Daily |
| 1 | Global index | Stooq | Yahoo, FRED (S&P 500) | Daily |
| 2 | Macro Mỹ | FRED + ALFRED (vintage) | BLS, BEA | D/W/M/Q |
| 2 | Lãi suất Mỹ | FRED (DGS*, yield curve) | US Treasury fiscaldata | Daily |
| 2 | **US equity + ETF minute** | **Alpaca** (free plan, feed SIP) | Polygon/Massive free (2 năm, vài mã) | **Minute** |
| 3 | **Crypto** | **Binance public data** (data.binance.vision) | CoinGecko (daily) | Daily + **minute** |
| 3 | Commodity | FRED (dầu WTI/Brent, vàng...), EIA | Stooq continuous futures | Daily |
| 3 | FX | FRED (DEX*), ECB reference rates | Stooq | Daily |
| 4 | **FX, vàng, dầu, index minute** | **Dukascopy** (cần kiểm tra điều khoản trước) | yfinance (chỉ ~7–30 ngày gần nhất) | **Minute** |
| 4 | Macro toàn cầu | World Bank, IMF, OECD | | M/Q/A |
| 5 | Factor benchmark | Kenneth French Data Library | | Daily |
| Sau khi có disk | Fundamentals Mỹ | SEC EDGAR (XBRL, companyfacts) | | Theo quý |
| Sau prototype | Thị trường Việt Nam | vnstock | | Daily |

Tất cả nguồn trên miễn phí. Cần đăng ký API key miễn phí: FRED, EIA, BLS, BEA, **Alpaca** (tạo tài khoản, không cần nạp tiền), Polygon/Massive.

**Thông tin đã kiểm tra về các nguồn minute (2026-10-13):**
- **Alpaca:** bản Basic miễn phí chỉ có real-time từ sàn IEX, nhưng **dữ liệu lịch sử full-market (SIP) dùng được nếu cũ hơn 15 phút**, và giống với dữ liệu của gói trả phí. Lịch sử bắt đầu từ khoảng 2016, nhưng con số này lấy từ nguồn bên thứ ba, chưa thấy trong tài liệu chính thức của Alpaca. Nguồn: [Alpaca forum](https://forum.alpaca.markets/t/cant-read-spy-minutes/14293), [Alpaca docs](https://docs.alpaca.markets/docs/about-market-data-api).
- **Polygon (nay đổi tên thành Massive):** bản miễn phí có 2 năm lịch sử, giới hạn 5 request/phút. Chỉ dùng để đối chiếu cho vài mã. Nguồn: [apicostcalc](https://apicostcalc.com/polygon.html).
- **Dukascopy:** có công cụ bên thứ ba để tải minute và tick miễn phí, nhưng **chưa tìm được điều khoản sử dụng chính thức**, và có báo cáo về giới hạn tải chặt. Phải đọc điều khoản trên trang Dukascopy trước khi dùng (task T1).

### 3.3 Universe

**US daily (Quyết định tạm):**
- Thành phần S&P 500 hiện tại và lịch sử thay đổi, lấy từ trang Wikipedia "List of S&P 500 companies" (có bảng thay đổi thành phần) hoặc dataset công khai trên GitHub tổng hợp thành phần lịch sử.
- Khoảng 30 ETF lớn: SPY, QQQ, IWM, DIA, các sector ETF (XLK, XLF, XLE...), TLT, IEF, GLD, SLV, USO, UUP, HYG, LQD, EEM, EFA...

**Minute pilot (Quyết định tạm):**
- **US:** 30 ETF ở trên + ~20 cổ phiếu S&P 500 có giá trị giao dịch trung bình lớn nhất (danh sách chốt cố định ở T1, ghi vào repo).
- **Crypto:** top 10 cặp USDT trên Binance theo volume (BTC, ETH, SOL, BNB, XRP...).
- **Dukascopy (nếu điều khoản cho phép):** EURUSD, USDJPY, GBPUSD, AUDUSD, USDCHF, USDCAD, XAUUSD, XAGUSD, Brent, WTI, US500, NAS100.

### 3.4 Lịch sử

| Loại | Quyết định (tạm) |
|---|---|
| US daily | Từ 2005-01-01 (có khủng hoảng 2008, 2020) |
| Index, commodity, FX, lãi suất daily | Từ 2000 hoặc dài nhất có thể |
| Macro | Dài nhất có thể |
| Crypto daily | Từ 2017 (Binance) |
| Minute (pilot) | Từ 2024-01-01 cho US và crypto |
| Minute (sau khi có disk) | US từ 2016, crypto từ 2018, FX/CFD từ 2010 |
| Fundamentals | 10 năm, sau khi có disk |

### 3.5 Lưu ý kỹ thuật riêng cho minute

| Vấn đề | Cách xử lý |
|---|---|
| Phiên giao dịch US | Lưu cả pre-market (04:00–09:30 ET) và after-hours (16:00–20:00 ET), thêm cột `session` = `pre` / `regular` / `post` để agent lọc |
| Giờ mùa hè/mùa đông (DST) | Lưu `ts_utc`. Khi xác định phiên phải đổi sang giờ New York theo đúng ngày, không cộng trừ cố định |
| Split / cổ tức | Lưu **giá gốc** (Alpaca tham số `adjustment=raw`). Giá điều chỉnh tính từ `ref.corporate_actions` khi query |
| Nến trống | Phút không có giao dịch thì nguồn không trả nến. Không tự điền, để agent quyết định khi resample |
| Volume của FX / CFD | Dukascopy chỉ có tick volume (số lần giá đổi), **không phải khối lượng thật**. Ghi rõ trong catalog |
| Rate limit | Backfill theo từng mã × từng tháng, có checkpoint để chạy lại từ chỗ dừng |
| Crypto | Binance có file zip theo ngày/tháng, tải file thay vì gọi API cho backfill |

---

## 4. Hạn chế đã biết của phương án 0 USD

Leader cần biết các điểm này. Chúng **không chặn** prototype, nhưng sẽ làm kết quả backtest kém tin cậy hơn.

| Hạn chế | Hậu quả | Cách giảm nhẹ trong prototype |
|---|---|---|
| **Survivorship bias**: nguồn miễn phí thường thiếu giá của mã đã hủy niêm yết. Universe minute lại chọn theo thanh khoản **hiện tại** | Backtest US equity đẹp hơn thực tế | Ghi lại danh sách mã từng thuộc S&P 500 nhưng không tải được giá. Mọi kết quả backtest phải đính kèm cảnh báo này |
| **Lịch sử minute ngắn ở pilot** (từ 2024, gần 3 năm) | Ít chế độ thị trường để kiểm chứng (không có 2008, 2020) | Mở rộng về 2016 khi có disk |
| **Alpaca free**: chưa xác nhận chính thức lịch sử bắt đầu từ năm nào, có rate limit | Có thể thiếu một phần lịch sử, backfill chậm | Kiểm tra ở T1 bằng cách tải thử SPY từ 2016 |
| **Dukascopy**: điều khoản chưa rõ, giá là báo giá của Dukascopy chứ không phải giá sàn, volume là tick volume | Có thể không được phép dùng. Giá index/commodity CFD lệch nhẹ so với futures thật | Nếu điều khoản không cho phép thì bỏ, chỉ giữ FX/commodity daily |
| **Yahoo (yfinance) không chính thức** | Có thể hỏng bất cứ lúc nào, không được redistribution | Chỉ dùng để đối chiếu, Stooq là nguồn chính |
| **Chất lượng Stooq không được đảm bảo** | Có thể có lỗi giá, thiếu ngày | Đối chiếu chéo với Yahoo, và với daily tổng hợp từ minute của Alpaca |
| **Không có futures minute thật** | Commodity intraday chỉ có qua CFD của Dukascopy | Chấp nhận ở prototype |
| **Rate limit của API miễn phí** | Backfill chậm | Backfill một lần, chạy qua đêm, có checkpoint |

**Điều kiện để chuyển sang nguồn trả phí sau prototype:** khi agent đã tìm được alpha và cần kiểm chứng lại trên data survivorship-free hoặc futures minute thật (gợi ý: Norgate hoặc Sharadar cho daily, Databento cho minute).

---

## 5. Pháp lý

| Câu hỏi | Quyết định (tạm) |
|---|---|
| Dùng nguồn không chính thức (yfinance) | Có, nhưng chỉ để đối chiếu, nội bộ |
| Dukascopy | Chỉ dùng sau khi đọc điều khoản và xác nhận được phép tải để nghiên cứu nội bộ |
| Alpaca, Polygon/Massive | Đọc điều khoản market data của tài khoản free ở T1, đặc biệt mục lưu trữ dữ liệu |
| Chia sẻ data ra ngoài công ty | **Không**. Data chỉ dùng nội bộ cho nghiên cứu |
| Repo | Private. Không commit API key (dùng file `.env`, có trong `.gitignore`) và không commit data |
| Crawl web | Tôn trọng rate limit, ưu tiên API và file tải chính thức thay vì scrape HTML |

---

## 6. Vận hành

| Câu hỏi | Quyết định (tạm) |
|---|---|
| Lịch cập nhật US daily + minute | 06:30 giờ VN (sau khi after-hours ở Mỹ kết thúc lúc 20:00 ET) |
| Lịch cập nhật crypto daily + minute | 08:00 giờ VN (sau khi Binance đăng file của ngày UTC trước) |
| Lịch cập nhật FX/CFD minute | 08:00 giờ VN, từ thứ Ba đến thứ Bảy |
| Lịch cập nhật macro | 1 lần/ngày, chỉ tải series có thay đổi |
| Disk guard | Mọi DAG kiểm tra disk trước khi chạy, dừng và báo lỗi nếu còn dưới 3 GB |
| Cảnh báo khi DAG lỗi | Telegram bot (miễn phí). Đổi sang Slack nếu team dùng Slack |
| Quyền ghi DB | Chỉ tài khoản pipeline |
| Quyền đọc | Team research + agent, chỉ đọc |
| Repo | Private trên GitHub của công ty (cần leader cho biết org), add leader làm reviewer |

---

## 7. Agent dùng data thế nào

| Câu hỏi | Quyết định (tạm) |
|---|---|
| Agent chạy ở đâu | Cùng server bee |
| Cách đọc | Thư viện Python nội bộ: `get_bars(symbols, start, end, freq, as_of)`, `get_macro(...)`. `freq` hỗ trợ `1min`, `5min`, `1h`, `1d`. `as_of` bắt buộc |
| SQL tự do | Cho phép, nhưng chỉ trên các view PIT ở tầng gold, bằng tài khoản read-only |

---

## 8. Ngoài phạm vi prototype

- Tick data và order book
- Futures minute thật (chỉ có CFD qua Dukascopy)
- Tin tức, mạng xã hội, sentiment
- Options
- Real-time streaming (chỉ làm batch theo lịch)
- Nguồn trả phí
- Thị trường Việt Nam (làm ngay sau prototype nếu leader muốn)
- Backtest engine và execution

---

## 9. Câu hỏi còn lại cho leader

Chỉ còn những câu team không tự quyết được:

1. **Có thể mở rộng disk server bee thêm ~100 GB (tối thiểu 50 GB) không?** Pilot minute (đã thu nhỏ theo số đo thực tế) vừa với 16 GB hiện tại nhưng chỉ còn ~5 GB dự phòng. Muốn mở rộng minute cho toàn bộ S&P 500 và lịch sử từ 2016 thì cần thêm disk.
2. Server bee đang chạy những service nào khác (đang dùng 6.7 GB RAM, 33 GB disk)? Có được dọn bớt không?
3. Đồng ý phương án 0 USD với các hạn chế ở mục 4 không?
4. Universe minute pilot ở mục 3.3 có ổn không, hay muốn ưu tiên nhóm khác (ví dụ chỉ crypto, hoặc chỉ US)?
5. Có muốn đưa thị trường Việt Nam vào prototype không?
6. Repo đặt ở GitHub org nào?
7. Team dùng Telegram hay Slack để nhận cảnh báo?

---

## 10. Xác nhận

| | Tên | Ngày |
|---|---|---|
| Người soạn | | 2026-10-13 |
| Leader duyệt | | |
