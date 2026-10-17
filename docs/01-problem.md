# 01. Bài toán

> Đọc đầu tiên. Tài liệu này trả lời: tầng data phục vụ ai, phải giải quyết vấn đề gì, vì sao khó, ràng buộc gì, và thế nào là xong.
> Tiếp theo: [02-architecture.md](02-architecture.md) (kiến trúc) → [03-how-it-works.md](03-how-it-works.md) (các module tương tác và hoạt động thế nào) → [04-data-model.md](04-data-model.md) (thiết kế database) → [specs/](specs/) (chi tiết từng module).

---

## 1. Bối cảnh

**Alpha research agent** là một agent tự động tìm **alpha**, tức tín hiệu giao dịch cho lợi nhuận vượt thị trường sau khi đã trừ rủi ro. Vòng lặp của agent:

```
 ý tưởng ──► viết tín hiệu ──► backtest trên dữ liệu lịch sử ──► đánh giá ──► ý tưởng mới
                                         ▲
                                         │  đọc dữ liệu tại từng ngày t trong quá khứ
                                         │
                                  ┌──────┴──────┐
                                  │  TẦNG DATA  │  ◄── phạm vi của repo này
                                  └─────────────┘
```

Agent thử hàng nghìn ý tưởng. Mỗi ý tưởng được backtest: giả lập rằng ta đang đứng ở từng ngày `t` trong quá khứ, chỉ biết những gì đã biết tại `t`, ra quyết định, rồi xem kết quả. Vì agent thử rất nhiều, nó sẽ **tìm ra và khuếch đại mọi lỗi trong dữ liệu**: lỗi nào làm backtest đẹp hơn thực tế thì agent sẽ khai thác lỗi đó và báo cáo nó là alpha.

Vì vậy chất lượng của agent bị giới hạn bởi chất lượng của dữ liệu, không phải bởi model.

---

## 2. Bài toán

**Xây tầng dữ liệu cung cấp cho agent dữ liệu thị trường và macro đầy đủ, sạch, truy được nguồn gốc, và đúng như những gì đã biết tại từng thời điểm trong quá khứ (point-in-time), chỉ bằng nguồn miễn phí và trên một server nhỏ.**

### 2.1 Năm lỗi dữ liệu tạo ra alpha ảo

Đây là phần khó nhất và là lý do tầng data không chỉ là "crawl rồi lưu".

| Lỗi | Chuyện gì xảy ra | Ví dụ | Hậu quả với agent |
|---|---|---|---|
| **Look-ahead bias** | Dùng thông tin chưa được công bố tại thời điểm đó | GDP quý 3 có `obs_date` 2026-07-01 nhưng cuối tháng 10 mới công bố. Nếu backtest dùng nó từ ngày 1/10 là dùng tương lai | Agent "dự đoán" được thị trường bằng số liệu nó chưa thể biết. Backtest rất đẹp, giao dịch thật thua |
| **Số liệu bị sửa (revision)** | Số macro được công bố rồi sửa nhiều lần | GDP công bố lần đầu 2.1%, sau sửa còn 1.8%. Nguồn thông thường chỉ giữ 1.8% | Backtest dùng số đã sửa, là số mà lúc đó chưa ai biết. Đây là một dạng look-ahead khó thấy |
| **Survivorship bias** | Chỉ có dữ liệu của công ty còn tồn tại đến hôm nay | Danh sách S&P 500 hôm nay không có Lehman Brothers. Backtest 2008 trên danh sách hôm nay đã loại sẵn những công ty phá sản | Chiến lược "mua cổ phiếu giảm mạnh" trông rất lời, vì những mã giảm đến 0 đã bị loại khỏi dữ liệu |
| **Corporate action chưa điều chỉnh** | Giá nhảy khi chia tách cổ phiếu (split) hoặc trả cổ tức | NVDA split 10:1 ngày 2024-06-10: giá giảm ~90% sau một đêm, nhưng giá trị nắm giữ không đổi | Agent tưởng là crash và học sai. Ngược lại, điều chỉnh sai cũng tạo lợi nhuận ảo |
| **Lệch timezone, lệch phiên** | Thời gian ghi theo giờ địa phương hoặc gộp nhầm phiên | Nến 9:30 giờ New York bị ghi là 9:30 UTC (lệch 4–5 giờ). Giờ mùa hè đổi hai lần mỗi năm | Tín hiệu intraday dùng giá của tương lai hoặc của phiên khác |

### 2.2 Các khó khăn thực tế khác

| Khó khăn | Chi tiết |
|---|---|
| **Nhiều nguồn, mỗi nguồn một kiểu** | 17 nguồn: file zip theo ngày (Binance), REST có phân trang (Alpaca), JSON không chính thức (Yahoo), SDMX (ECB, IMF, OECD), CSV dạng rộng (Treasury)... Mỗi nguồn có quy ước thời gian, đơn vị, cách sửa số liệu riêng |
| **Nguồn miễn phí không ổn định** | Có thể đổi format, chặn bot, hoặc ngừng hoạt động bất cứ lúc nào. Khi kiểm tra ngày 2026-10-15, Stooq đã chặn bot, CME chặn IP, Nasdaq Data Link trả 403 |
| **Nguồn miễn phí thiếu dữ liệu** | Thường không có mã đã hủy niêm yết, không có lịch sử thành phần index, không có vintage của số liệu. Phải tự dựng lại từ nhiều nguồn |
| **Dữ liệu lớn so với server** | Minute US đầy đủ là hàng tỷ dòng. Server chỉ còn 16 GB disk trống, và đang chạy chung với service khác |
| **Phải truy được nguồn gốc** | Khi agent báo một alpha, team phải trả lời được "số này lấy từ đâu, lúc nào, đã qua những bước nào" trong vài phút |

---

## 3. Yêu cầu

### 3.1 Chức năng: dữ liệu cần có

| Nhóm | Granularity | Universe | Lịch sử |
|---|---|---|---|
| US equity + ETF | Daily | S&P 500 (cả mã từng thuộc) + ~30 ETF | Từ 2005 |
| US equity + ETF | 1 phút (có pre/post market) | Pilot: 20 cổ phiếu + 30 ETF | Pilot từ 2024; sau khi có thêm disk từ 2016 |
| Index toàn cầu | Daily | ~10 index chính | Từ 2000 |
| Crypto | 1 phút, funding rate, open interest | Pilot: top 10 cặp USDT | Pilot từ 2024; sau đó từ 2018 |
| Macro Mỹ | Daily → quý, **có vintage** | ~30 series chính có vintage, ~500 series mở rộng | Dài nhất có thể |
| Lãi suất, tỷ giá, commodity | Daily | Yield curve, lãi suất qua đêm, ~30 tỷ giá, năng lượng | Dài nhất có thể |
| Macro toàn cầu | Tháng → năm | G20 | Từ 1990 |
| Factor | Daily | Fama-French 3, 5 factor, momentum | Từ 1926 |
| Fundamentals Mỹ | Theo quý, theo ngày nộp báo cáo | S&P 500 | 10 năm, sau khi có thêm disk |
| Reference | | Security master, lịch sàn, corporate actions, thành phần S&P 500 theo ngày | Từ 2005 |

Agent đọc dữ liệu qua một API duy nhất, read-only, với tham số `as_of` bắt buộc (`get_bars`, `get_macro`, `get_universe`, ...).

### 3.2 Phi chức năng

| Yêu cầu | Ý nghĩa cụ thể |
|---|---|
| **Point-in-time** | Đọc tại `as_of = t` không bao giờ trả về thông tin được công bố sau `t`. Có test tự động chứng minh |
| **Truy được nguồn gốc** | Mỗi dòng có `source`, `ingested_at`. Mỗi bảng có trong catalog. Mỗi file tải về có manifest (URL, checksum) |
| **Tái lập được** | Chạy lại ETL từ raw đã lưu, không cần crawl lại. Chạy lại cùng một phần việc cho cùng kết quả |
| **Không sửa tay** | Mọi thay đổi đi qua code và pipeline, có log |
| **Tự vận hành** | Chạy theo lịch hằng ngày, tự retry khi nguồn đăng muộn, cảnh báo khi lỗi hoặc dữ liệu không được cập nhật |
| **An toàn cho server** | Không bao giờ làm đầy disk hay chiếm hết RAM, vì server chạy chung với service khác |

---

## 4. Ràng buộc

| Hạng mục | Giá trị |
|---|---|
| Ngân sách data | **0 USD.** Chỉ dùng nguồn miễn phí, gọi được bằng chương trình, điều khoản cho phép dùng nội bộ |
| Server bee | Máy ảo KVM, AMD EPYC 4 core, 15 GB RAM (**8.5 GB khả dụng**), 48 GB disk (**16 GB trống**), đo ngày 2026-10-13 |
| Dùng chung server | Có service khác đang chạy (dùng 6.7 GB RAM, 33 GB disk) |
| Thời gian | ~4 tuần cho prototype |
| Pháp lý | Dữ liệu chỉ dùng nội bộ cho nghiên cứu, không phân phối lại. Repo private, không commit key và data |

**Disk là ràng buộc chặt nhất.** Bản pilot vừa đủ 16 GB. Bản đầy đủ (minute S&P 500 từ 2016, fundamentals) cần thêm ~100 GB.

---

## 5. Tiêu chí thành công

Prototype được coi là xong khi:

- [ ] Mọi bảng đều có `source` và có trong catalog.
- [ ] Mọi số macro và fundamentals có `available_from`; có test PIT với CPI, GDP và một công ty mẫu.
- [ ] Universe S&P 500 theo ngày có cả mã đã bị loại hoặc hủy niêm yết. Độ phủ giá của các mã này được đo, công bố theo năm, và agent được cảnh báo khi thiếu.
- [ ] Có cả giá gốc và giá điều chỉnh; các split lớn đã biết được điều chỉnh đúng.
- [ ] Chạy lại được toàn bộ silver và gold từ raw mà không crawl lại.
- [ ] Các job hằng ngày chạy ổn định một tuần trên server bee; có cảnh báo khi lỗi.
- [ ] Gọi `get_*` không có `as_of` thì lỗi; có `as_of` thì không bao giờ trả dữ liệu tương lai.

---

## 6. Ngoài phạm vi

- Agent research, backtest engine, execution
- Tick data, order book, options
- FX, commodity, index ở mức phút (không có nguồn miễn phí dùng được)
- Futures minute thật
- Tin tức, mạng xã hội, sentiment
- Real-time streaming (chỉ batch theo lịch)
- Nguồn trả phí
- Thị trường Việt Nam (làm sau prototype nếu cần)

---

## 7. Thuật ngữ

| Thuật ngữ | Giải thích |
|---|---|
| Alpha | Lợi nhuận vượt mức giải thích được bởi thị trường và các factor đã biết |
| Backtest | Giả lập chiến lược trên dữ liệu lịch sử |
| OHLCV | Open, High, Low, Close, Volume của một nến (bar) |
| Bar / nến | Gom giá trong một khoảng thời gian (1 phút, 1 ngày) |
| Point-in-time (PIT) | Dữ liệu đúng như những gì đã biết tại thời điểm đó |
| `as_of` | Thời điểm "đứng" khi đọc dữ liệu; chỉ thấy thông tin đã có trước thời điểm này |
| Vintage | Một phiên bản của số liệu tại một lần công bố hoặc sửa |
| `available_from` / `available_to` | Khoảng thời gian một giá trị (một vintage) có hiệu lực |
| Survivorship-free | Dữ liệu có cả các mã đã biến mất |
| Corporate action | Sự kiện làm thay đổi cổ phiếu: split, cổ tức, spin-off, đổi mã |
| Adjusted price | Giá đã điều chỉnh cho split và cổ tức, để so sánh lịch sử liền mạch |
| Universe | Tập mã được xét trong nghiên cứu |
| Factor | Nguồn lợi nhuận hệ thống đã biết (thị trường, quy mô, giá trị, momentum) |
| Bronze / Silver / Gold | Raw nguyên bản / đã chuẩn hoá và kiểm tra / sẵn dùng cho research |
| Partition | Đơn vị nhỏ nhất của một lần chạy pipeline, ví dụ một mã trong một ngày |
| Backfill | Tải lịch sử quá khứ một lần |
| Idempotent | Chạy lại nhiều lần vẫn cho cùng kết quả |
| Lineage | Truy vết một số liệu về nguồn gốc |
