# Spec: `gold.universe_daily`

> Code: `src/alphadata/gold/universe_daily/` · Tầng: gold · Phase 3
> Phụ thuộc: `reference.index_membership`, `reference.security_master`, `gold.bars`

## Mục đích

Trả lời: **tại ngày `t`, những mã nào hợp lệ để agent research?** Một mã hợp lệ khi thuộc universe tại `t` (không dùng thông tin sau `t`) và có đủ dữ liệu.

Phân biệt với [06-universe.md](../06-universe.md): `universe` là danh sách cần crawl (tĩnh), còn bảng này là danh sách research theo từng ngày.

## Bảng

Ghi bảng `universe_daily`: một dòng cho mỗi (universe, ngày, chứng khoán thuộc universe tại ngày đó), kèm `has_price`, `adv_20d` (tính đến `t-1`) và `tradable` (`has_price` và `adv_20d` trên ngưỡng, mặc định 1 triệu USD). Cột và kiểu ở [04-data-model.md](../../04-data-model.md) mục 6.

## Định nghĩa

| universe_code | Thành viên tại `t` |
|---|---|
| `SP500` | `index_membership.members("SP500", t)` |
| `US_ETF_CORE` | Các ETF trong `config/universe/us_etf_core.yaml` đã niêm yết tại `t` |
| `US_MINUTE_PILOT` | ETF core + 20 cổ phiếu pilot. **Cảnh báo:** 20 cổ phiếu được chọn theo thanh khoản ở thời điểm chốt, nên universe này có survivorship bias. Ghi rõ trong catalog |
| `CRYPTO_PILOT` | 10 cặp pilot, từ ngày cặp có dữ liệu trên Binance |

Mọi đặc trưng dùng để lọc (ví dụ `adv_20d`) chỉ dùng dữ liệu đến `t-1`.

## Độ phủ giá và survivorship

Nguồn miễn phí (Yahoo, Alpaca) thường không còn giá của mã đã hủy niêm yết. Dữ liệu có thì dùng được; vấn đề là **thiếu**. Thiếu như vậy thì mã đó coi như không tồn tại khi backtest, và vì mã hay bị thiếu thường là mã đã phá sản hoặc giảm mạnh, backtest sẽ lạc quan hơn thực tế. Không sửa được chỗ thiếu trong ngân sách 0 USD, nên cách xử lý là **đo, đánh dấu và cảnh báo**, không chặn pipeline.

**Quy tắc lưu:** mã thuộc universe tại `t` luôn có dòng trong bảng, kể cả khi không có giá (`has_price = false`). Không bao giờ bỏ dòng vì thiếu giá.

**Định nghĩa độ phủ** (dùng chung cho check, gate và API):

```
độ phủ(universe, năm) = số (ngày, security_id) có has_price = true
                        ─────────────────────────────────────────
                        số (ngày, security_id) thuộc universe trong năm đó
```

Kèm theo danh sách mã **không có giá ngày nào** trong suốt thời gian là thành viên.

**Hành động mặc định theo độ phủ của từng năm:**

| Độ phủ | Hành động |
|---|---|
| ≥ 90% | Check `price_coverage` qua |
| 70% – 90% | Check `price_coverage` ở mức `warn`, hiện trong báo cáo DQ. Pipeline chạy bình thường |
| < 70% | Như trên, thêm alert gửi leader. Pipeline vẫn chạy bình thường |

Ngưỡng khai báo trong code của check, đổi được mà không sửa spec khác.

**Công bố con số:**
- Check `price_coverage` ghi độ phủ theo năm và danh sách mã thiếu vào `dq_results.detail` (xem [05-quality.md](../05-quality.md)).
- Job ghi độ phủ tổng và năm thấp nhất vào `table_catalog.notes` của `universe_daily`, để ai xem catalog cũng thấy.
- `access.get_universe` cảnh báo agent khi universe tại `as_of` có mã thiếu giá; `access.get_coverage` trả độ phủ theo năm (xem [10-access.md](../10-access.md)).

## Done when

- `SP500` tại 2008-09-01 có Lehman Brothers, có dòng dù `has_price` là true hay false.
- Check `price_coverage` chạy sau mỗi lần dựng bảng, có độ phủ theo năm từ 2005.
- Test: lake fixture có một mã thuộc universe nhưng không có bar nào thì mã đó vẫn có dòng với `has_price = false`, độ phủ giảm đúng tỷ lệ.
