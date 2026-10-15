# Spec: `gold.universe_daily`

> Code: `src/alphadata/gold/universe_daily/` · Tầng: gold · Phase 3
> Phụ thuộc: `reference.index_membership`, `reference.security_master`, `gold.bars`

## Mục đích

Trả lời: **tại ngày `t`, những mã nào hợp lệ để agent research?** Một mã hợp lệ khi thuộc universe tại `t` (không dùng thông tin sau `t`) và có đủ dữ liệu.

Phân biệt với [universe.md](../universe.md): `universe` là danh sách cần crawl (tĩnh), còn bảng này là danh sách research theo từng ngày.

## Bảng `universe_daily`

| Cột | Ghi chú |
|---|---|
| universe_code | `SP500`, `US_ETF_CORE`, `US_MINUTE_PILOT`, `CRYPTO_PILOT` |
| date | Ngày giao dịch |
| security_id | |
| has_price | Có bar daily ngày `t` |
| adv_20d | Giá trị giao dịch trung bình 20 ngày **tính đến `t-1`** (USD) |
| tradable | `has_price` và `adv_20d` trên ngưỡng tối thiểu (mặc định 1 triệu USD) |

## Định nghĩa

| universe_code | Thành viên tại `t` |
|---|---|
| `SP500` | `index_membership.members("SP500", t)` |
| `US_ETF_CORE` | Các ETF trong `config/universe/us_etf_core.yaml` đã niêm yết tại `t` |
| `US_MINUTE_PILOT` | ETF core + 20 cổ phiếu pilot. **Cảnh báo:** 20 cổ phiếu được chọn theo thanh khoản ở thời điểm chốt, nên universe này có survivorship bias. Ghi rõ trong catalog |
| `CRYPTO_PILOT` | 10 cặp pilot, từ ngày cặp có dữ liệu trên Binance |

Mọi đặc trưng dùng để lọc (ví dụ `adv_20d`) chỉ dùng dữ liệu đến `t-1`.

## Báo cáo survivorship

Job dựng bảng này xuất kèm danh sách `(universe_code, date, security_id)` thuộc universe nhưng `has_price = false`. Tỷ lệ này cho mỗi năm là con số đính kèm mọi kết quả backtest.

## Done when

- `SP500` tại 2008-09-01 có Lehman Brothers với `has_price` đúng thực tế dữ liệu.
- Báo cáo survivorship theo năm từ 2005.
