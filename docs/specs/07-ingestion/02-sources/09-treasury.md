# Spec nguồn: US Treasury

> Code: `src/alphadata/ingestion/sources/treasury/` · Ưu tiên: **P2** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ CSV par yield curve năm 2026 (dữ liệu đến 2026-10-13) và CSV real yield curve tải được. ✅ API `api.fiscaldata.treasury.gov` phản hồi.

## Vai trò

Yield curve **chính thức** của Bộ Tài chính Mỹ, đủ các kỳ hạn trong một bảng (1 tháng đến 30 năm), có cả real yield (TIPS). FRED có các series `DGS*` tương tự nhưng là bản sao; Treasury là nguồn gốc và có thêm kỳ hạn (1.5 tháng, 4 tháng).

## Truy cập

| Mục | Giá trị |
|---|---|
| Endpoint | `https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/<YYYY>/all?type=<type>&field_tdr_date_value=<YYYY>&page&_format=csv` |
| `type` | `daily_treasury_yield_curve` (par yield), `daily_treasury_real_yield_curve` (real yield) |
| Key | Không cần |
| Điều khoản | Dữ liệu công khai của chính phủ Mỹ |

API fiscaldata (`api.fiscaldata.treasury.gov`) có dữ liệu tài khoá (nợ công, lãi suất trung bình). **Chưa dùng ở prototype**, ghi lại để mở rộng sau.

## Datasets

| Dataset | Bảng silver | Partition |
|---|---|---|
| `treasury_par_yield` | `macro_observations` | year |
| `treasury_real_yield` | `macro_observations` | year |

## Normalize

- CSV dạng rộng: `Date`, rồi một cột mỗi kỳ hạn (`"1 Mo"`, `"1.5 Month"`, ..., `"30 Yr"`). Chuyển sang dạng dài.
- `source = "treasury"`, `series_id = "par_<kỳ hạn>"` hoặc `"real_<kỳ hạn>"` (ví dụ `par_10y`, `real_5y`). Ánh xạ tên cột sang mã kỳ hạn đặt trong code, cột lạ → `SourceFormatChanged`.
- `Date` dạng `MM/DD/YYYY`.
- Treasury không công bố vintage: `vintage_kind = "observed"`, `available_from = obs_date + 1 ngày` khi backfill (số được công bố cuối ngày giao dịch, giờ Mỹ), `= ngày crawl` khi chạy hằng ngày.
- Kỳ hạn mới xuất hiện giữa chừng (ví dụ 1.5 tháng từ 2025): năm cũ không có cột đó là bình thường.

## Lịch

23:00 UTC thứ Hai–thứ Sáu. Mỗi lần tải lại cả năm hiện tại (file nhỏ).

## Done when

- Par yield từ 1990, real yield từ 2003, có đủ mọi kỳ hạn.
- `par_10y` khớp FRED `DGS10` trong mọi ngày chung.
