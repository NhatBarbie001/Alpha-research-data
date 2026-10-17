# Spec nguồn: World Bank Indicators API

> Code: `src/alphadata/ingestion/sources/worldbank/` · Ưu tiên: **P4** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `api.worldbank.org/v2/country/US/indicator/NY.GDP.MKTP.CD?format=json` trả JSON (dữ liệu đến 2025, cập nhật 2026-10-08).

## Vai trò

Chỉ số macro **theo năm** cho nhiều nước (GDP, lạm phát, dân số, nợ, thương mại). Tần suất thấp, chủ yếu dùng làm đặc trưng cấp quốc gia cho alpha liên thị trường.

## Truy cập

| Mục | Giá trị |
|---|---|
| Endpoint | `https://api.worldbank.org/v2/country/<ISO2 hoặc all>/indicator/<indicator>?format=json&per_page=1000&page=<n>` |
| Key | Không cần |
| Định dạng | JSON: phần tử 0 là thông tin phân trang, phần tử 1 là dữ liệu |
| Điều khoản | CC BY 4.0 |

## Dataset

| Dataset | Bảng silver | Partition |
|---|---|---|
| `worldbank_indicator` | `macro_observations` | indicator |

Universe `worldbank_core.yaml` (đề xuất): `NY.GDP.MKTP.CD`, `NY.GDP.MKTP.KD.ZG`, `FP.CPI.TOTL.ZG`, `GC.DOD.TOTL.GD.ZS`, `NE.TRD.GNFS.ZS`, `SL.UEM.TOTL.ZS`, cho G20 + Việt Nam.

## Normalize

- `series_id = "<indicator>:<countryiso3code>"`, `obs_date = <năm>-01-01`.
- `vintage_kind = "observed"`. Khi backfill: `available_from = 1/7 năm kế tiếp` (số liệu năm thường có vào giữa năm sau). Mỗi lần World Bank cập nhật (`lastupdated` đổi) thì ghi nhận giá trị đổi như vintage mới.

## Lịch

Hằng tháng, ngày 10. Chỉ tải lại khi `lastupdated` mới hơn lần trước.

## Done when

- Sáu indicator đề xuất có đủ cho G20 từ 1990.
