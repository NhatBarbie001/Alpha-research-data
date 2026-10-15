# Spec nguồn: NY Fed Markets API

> Code: `src/alphadata/ingestion/sources/nyfed/` · Ưu tiên: **P2** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `markets.newyorkfed.org/api/rates/secured/sofr/last/2.json` và `.../unsecured/effr/last/1.json` trả JSON.

## Vai trò

Lãi suất tham chiếu qua đêm chính thức: SOFR, EFFR, OBFR, kèm phân vị và khối lượng. Đây là nguồn gốc; FRED chỉ có giá trị chính, không có phân vị và khối lượng.

## Truy cập

| Mục | Giá trị |
|---|---|
| Base URL | `https://markets.newyorkfed.org/api/rates/` |
| Endpoint | `secured/sofr`, `unsecured/effr`, `unsecured/obfr`, với `/last/<n>.json` hoặc `/search.json?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD` |
| Key | Không cần |
| Điều khoản | Dữ liệu công khai của Federal Reserve Bank of New York |

## Dataset

| Dataset | Bảng silver | Partition |
|---|---|---|
| `nyfed_reference_rates` | `macro_observations` | series_id (`sofr`, `effr`, `obfr`) |

## Normalize

- Mỗi phần tử `refRates[]`: `effectiveDate`, `type`, `percentRate`, `percentPercentile1/25/75/99`, `volumeInBillions`, `revisionIndicator`.
- Mỗi trường số là một series: `series_id = "<type>"` cho `percentRate`, `"<type>_p1"`, `"<type>_p25"`, `"<type>_p75"`, `"<type>_p99"`, `"<type>_volume"`.
- `revisionIndicator` khác rỗng nghĩa là số đã bị sửa: đóng `available_to` của bản cũ, mở bản mới với `available_from = ngày crawl`.
- `vintage_kind = "observed"`. Khi backfill: `available_from = effectiveDate + 1 ngày` (NY Fed công bố sáng ngày làm việc kế tiếp).

## Lịch

14:00 UTC thứ Hai–thứ Sáu (sau giờ công bố 8:00 ET).

## Done when

- SOFR từ 2018-04, EFFR và OBFR từ 2016 có đủ.
