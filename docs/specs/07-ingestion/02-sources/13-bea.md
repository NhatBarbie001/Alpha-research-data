# Spec nguồn: BEA (Bureau of Economic Analysis)

> Code: `src/alphadata/ingestion/sources/bea/` · Ưu tiên: **P3** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `apps.bea.gov/api/data` phản hồi lỗi "UserID không hợp lệ" với key giả (đúng).

## Vai trò

GDP và thu nhập **chi tiết theo cấu phần** (bảng NIPA). GDP tổng và PCE đã có từ FRED có vintage; BEA dùng cho các dòng chi tiết.

## Truy cập

| Mục | Giá trị |
|---|---|
| Endpoint | `https://apps.bea.gov/api/data?UserID=<key>&method=GetData&datasetname=NIPA&TableName=<bảng>&Frequency=Q&Year=ALL&ResultFormat=JSON` |
| Key | Miễn phí, đăng ký tại apps.bea.gov/api. Che `UserID` khi ghi log |
| Rate limit | 100 request/phút, 100 MB/phút, 30 lỗi/phút. Vượt thì bị khoá 1 giờ. Tự giới hạn 1 request/giây |
| Điều khoản | Dữ liệu công khai của chính phủ Mỹ |

## Dataset

| Dataset | Bảng silver | Partition |
|---|---|---|
| `bea_nipa` | `macro_observations` | table_name |

Universe `bea_nipa.yaml` (đề xuất): T10101 (tăng trưởng GDP thực và cấu phần), T10105 (GDP danh nghĩa), T20100 (thu nhập cá nhân), T20600 (thu nhập và chi tiêu cá nhân theo tháng).

## Normalize

- Mỗi dòng: `TableName`, `SeriesCode`, `LineNumber`, `LineDescription`, `TimePeriod` (`2026Q2`, `2026M08`), `DataValue` (có dấu phẩy ngăn nghìn), `CL_UNIT`, `UNIT_MULT`.
- `series_id = "<TableName>:<SeriesCode>"`. Giá trị nhân theo `UNIT_MULT`, ghi đơn vị vào `macro_series`.
- `vintage_kind = "observed"`. Khi backfill: `available_from` theo lịch release GDP/Personal Income trong `fred_release_dates`, không có thì `+30 ngày` sau cuối quý/tháng.

## Lịch

14:00 UTC các ngày có release của BEA, mặc định hằng tuần.

## Done when

- Bốn bảng đề xuất có dữ liệu từ 1947 (quý) / 1959 (tháng).
