# Spec nguồn: OECD Data Explorer (SDMX)

> Code: `src/alphadata/ingestion/sources/oecd/` · Ưu tiên: **P4** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `sdmx.oecd.org/public/rest/dataflow/all` trả danh sách dataflow (SDMX-JSON 1.7 MB).

## Vai trò

**Composite Leading Indicators (CLI)** của OECD, một chỉ báo chu kỳ được dùng rộng rãi, cùng các series macro của các nước OECD (lãi suất, thất nghiệp, sản xuất công nghiệp).

## Truy cập

| Mục | Giá trị |
|---|---|
| Endpoint | `https://sdmx.oecd.org/public/rest/data/<agency>,<flow>,<version>/<key>?startPeriod=YYYY-MM&format=csvfilewithlabels` |
| Key | Không cần |
| Rate limit | OECD có giới hạn số request theo giờ cho API công khai. Con số cụ thể kiểm tra khi viết connector. Tự giới hạn 1 request/5 giây và gom nhiều nước vào một key |
| Điều khoản | Dữ liệu OECD dùng lại được khi ghi nguồn (kiểm tra lại điều khoản trên trang OECD) |

## Dataset

| Dataset | Bảng silver | Partition |
|---|---|---|
| `oecd_series` | `macro_observations` | flow |

Universe `oecd_core.yaml` (đề xuất): CLI theo tháng (dataflow `DSD_STES@DF_CLI`) cho G7 + Trung Quốc + Ấn Độ + Brazil. Mã dataflow và key chính xác chốt khi viết connector.

## Normalize

- `series_id` = key SDMX đầy đủ của series. `obs_date` từ `TIME_PERIOD`.
- CLI bị sửa lại mỗi tháng (giá trị các tháng gần nhất thay đổi): `vintage_kind = "observed"`, ghi nhận giá trị đổi như vintage mới. Khi backfill: `available_from = obs_date + 40 ngày` (CLI công bố khoảng giữa tháng sau tháng kế tiếp).

## Lịch

Hằng tuần, thứ Ba.

## Done when

- CLI của 10 nước đề xuất có từ 1960 (hoặc dài nhất có thể), và lần chạy hằng tuần ghi nhận được các lần sửa.
