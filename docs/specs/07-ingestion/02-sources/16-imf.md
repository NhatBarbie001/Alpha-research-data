# Spec nguồn: IMF

> Code: `src/alphadata/ingestion/sources/imf/` · Ưu tiên: **P4** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `api.imf.org/external/sdmx/2.1/dataflow` trả danh sách dataflow (XML 449 KB). ✅ DataMapper `www.imf.org/external/datamapper/api/v1/NGDP_RPCH/USA` trả JSON. ❌ Endpoint cũ `dataservices.imf.org/REST/SDMX_JSON.svc` không phản hồi (IMF đã chuyển sang portal mới).

## Vai trò

- **WEO (World Economic Outlook)**: dự báo tăng trưởng, lạm phát, cán cân vãng lai của IMF, công bố tháng 4 và tháng 10. Bản thân dự báo và việc sửa dự báo là tín hiệu.
- **IFS** và các dataset SDMX khác: tỷ giá, dự trữ ngoại hối, lãi suất chính sách của nhiều nước.

## Truy cập

| Nguồn | Endpoint | Ghi chú |
|---|---|---|
| DataMapper | `https://www.imf.org/external/datamapper/api/v1/<indicator>/<ISO3>` | JSON đơn giản, chỉ bản WEO mới nhất, không có vintage |
| SDMX | `https://api.imf.org/external/sdmx/2.1/data/<flow>/<key>` | Chuẩn SDMX, có nhiều dataset. Cấu trúc key xem từ `/dataflow` và `/datastructure` |

Không cần key. Điều khoản: dữ liệu IMF được dùng lại khi ghi nguồn; kiểm tra điều khoản cụ thể của từng dataset trên trang IMF khi viết connector.

## Dataset

| Dataset | Bảng silver | Partition |
|---|---|---|
| `imf_weo` | `macro_observations` | indicator |
| `imf_sdmx` | `macro_observations` | flow |

Universe `imf_core.yaml` (đề xuất): WEO `NGDP_RPCH` (tăng trưởng GDP thực), `PCPIPCH` (lạm phát), `BCA_NGDPD` (cán cân vãng lai / GDP), cho G20 + Việt Nam.

## Normalize

- `series_id = "<indicator>:<ISO3>"`, `obs_date = <năm>-01-01`. WEO có cả năm tương lai (dự báo): giữ nguyên, thêm `notes = "forecast"` ở `macro_series`.
- `vintage_kind = "observed"`. **Mỗi kỳ WEO là một vintage**: lưu snapshot mỗi lần tải; giá trị đổi so với lần trước thì mở bản mới với `available_from` = ngày phát hành WEO (tháng 4, tháng 10) nếu biết, nếu không thì ngày crawl.
- Backfill không lấy được các kỳ WEO cũ qua DataMapper. Các kỳ cũ có dạng file tải về trên trang WEO database: ghi nhận là việc mở rộng, chưa làm ở prototype.

## Lịch

Hằng tuần, thứ Hai. Tháng 4 và tháng 10 hằng ngày trong 2 tuần quanh ngày phát hành WEO.

## Done when

- Ba indicator WEO đề xuất có snapshot đầu tiên, và lần chạy sau ghi nhận đúng giá trị đổi như vintage mới.
