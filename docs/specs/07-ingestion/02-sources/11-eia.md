# Spec nguồn: EIA (US Energy Information Administration)

> Code: `src/alphadata/ingestion/sources/eia/` · Ưu tiên: **P3** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `api.eia.gov/v2/petroleum/pri/spt/data/` phản hồi `API_KEY_MISSING` khi thiếu key (đúng).

## Vai trò

Giá spot năng lượng (dầu, khí, xăng) và **tồn kho dầu hằng tuần**, một chỉ báo được thị trường theo dõi sát. FRED có một số giá spot (bản sao từ EIA), nhưng không có tồn kho chi tiết.

## Truy cập

| Mục | Giá trị |
|---|---|
| Base URL | `https://api.eia.gov/v2/` |
| Key | Miễn phí, đăng ký tại eia.gov/opendata. Query `api_key` (che khi ghi log) |
| Giới hạn | Tối đa 5.000 dòng mỗi response, phân trang bằng `offset`. Rate limit không công bố rõ, tự giới hạn 1 request/giây |
| Điều khoản | Dữ liệu công khai của chính phủ Mỹ |

## Dataset

| Dataset | Bảng silver | Partition |
|---|---|---|
| `eia_series` | `macro_observations` | series_id |

Universe `eia_core.yaml` (đề xuất): giá spot WTI, Brent, Henry Hub, xăng RBOB; tồn kho dầu thô thương mại Mỹ (không gồm SPR), tồn kho xăng, tồn kho distillate (weekly). Mã series cụ thể chốt khi viết connector bằng API browse `/v2/<route>/facet`.

## Normalize

- `series_id` = `<route>:<facet series>` (ví dụ `petroleum/pri/spt:RWTC`).
- `obs_date` từ `period`. Weekly của EIA lấy ngày kết thúc tuần.
- `vintage_kind = "observed"`. Báo cáo tồn kho tuần (Weekly Petroleum Status Report) công bố thứ Tư 10:30 ET cho tuần kết thúc thứ Sáu trước: khi backfill `available_from = obs_date + 5 ngày`. Giá spot daily: `+1 ngày`.

## Lịch

Daily spot: 22:00 UTC hằng ngày. Tồn kho: 16:00 UTC thứ Tư và thứ Năm (phòng trễ do nghỉ lễ).

## Done when

- Tồn kho dầu thô tuần có từ 1982, giá WTI daily khớp FRED `DCOILWTICO`.
