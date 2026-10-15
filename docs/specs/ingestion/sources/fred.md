# Spec nguồn: FRED / ALFRED

> Code: `src/alphadata/ingestion/sources/fred/` · Ưu tiên: **P1** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `api.stlouisfed.org/fred/series/observations` phản hồi "api_key is not set" khi thiếu key (đúng). ❌ `fred.stlouisfed.org/graph/fredgraph.csv` bị timeout, không dùng.

## Vai trò

Nguồn trung tâm cho macro Mỹ, lãi suất, tỷ giá, giá commodity daily và VIX. ALFRED (cùng API) cho phép lấy **mọi vintage**, là nền tảng của macro point-in-time.

## Truy cập

| Mục | Giá trị |
|---|---|
| Base URL | `https://api.stlouisfed.org/fred/` |
| Key | Miễn phí, đăng ký tại fred.stlouisfed.org. Truyền qua query `api_key` (phải che khi ghi log) |
| Rate limit | 120 request/phút mỗi key (theo nhiều nguồn bên thứ ba, chưa thấy trong tài liệu chính thức). Tự giới hạn 1 request/giây |
| Điều khoản | Phần lớn series là public domain. Một số series có bản quyền của bên thứ ba (ví dụ S&P, ICE BofA): ghi chú trong `macro_series.notes`, không phân phối lại |

## Datasets

| Dataset | Bảng silver | Partition | Endpoint |
|---|---|---|---|
| `fred_series` | `macro_observations` | series_id | `series/observations?realtime_start=1776-07-04&realtime_end=9999-12-31&file_type=json` |
| `fred_series_current` | `macro_observations` | series_id | `series/observations?file_type=json` (chỉ bản hiện hành, cho `fred_wide`) |
| `fred_series_meta` | `macro_series` | series_id | `series?series_id=...` |
| `fred_release_dates` | `macro_release_dates` | release_id | `release/dates?include_release_dates_with_no_data=true` |

`fred_series` dùng cho `fred_core` (~30 series cần vintage). `fred_series_current` cho `fred_wide` (~500 series) để tiết kiệm request và dung lượng.

## Normalize

**`fred_series`**: mỗi observation có `realtime_start`, `realtime_end`, `date`, `value`.
- `obs_date = date`, `available_from = realtime_start`, `available_to = realtime_end`, `vintage_kind = "official"`.
- `value = "."` → null.
- Series có rất nhiều vintage (ví dụ PAYEMS) có thể vượt giới hạn 100.000 dòng mỗi request: phân trang bằng `offset`, mỗi trang một file raw.

**`fred_series_current`**: `available_from` theo quy tắc `observed`:
- Lần đầu crawl một quan sát khi backfill: `available_from = obs_date + độ trễ công bố mặc định` (lấy từ `fred_release_dates` nếu series thuộc release đã biết, nếu không dùng bảng độ trễ theo frequency: D +1 ngày, W +7, M +45, Q +90).
- Chạy hằng ngày: quan sát mới hoặc giá trị đổi → `available_from = ngày crawl`, đóng `available_to` của bản cũ.
- `vintage_kind = "observed"`.

## Checks riêng

| Check | Mức |
|---|---|
| `vintage_order` | error |
| Không có quan sát nào có `available_from < obs_date` | warn (FRED có vài series dự báo, cần xem từng trường hợp) |

## Lịch

22:00 UTC hằng ngày (05:00 giờ VN). Chỉ tải lại series có `last_updated` (từ `fred_series_meta`) mới hơn lần tải trước.

## Done when

- Test PIT: `CPIAUCSL` tại `as_of = 2024-06-01` trả đúng giá trị tháng 4/2024 đã công bố lúc đó, không phải giá trị đã sửa sau này.
- 30 series của `fred_core` có đủ vintage.
