# Spec nguồn: BLS (Bureau of Labor Statistics)

> Code: `src/alphadata/ingestion/sources/bls/` · Ưu tiên: **P3** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ API v1 `api.bls.gov/publicAPI/v1/timeseries/data/CUUR0000SA0` trả CPI đến tháng 9/2026, không cần key.

## Vai trò

CPI và việc làm **chi tiết** (theo nhóm hàng, ngành, vùng) mà FRED không có đủ. CPI tổng và tỷ lệ thất nghiệp đã có từ FRED (có vintage), nên BLS chỉ dùng cho series chi tiết.

## Truy cập

| Mục | v1 (không key) | v2 (key miễn phí) |
|---|---|---|
| Endpoint | `https://api.bls.gov/publicAPI/v1/timeseries/data/` | `https://api.bls.gov/publicAPI/v2/timeseries/data/` |
| Request/ngày | 25 | 500 |
| Series/request | 25 | 50 |
| Số năm/request | 10 | 20 |

Dùng v2 (POST JSON, `registrationkey` trong body, che khi ghi log). Con số giới hạn ở trên lấy theo tài liệu BLS, kiểm tra lại khi đăng ký key.

## Dataset

| Dataset | Bảng silver | Partition |
|---|---|---|
| `bls_series` | `macro_observations` | series_id |

Gom nhiều series vào một request (tối đa 50) để tiết kiệm hạn mức. Partition theo nhóm series thay vì từng series nếu universe lớn.

## Normalize

- Mỗi phần tử: `year`, `period` (`M01`–`M12`, `M13` = trung bình năm, `Q01`–`Q05`, `A01`), `value`, `footnotes`.
- Bỏ `M13`. `obs_date` = ngày đầu tháng/quý.
- Footnote "preliminary" ghi vào ghi chú, không dùng làm vintage.
- `vintage_kind = "observed"`. Khi backfill: `available_from` lấy từ lịch công bố của release tương ứng trong `fred_release_dates` (CPI, Employment Situation đều có trên FRED). Không có thì `+45 ngày`.

## Lịch

14:00 UTC các ngày có release của BLS (đọc từ `macro_release_dates`), mặc định hằng ngày nếu chưa có lịch.

## Done when

- Universe `bls_detail.yaml` (CPI theo nhóm hàng chính, lương giờ theo ngành) có đủ lịch sử 20 năm.
