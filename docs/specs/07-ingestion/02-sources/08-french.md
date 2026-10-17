# Spec nguồn: Kenneth French Data Library

> Code: `src/alphadata/ingestion/sources/french/` · Ưu tiên: **P2** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `F-F_Research_Data_Factors_daily_CSV.zip` tải được (178 KB).

## Vai trò

Factor chuẩn (Fama-French, momentum) để agent **benchmark** alpha: một alpha chỉ là beta với các factor này thì không phải alpha thật.

## Truy cập

| Mục | Giá trị |
|---|---|
| Base URL | `https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/` |
| Key | Không cần |
| Định dạng | Zip chứa một CSV có vài dòng mô tả ở đầu, có thể có nhiều bảng trong một file (daily rồi annual) |
| Điều khoản | Công khai cho nghiên cứu |

## Dataset

| Dataset | Bảng silver | Partition |
|---|---|---|
| `french_factors` | `factor_returns` | dataset |

File cần tải (giá trị `dataset` tương ứng):

| File | `dataset` | Factor |
|---|---|---|
| `F-F_Research_Data_Factors_daily_CSV.zip` | `ff3_daily` | Mkt-RF, SMB, HML, RF |
| `F-F_Research_Data_5_Factors_2x3_daily_CSV.zip` | `ff5_daily` | Mkt-RF, SMB, HML, RMW, CMA, RF |
| `F-F_Momentum_Factor_daily_CSV.zip` | `mom_daily` | Mom |

## Normalize

- Bỏ các dòng mô tả ở đầu, đọc đến dòng trống đầu tiên (hết bảng daily).
- `date` từ dạng `YYYYMMDD`. Giá trị là **phần trăm** mỗi ngày, giữ nguyên đơn vị và ghi trong catalog.
- `vintage_date` = ngày tải. Thư viện được tính lại định kỳ (thường hằng năm) nên giá trị cũ có thể đổi; giữ mọi vintage.

## Lịch

Hằng tháng, ngày 5, 03:00 UTC. File được cập nhật hằng tháng.

## Done when

- Ba dataset có dữ liệu từ 1926 (FF3, Mom) và 1963 (FF5).
- Format CSV đổi (thêm dòng mô tả, đổi tên cột) thì connector báo `SourceFormatChanged`, có test.
