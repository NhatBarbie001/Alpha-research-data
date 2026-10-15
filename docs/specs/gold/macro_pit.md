# Spec: `gold.macro_pit`

> Code: `src/alphadata/gold/macro_pit/` · Tầng: gold · Phase 3
> Phụ thuộc: silver `macro_observations`, `macro_series`, `fundamentals_facts`, `factor_returns`; `reference.security_master`

## Mục đích

Dữ liệu macro, factor và fundamentals ở dạng **point-in-time**: với mỗi ngày `as_of`, chỉ thấy giá trị đã được biết tại ngày đó.

## Bảng

**`macro_pit`**: khoá (`series_key`, `obs_date`, `available_from`).

| Cột | Ghi chú |
|---|---|
| series_key | `<source>:<series_id>`, ví dụ `fred:CPIAUCSL`, `treasury:par_10y` |
| obs_date, value | |
| available_from, available_to | Từ silver |
| vintage_kind | `official` / `observed` |
| frequency, units | Từ `macro_series` |

Gồm cả factor Fama-French (`french:ff3_daily:HML`), với `available_from = vintage_date` cho vintage sau lần tải đầu, và `= date + 1 ngày` cho lần tải đầu.

**`fundamentals_pit`**: khoá (`security_id`, `concept`, `period_end`, `available_from`).

| Cột | Ghi chú |
|---|---|
| security_id | Map từ `cik` qua `security_master` |
| concept, unit, value | |
| period_start, period_end, fiscal_period | |
| available_from | `filed_date + 1 ngày` (báo cáo có thể nộp sau giờ đóng cửa) |
| available_to | Ngày filing kế tiếp sửa cùng số liệu, hoặc `9999-12-31` |
| form, accession | |

## Quy tắc PIT

- Query tại `as_of`: lấy dòng có `available_from <= as_of AND as_of <= available_to`.
- Với `observed` khi backfill, `available_from` là **ước lượng** theo độ trễ công bố. Ước lượng phải nghiêng về phía muộn (an toàn): thà agent thấy số muộn một ngày còn hơn thấy sớm.
- Fundamentals: cùng (`security_id`, `concept`, `period_end`) có nhiều filing (10-Q ban đầu, 10-K, 10-K/A) → mỗi filing là một vintage, filing sau đóng `available_to` của filing trước.

## Done when

- Test PIT cho `fred:CPIAUCSL`, `fred:GDP`, `fred:PAYEMS`: giá trị tại một `as_of` quá khứ khớp bản công bố lúc đó (đối chiếu thủ công trên ALFRED).
- Test fundamentals: doanh thu một quý của một công ty có 10-Q rồi bị sửa ở 10-K, `as_of` giữa hai ngày nộp trả giá trị 10-Q.
- Không dòng nào có `available_from < obs_date` trừ series dự báo đã ghi chú.
