# Spec nguồn: SEC EDGAR

> Code: `src/alphadata/ingestion/sources/sec_edgar/` · Ưu tiên: **P2** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json` (Apple, 3.8 MB) và `www.sec.gov/files/company_tickers.json` tải được với User-Agent có email.

## Vai trò

- Fundamentals của công ty Mỹ, với **ngày nộp báo cáo (`filed`) làm thời điểm công bố**, là chìa khoá cho fundamentals point-in-time.
- Ánh xạ CIK ↔ ticker cho `reference.security_master`.

## Truy cập

| Mục | Giá trị |
|---|---|
| Endpoint | `https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`, `https://data.sec.gov/submissions/CIK##########.json`, `https://www.sec.gov/files/company_tickers.json` |
| Bulk | `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` (toàn bộ companyfacts, cập nhật hằng đêm) |
| Key | Không cần. **Bắt buộc** header `User-Agent` có tên và email liên hệ, nếu không SEC chặn |
| Rate limit | 10 request/giây (SEC công bố). Tự giới hạn 8/giây |
| Điều khoản | Dữ liệu công khai của chính phủ Mỹ |

## Datasets

| Dataset | Bảng silver | Partition | Nguồn |
|---|---|---|---|
| `sec_companyfacts` | `fundamentals_facts` | cik | companyfacts JSON từng công ty (hằng ngày, chỉ công ty có filing mới) hoặc bulk zip (backfill) |
| `sec_company_tickers` | `listing_snapshots` | snapshot_date | `company_tickers.json` |

## Normalize

**companyfacts.** Cấu trúc: `facts.<taxonomy>.<concept>.units.<unit>[]`, mỗi phần tử có `start` (tuỳ), `end`, `val`, `accn`, `fy`, `fp`, `form`, `filed`, `frame` (tuỳ).
- Một dòng silver cho mỗi phần tử. `filed_date = filed`, `period_start = start`, `period_end = end`.
- Cùng một số liệu xuất hiện trong nhiều filing (10-Q rồi 10-K, hoặc bản sửa 10-K/A): **giữ tất cả**, khoá có `accession`. Gold chọn bản theo `as_of`.
- Chỉ lấy taxonomy `us-gaap` và `dei` ở prototype.

**company_tickers.** JSON `{"0": {"cik_str", "ticker", "title"}, ...}` → `listing_snapshots` với `source = "sec_edgar"`, `snapshot_date = ngày tải`, `cik`.

## Hành vi đặc biệt

- Backfill: tải bulk zip một lần (~1 GB nén, kiểm tra kích thước trước khi tải vì disk hạn chế), chỉ giải nén công ty thuộc universe S&P 500. Xoá zip sau khi xong.
- Hằng ngày: đọc `submissions` của từng CIK trong universe để biết có filing mới không, chỉ tải lại companyfacts khi có.
- `company_tickers.json` chỉ có mã **đang** niêm yết. Lưu snapshot hằng ngày để dần có lịch sử.

## Checks riêng

| Check | Mức |
|---|---|
| `filed_date >= period_end` | warn |
| `cik` có trong universe | warn |

## Lịch

Sau 06:00 UTC hằng ngày (SEC cập nhật bulk hằng đêm, giờ Mỹ).

## Dung lượng

S&P 500, 10 năm, chỉ `us-gaap` + `dei`: ước 2–5 GB. **Chỉ làm sau khi có thêm disk** (xem [02-architecture.md](../../../02-architecture.md) mục 5.2). Trước đó chỉ chạy `sec_company_tickers`.

## Done when

- Với một công ty mẫu, doanh thu một quý khớp báo cáo gốc, và `filed_date` khớp ngày nộp trên EDGAR.
