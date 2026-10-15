# Spec nguồn: Nasdaq Trader symbol directory

> Code: `src/alphadata/ingestion/sources/nasdaq_trader/` · Ưu tiên: **P2** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt` tải được (349 KB).

## Vai trò

Danh sách mã đang niêm yết trên các sàn Mỹ, có cờ ETF và sàn. Snapshot hằng ngày giúp `reference.security_master` biết mã nào mới niêm yết, mã nào biến mất (hủy niêm yết hoặc đổi tên) **từ nay về sau**. Không có lịch sử quá khứ.

## Truy cập

| File | URL | Nội dung |
|---|---|---|
| `nasdaqlisted.txt` | `https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt` | Mã niêm yết trên Nasdaq |
| `otherlisted.txt` | `https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt` | Mã niêm yết trên NYSE, NYSE American, NYSE Arca, Cboe, ... |

Không cần key. Định dạng: phân cách `|`, dòng cuối là `File Creation Time: ...`. Cập nhật mỗi ngày giao dịch.

## Dataset

| Dataset | Bảng silver | Partition | keep_raw |
|---|---|---|---|
| `nasdaq_listings` | `listing_snapshots` | snapshot_date | True (file nhỏ, raw là lịch sử duy nhất) |

## Normalize

- `nasdaqlisted`: Symbol, Security Name, Market Category, Test Issue, Financial Status, Round Lot Size, ETF, NextShares.
- `otherlisted`: ACT Symbol, Security Name, Exchange (A/N/P/Z/V), CQS Symbol, ETF, Round Lot Size, Test Issue, NASDAQ Symbol.
- `snapshot_date` = ngày trong dòng `File Creation Time`, không phải ngày tải.
- Bỏ dòng `Test Issue = Y` khỏi phân tích, nhưng vẫn lưu với `is_test_issue = true`.

## Lịch

Hằng ngày 02:00 UTC, thứ Ba–thứ Bảy.

## Done when

- Sau 1 tuần, so sánh hai snapshot liên tiếp cho ra danh sách mã mới / mã biến mất.
