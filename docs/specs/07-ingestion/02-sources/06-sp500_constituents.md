# Spec nguồn: S&P 500 constituents (Wikipedia + GitHub fja05680/sp500)

> Code: `src/alphadata/ingestion/sources/sp500_constituents/` · Ưu tiên: **P2** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ Trang Wikipedia "List of S&P 500 companies" tải được. ✅ File `S&P 500 Historical Components & Changes (Updated).csv` trên GitHub tải được, có dữ liệu từ 1996-01-02 đến 2026-08-18, commit gần nhất 2026-09-07, license MIT.

## Vai trò

Đầu vào cho `reference.index_membership`: mã nào thuộc S&P 500 tại từng ngày. Đây là cách duy nhất miễn phí để giảm survivorship bias cho universe US.

## Truy cập

| Nguồn | URL | Định dạng | Ghi chú |
|---|---|---|---|
| GitHub fja05680/sp500 | `https://raw.githubusercontent.com/fja05680/sp500/master/S%26P%20500%20Historical%20Components%20%26%20Changes%20(Updated).csv` | CSV: `date,tickers` (danh sách mã cách nhau bởi dấu phẩy) | Mỗi dòng là thành phần đầy đủ tại một ngày có thay đổi. License MIT. Cập nhật thủ công bởi tác giả, có thể trễ vài tuần |
| Wikipedia | `https://en.wikipedia.org/wiki/List_of_S%26P_500_companies` | HTML, 2 bảng: thành phần hiện tại (có CIK, ngày thêm vào) và lịch sử thay đổi | License CC BY-SA. Dùng User-Agent có liên hệ theo chính sách của Wikimedia. Có thể dùng MediaWiki API (`action=parse`) thay vì HTML |

Tên file GitHub có thể đổi (tác giả từng đặt ngày vào tên file). Connector liệt kê thư mục qua GitHub API và chọn file `...(Updated).csv`, báo `SourceFormatChanged` nếu không thấy.

## Datasets

| Dataset | Bảng silver | Partition |
|---|---|---|
| `sp500_history_github` | `index_constituent_snapshots` | index_code |
| `sp500_wikipedia` | `index_constituent_changes` (bảng thay đổi) và `index_constituent_snapshots` (bảng hiện tại, `snapshot_date` = ngày tải) | index_code |

`sp500_wikipedia` ghi vào hai bảng nên tách thành hai dataset con dùng chung raw: `sp500_wikipedia_changes`, `sp500_wikipedia_current`.

## Normalize

- GitHub: mỗi dòng `date, tickers` → nhiều dòng `(SP500, snapshot_date=date, symbol)`, `source = "fja05680"`.
- Wikipedia changes: mỗi dòng có ngày, mã thêm, tên, mã bỏ, tên, lý do → hai dòng `add` / `remove`.
- Ticker giữ nguyên dạng của nguồn (ví dụ `BRK.B`). Chuẩn hoá ký hiệu (dấu chấm, gạch ngang) làm ở `reference.security_master`.

## Đối chiếu

`reference.index_membership` dùng GitHub làm chuỗi chính, Wikipedia để cập nhật phần sau ngày cuối của GitHub và để đối chiếu. Snapshot hiện tại của Wikipedia phải khớp dòng cuối đã dựng; lệch thì ghi warn.

## Lịch

Hằng tuần, thứ Hai 02:00 UTC.

## Done when

- Dựng được thành phần S&P 500 cho mọi ngày từ 2005, mỗi ngày ~500 mã (±5).
- Danh sách mã từng thuộc S&P 500 nhưng Yahoo/Alpaca không có giá được xuất ra file báo cáo.
