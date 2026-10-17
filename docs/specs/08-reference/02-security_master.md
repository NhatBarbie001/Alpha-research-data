# Spec: `reference.security_master`

> Code: `src/alphadata/reference/security_master/` · Tầng: reference (bảng gold) · Phase 2
> Phụ thuộc: `storage`, `schemas`, silver `listing_snapshots`, `index_constituent_*`, `equity_corporate_actions`, `equity_bars_1d`

## Mục đích

Gán mỗi chứng khoán một `security_id` nội bộ **không đổi theo thời gian**, và lưu lịch sử ticker, sàn, trạng thái niêm yết của nó. Ticker không dùng làm khoá được vì ticker đổi (FB → META) và được tái sử dụng (mã của công ty đã hủy niêm yết được gán cho công ty khác).

## Bảng

| Bảng | Một dòng là |
|---|---|
| `security_master` | Một chứng khoán, khoá `security_id` |
| `security_symbols` | Một ticker của một chứng khoán theo một nguồn trong một khoảng thời gian |

Cột, kiểu, ràng buộc ở [04-data-model.md](../../04-data-model.md) mục 5. Hai ràng buộc quan trọng nhất:
- `security_id` không bao giờ tái sử dụng và không đổi khi dựng lại: job đọc bảng cũ, giữ id đã cấp, chỉ cấp id mới cho chứng khoán mới.
- Với cùng (`symbol_source`, `symbol`), các khoảng hiệu lực không chồng nhau, nên một ticker tại một ngày chỉ ra đúng một `security_id`.

## Quy tắc dựng

1. **Ticker chuẩn (`canonical`)**: dấu chấm cho class cổ phiếu (`BRK.B`). Ánh xạ sang định dạng của từng nguồn: Yahoo dùng gạch ngang (`BRK-B`), Alpaca dùng dấu chấm. Crypto dùng nguyên tên cặp Binance.
2. **Equity Mỹ có CIK**: một CIK + một class cổ phiếu = một `security_id`. Đổi ticker cùng CIK thì thêm dòng mới vào `security_symbols`, giữ `security_id`.
3. **Không có CIK** (ETF, mã cũ chỉ thấy trong lịch sử S&P 500): ghép theo ticker + khoảng thời gian không chồng lấn. Ticker biến mất rồi xuất hiện lại sau hơn 30 ngày với tên khác → coi là chứng khoán mới.
4. **Đổi tên mã** lấy từ `equity_corporate_actions` (`symbol_change`) và từ chênh lệch giữa hai snapshot `listing_snapshots` liên tiếp (mã cũ biến mất, mã mới xuất hiện cùng CIK).
5. Trường hợp không tự quyết được thì ghi vào file `config/reference/security_overrides.yaml` (sửa tay có review, là ngoại lệ duy nhất cho nguyên tắc "không sửa tay"). File này có ghi chú lý do cho từng dòng.

## API

```python
resolve(symbol, on_date, symbol_source="canonical") -> int | None
symbols_of(security_id, on_date) -> dict[str, str]     # {nguồn: ticker}
```

## Hạn chế

Không có nguồn miễn phí nào cho lịch sử ticker đầy đủ trước khi bắt đầu chụp snapshot. Lịch sử trước đó dựng từ thành phần S&P 500 và CIK, nên các mã ngoài S&P 500 có thể bị ghép sai. Ghi rõ trong catalog.

## Done when

- META và FB cùng một `security_id`.
- Có các mã đã hủy niêm yết (ví dụ từng thuộc S&P 500 rồi phá sản hoặc bị mua lại) với `status = delisted`.
- `resolve` có test cho mã đổi tên và mã bị tái sử dụng.
