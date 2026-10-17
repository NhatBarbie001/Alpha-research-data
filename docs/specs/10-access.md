# Spec: `access`

> Code: `src/alphadata/access/` · Tầng: access · Phase 3 (view silver cho debug có từ Phase 0)
> Phụ thuộc: `gold.*`, `reference.*`, `storage`

## Mục đích

Lớp duy nhất agent dùng để đọc data. Read-only, `as_of` bắt buộc, để không ai vô tình lấy dữ liệu tương lai.

## File

| File | Nội dung |
|---|---|
| `views.py` | `connect()`: mở DuckDB, tạo view cho mọi bảng silver và gold có file |
| `api.py` | `get_bars`, `get_macro`, `get_fundamentals`, `get_universe`, `get_factors` |

## API

```python
get_bars(symbols_or_ids, start, end, freq="1d", *, as_of, adjusted=True, sessions=("regular",)) -> DataFrame
get_macro(series_keys, start, end, *, as_of) -> DataFrame
get_fundamentals(symbols_or_ids, concepts, *, as_of) -> DataFrame
get_universe(universe_code, *, as_of, tradable_only=True) -> list[int]
get_coverage(universe_code, start, end, *, as_of) -> DataFrame
get_factors(dataset, start, end, *, as_of) -> DataFrame
```

| Tham số | Quy tắc |
|---|---|
| `as_of` | **Bắt buộc**, không có mặc định. Kiểu ngày hoặc timestamp UTC |
| `end` | Bị cắt về `as_of` nếu lớn hơn, kèm warning |
| `symbols_or_ids` | Nhận ticker (resolve qua `security_master` tại `as_of`) hoặc `security_id` |
| `freq` | `1min`, `5min`, `15min`, `1h`, `1d`. Tần suất trên 1 phút được **tính từ 1 phút khi query** (DuckDB `time_bucket`), không lưu sẵn |
| `adjusted` | Nhân `adj_factor`. Docstring ghi rõ: giá điều chỉnh dùng cho lợi nhuận, không dùng làm mức giá PIT (xem [09-gold/01-bars.md](09-gold/01-bars.md)) |
| `sessions` | Chỉ áp cho minute US |

Resample không tự điền nến trống. Khoảng không có giao dịch thì không có dòng.

### Cảnh báo thiếu giá (survivorship)

Nguồn miễn phí thiếu giá của một số mã đã hủy niêm yết (xem [09-gold/02-universe_daily.md](09-gold/02-universe_daily.md) mục "Độ phủ giá và survivorship"). API không giấu chuyện này:

- `get_universe(..., tradable_only=True)` chỉ trả mã có giá, nên mã thiếu giá bị loại **ngầm**. Khi universe tại `as_of` có mã `has_price = false`, hàm phát `SurvivorshipWarning` (lớp con của `UserWarning`) ghi số mã thiếu và tỷ lệ, ví dụ `SP500 at 2008-09-15: 7/500 members have no price (1.4%)`.
- `tradable_only=False` trả đủ thành viên, kể cả mã thiếu giá, để agent tự quyết cách xử lý.
- `get_coverage` trả một dòng cho mỗi năm trong `[start, end]` (bị cắt về `as_of`): `year`, `member_days`, `priced_days`, `coverage`. Kết quả backtest nên kèm con số này.
- Docstring của `get_universe` và `get_bars` ghi rõ giới hạn này.

## Truy cập SQL

- Agent được chạy SQL tự do, nhưng chỉ trên view gold, qua kết nối DuckDB `read_only` và không có quyền ghi file.
- View silver chỉ dành cho CLI và debug của team.

## Hiệu năng

- DuckDB `memory_limit = '2GB'`, `threads = 2` trên server bee.
- Query minute phải lọc theo `symbol` và thời gian để DuckDB bỏ qua file Parquet không liên quan (nhờ partition `symbol/year/month`).

## Done when

- Test chứng minh `get_macro(..., as_of=t)` không bao giờ trả dòng có `available_from > t`.
- Test chứng minh gọi `get_*` không truyền `as_of` thì lỗi.
- Test: universe fixture có mã thiếu giá thì `get_universe` phát `SurvivorshipWarning`; `tradable_only=False` trả cả mã đó.
- `get_bars(["BTCUSDT"], ..., freq="1h")` cho kết quả khớp với tự resample bằng pandas.
