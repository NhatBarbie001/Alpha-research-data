# Spec: `storage`

> Code: `src/alphadata/storage/` · Tầng: xuyên suốt · Phase 0
> Phụ thuộc: `core`

## Mục đích

Mọi thao tác đọc ghi file của lake đi qua module này: bố cục thư mục, lưu raw kèm manifest, ghi Parquet an toàn, gộp dữ liệu, kiểm tra disk. Không module nào khác tự ghép đường dẫn hay gọi `pq.write_table`.

## File

| File | Nội dung |
|---|---|
| `layout.py` | Hàm sinh đường dẫn raw, silver, gold, checkpoint |
| `raw.py` | `RawFile`, `save_raw()`, `delete_raw()` |
| `parquet.py` | `read_parquet_if_exists()`, `write_parquet_atomic()`, `upsert()` |
| `disk.py` | `check_disk()` |

## Bố cục

```
<data_root>/
├── raw/<source>/<dataset>/<k1=v1>/<k2=v2>/<fetch_date>/<file>
│                                                      └── <file>.manifest.json
├── silver/<table>/<k=v>/.../part.parquet
├── gold/<table>/<k=v>/.../part.parquet
└── checkpoints/<dataset>/<backfill_id>.json
```

- Thứ tự `k=v` trong raw: các `partition_keys` của dataset theo thứ tự khai báo, sau đó các khoá còn lại theo alphabet.
- `fetch_date` là ngày UTC lúc tải. Tải lại cùng partition vào ngày khác thì tạo thư mục mới, không ghi đè raw cũ.
- Partition của silver do từng dataset quyết định (xem [schemas.md](schemas.md)). Quy tắc chung: minute theo `symbol/year/month`, daily theo `symbol`, macro theo `source/series_id`.

## Raw

```python
@dataclass
class RawFile:
    path: Path
    url: str          # đã che key
    sha256: str
    bytes: int
    fetched_at: str   # ISO UTC

save_raw(raw_dir, content: bytes, filename, url) -> RawFile
```

Manifest là JSON cạnh file, gồm đúng các trường của `RawFile` (với `path` là tên file). Nếu nguồn có checksum riêng (Binance `.CHECKSUM`) thì connector so sánh trước khi gọi `save_raw`, sai thì raise lỗi.

## Parquet

```python
write_parquet_atomic(df, path) -> None
upsert(existing, new, key_cols) -> DataFrame
```

- Ghi ra `<path>.tmp` rồi `os.replace`, để file không bao giờ bị ghi dở.
- Nén `zstd` mức 9, bật `byte_stream_split` cho cột số thực (nhỏ hơn ~20% so với mặc định, đo trên nến 1 phút).
- Trước khi ghi, ép kiểu theo hợp đồng trong `schemas` (thiếu cột hoặc sai kiểu thì raise).
- `upsert`: nối cũ và mới, trùng khoá giữ bản mới, sắp xếp theo khoá.
- Một file Parquet không vượt quá ~1 tháng minute của một mã, để merge không ngốn RAM.

## Disk guard

`check_disk(settings) -> float`: trả số GB trống của phân vùng chứa `data_root`. Dưới `min_free_gb` thì raise `DiskFullError`. Gọi ở đầu mỗi `run_partition` và trước mỗi bước backfill.

## Done when

- Test ghi atomic: giả lập lỗi giữa chừng, file cũ còn nguyên, không có file hỏng.
- Test `upsert` giữ bản mới khi trùng khoá.
- Test `check_disk` với ngưỡng cao hơn disk trống thì raise.
