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
- `fetch_date` là ngày UTC lúc tải. Tải lại cùng partition vào ngày khác thì tạo thư mục mới, không ghi đè raw cũ. Tải lại trong cùng ngày thì ghi đè (atomic).
- Partition của silver do từng dataset quyết định (xem [03-schemas.md](03-schemas.md)). Quy tắc chung: minute theo `symbol/year/month`, daily theo `symbol`, macro theo `source/series_id`. Bảng reference nằm dưới `gold/<bảng>/part.parquet`, không partition.

```python
raw_dir(settings, source, dataset, partition, *, partition_keys, fetch_date) -> Path
table_file(settings, layer: "silver" | "gold", table, partition=None) -> Path   # .../part.parquet
table_dir(settings, layer, table) -> Path
checkpoint_file(settings, dataset, backfill_id) -> Path
```

**An toàn đường dẫn** (giá trị partition có thể đến từ CLI):
- Tên `source`, `dataset`, `table`, khoá partition phải là `snake_case` chữ thường; sai thì `ValueError`.
- Giá trị partition được mã hoá `%XX` cho mọi ký tự ngoài chữ, số, `_.-~` (ví dụ `EURUSD=X` → `EURUSD%3DX`, `^GSPC` → `%5EGSPC`), nên `/` hay `..` không thể thoát ra khỏi thư mục bảng. Giá trị rỗng, `.`, `..` bị từ chối. Ngày ghi dạng ISO. Cột `symbol` trong file vẫn giữ giá trị gốc; đường dẫn chỉ dùng để lọc file.

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
read_raw(path) -> bytes          # kiểm sha256 với manifest, sai thì RawChecksumError
read_manifest(path) -> RawFile
delete_raw(path) -> None         # xoá file, giữ manifest; gọi lại không lỗi
```

Manifest là JSON cạnh file (`<file>.manifest.json`), gồm đúng các trường của `RawFile` (với `path` là tên file). File và manifest đều ghi atomic (file tạm, fsync, `os.replace`). `save_raw` che key trong `url` thêm một lần (`core.http.mask_secrets`) dù tầng trên đã che, vì manifest là chỗ dễ lộ key nhất. `filename` phải là tên file đơn, không có `/`, không bắt đầu bằng `.`.

`normalize` đọc raw qua `read_raw`, không đọc file trực tiếp: chạy lại ETL từ raw đã lưu mà file bị hỏng thì phải báo lỗi, không âm thầm ra silver sai.

Nếu nguồn có checksum riêng (Binance `.CHECKSUM`) thì connector so sánh trước khi gọi `save_raw`, sai thì raise lỗi.

## Parquet

```python
read_parquet_if_exists(path) -> DataFrame | None
write_parquet_atomic(df, path, *, schema: pa.Schema) -> None
upsert(existing: DataFrame | None, new, key_cols) -> DataFrame
```

- Ghi ra `<path>.tmp`, fsync, rồi `os.replace`, để file không bao giờ bị ghi dở. Lỗi giữa chừng thì file cũ còn nguyên và file tạm bị xoá.
- Nén `zstd` mức 9, bật `byte_stream_split` cho cột số thực (tắt dictionary cho các cột này), dictionary cho cột còn lại, row group 1 triệu dòng, có thống kê min/max ([04-data-model.md](../04-data-model.md) mục 9.1).
- **Hợp đồng bảng:** `schema` là bắt buộc, lấy từ `TableSchema.arrow_schema()`. `storage` không import `schemas` (sai hướng phụ thuộc), nên nhận Arrow schema thay vì `TableSchema`. Thiếu cột, thừa cột, kiểu không ép được, hoặc null ở cột không null thì raise `SchemaMismatch`, không ghi gì. Việc sắp cột, thêm cột suy ra là của `schemas.conform`, chạy trước bước này.
- `upsert`: nối cũ và mới, trùng khoá giữ bản mới (trùng ngay trong `new` thì giữ dòng sau), sắp xếp theo khoá, index đánh lại từ 0.
- Một file Parquet không vượt quá ~1 tháng minute của một mã, để merge không ngốn RAM.

## Disk guard

`check_disk(settings) -> float`: trả số GB trống của phân vùng chứa `data_root` (chưa tạo thì đo ở thư mục cha gần nhất). Dưới `min_free_gb` thì raise `DiskFullError`. Gọi ở đầu mỗi `run_partition` và trước mỗi bước backfill.

## Done when

- Test ghi atomic: giả lập lỗi giữa chừng, file cũ còn nguyên, không có file hỏng.
- Test `upsert` giữ bản mới khi trùng khoá.
- Test `check_disk` với ngưỡng cao hơn disk trống thì raise.
- Test giá trị partition như `../../etc` không thoát khỏi thư mục bảng; manifest không chứa key.
