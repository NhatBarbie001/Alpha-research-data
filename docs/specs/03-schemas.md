# Spec: `schemas`

> Code: `src/alphadata/schemas/` · Tầng: xuyên suốt · Phase 0 (silver), Phase 2 (reference), Phase 3 (gold)
> Phụ thuộc: `core`

## Mục đích

Đưa thiết kế bảng trong [04-data-model.md](../04-data-model.md) vào code, để mọi module dùng chung một định nghĩa: connector biết phải trả ra cột gì, `storage` từ chối ghi DataFrame sai schema, `quality` kiểm tra enum và null, `access` biết tạo view nào.

Thiết kế bảng (cột, kiểu, khoá, partition, quan hệ) **chỉ được sửa ở [04-data-model.md](../04-data-model.md)**, rồi cập nhật code trong module này cho khớp. File này không lặp lại danh sách bảng.

## File

| File | Nội dung |
|---|---|
| `types.py` | `TableSchema`, `Column`, ánh xạ kiểu logic sang Arrow, cột bắt buộc theo tầng |
| `conform.py` | `conform()`, `split_partitions()` |
| `silver.py` | `TableSchema` của các bảng silver |
| `reference.py` | `TableSchema` của các bảng reference (thêm ở phase 2) |
| `gold.py` | `TableSchema` của các bảng gold (thêm ở phase 3) |
| `enums.py` | Giá trị hợp lệ của các cột enum (mục 8 của data model) |
| `registry.py` | `get_schema(name)`, `list_schemas(layer=None)` |

Bảng metadata Postgres không nằm ở đây mà ở `metadata/tables.py` (SQLAlchemy).

**Thêm bảng theo tiến độ:** mỗi bảng được thêm vào code ở task dùng nó đầu tiên (ví dụ `crypto_bars_1m` ở P0-04, `equity_bars_1m` ở P1-02), không viết trước. Test so khớp tự kiểm mọi bảng có trong code.

## Định nghĩa

```python
LogicalType = Literal["string", "int32", "int64", "float64", "bool", "date", "timestamp", "list<string>"]

@dataclass(frozen=True)
class Column:
    name: str
    type: LogicalType
    nullable: bool = False
    enum: str | None = None          # tên enum trong enums.py

@dataclass(frozen=True)
class TableSchema:
    name: str
    layer: "silver" | "reference" | "gold"
    columns: tuple[Column, ...]      # không gồm cột bắt buộc của tầng: tự thêm vào cuối
    key: tuple[str, ...]
    partition: tuple[str, ...] = ()  # cột thật (symbol) hoặc cột suy ra (year, month)
    sort: tuple[str, ...] = ()
    partition_time: str | None = None  # cột date/timestamp để suy ra year, month
    version: int = 1                 # tăng khi thêm cột, ghi vào table_catalog.schema_version

    all_columns -> tuple[Column, ...]          # columns + cột bắt buộc của tầng
    storage_layer -> "silver" | "gold"         # reference nằm dưới gold/
    def arrow_schema(self) -> pa.Schema: ...
    def path_pattern(self) -> str: ...         # "silver/crypto_bars_1m/symbol=*/year=*/month=*/part.parquet"
```

Cột bắt buộc của tầng: silver thêm `source` (string), `ingested_at` (timestamp); reference và gold thêm `built_at` (timestamp). Khai báo lại các cột này trong `columns` là lỗi.

`TableSchema` tự kiểm khi khởi tạo (sai thì `ValueError` ngay lúc import): tên cột không trùng; `key`, `sort` chỉ gồm cột có thật; `partition` là cột có thật hoặc `year`/`month` kèm `partition_time` là cột `date`/`timestamp`; enum có trong `enums.py`.

## Hành vi

`conform(df, schema) -> DataFrame` đưa DataFrame về đúng hợp đồng, sai thì raise `SchemaMismatch` (trong `core.errors`):

| Trường hợp | Xử lý |
|---|---|
| Cột thừa | Raise, không âm thầm bỏ |
| Thiếu cột không null | Raise |
| Thiếu cột nullable | Thêm cột toàn null |
| Null ở cột không null | Raise |
| `string` | Mọi giá trị phải là `str` (số không tự đổi thành chuỗi) |
| `int32`, `int64` | Nhận số nguyên, số thực không có phần lẻ, chuỗi số; từ chối bool và số có phần lẻ. Cột nullable dùng `Int64`/`Int32` của pandas |
| `float64` | Nhận số và chuỗi số; từ chối bool |
| `bool` | Chỉ nhận bool |
| `timestamp` | Phải có timezone; đổi sang UTC, độ chính xác micro-giây. Datetime không có timezone bị từ chối, vì không biết là giờ nào |
| `date` | Nhận `datetime.date` hoặc datetime không timezone đúng nửa đêm; từ chối datetime có giờ hoặc có timezone |
| `list<string>` | Mỗi giá trị là list/tuple các `str`, hoặc null |
| Enum | Giá trị ngoài danh sách thì raise |

Kết quả có cột theo đúng thứ tự `arrow_schema()`.

`split_partitions(df, schema) -> list[(partition: dict, frame)]` chia DataFrame đã conform theo `partition`, mỗi phần sắp theo `sort` (không có thì theo `key`). `year`, `month` tính từ `partition_time` theo UTC. Giá trị partition là kiểu Python thuần (`str`, `int`), dùng thẳng cho `storage.layout.table_file`. **Cột suy ra không được thêm vào frame**: data model quy định chúng chỉ nằm trong đường dẫn. Cột partition thật (như `symbol`) vẫn nằm trong file.

Luồng ghi của pipeline:

```python
df = conform(df, schema)
for partition, frame in split_partitions(df, schema):
    path = layout.table_file(settings, schema.storage_layer, schema.name, partition)
    merged = upsert(read_parquet_if_exists(path), frame, schema.key)
    write_parquet_atomic(merged, path, schema=schema.arrow_schema())
```

- `storage` kiểm lại hợp đồng bằng Arrow schema nhưng không import `schemas` (đúng hướng phụ thuộc `storage ◄─ schemas`).
- `quality.schema_match` dùng `schema` để kiểm tra null và enum.
- `access.views` dùng `path_pattern()` để tạo view DuckDB cho mọi bảng có file.

## Test so khớp với data model

`tests/schemas/test_contract.py` đọc thẳng [04-data-model.md](../04-data-model.md) và so với code, cho mọi bảng trong `list_schemas()`:

- Cột: tên, kiểu, null, **thứ tự**, lấy từ bảng `| Cột | Kiểu | Null |` ngay dưới tiêu đề **`tên_bảng`** (cột bắt buộc của tầng được bỏ qua nếu data model có liệt kê).
- Khoá, partition, sắp xếp: lấy từ bảng tóm tắt `| Bảng | Khoá | Partition | Sắp xếp trong file |` của tầng.
- Tầng: theo mục chứa bảng (4 silver, 5 reference, 6 gold).
- Enum: giá trị trong `enums.py` so với mục 8.

Lệch thì test fail và in bảng markdown sinh từ code cạnh bảng trong data model. Bảng nào trong data model đang viết dạng rút gọn một dòng (ví dụ `crypto_funding_rates`) phải được chuyển sang dạng bảng `| Cột | Kiểu | Null |` khi thêm `TableSchema` của nó.

## Done when

- Bảng của mỗi task có `TableSchema` khi task đó xong; đến hết phase 3, mọi bảng trong [04-data-model.md](../04-data-model.md) mục 4, 5, 6 có `TableSchema`.
- Test so khớp với data model chạy trong CI; sửa một kiểu trong code thì test fail.
- `conform` có test cho thiếu cột, thừa cột, sai kiểu, enum lạ.
