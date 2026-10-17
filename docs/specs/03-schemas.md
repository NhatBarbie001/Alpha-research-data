# Spec: `schemas`

> Code: `src/alphadata/schemas/` · Tầng: xuyên suốt · Phase 0 (silver), Phase 2 (reference), Phase 3 (gold)
> Phụ thuộc: `core`

## Mục đích

Đưa thiết kế bảng trong [04-data-model.md](../04-data-model.md) vào code, để mọi module dùng chung một định nghĩa: connector biết phải trả ra cột gì, `storage` từ chối ghi DataFrame sai schema, `quality` kiểm tra enum và null, `access` biết tạo view nào.

Thiết kế bảng (cột, kiểu, khoá, partition, quan hệ) **chỉ được sửa ở [04-data-model.md](../04-data-model.md)**, rồi cập nhật code trong module này cho khớp. File này không lặp lại danh sách bảng.

## File

| File | Nội dung |
|---|---|
| `types.py` | `TableSchema`, `Column`, ánh xạ kiểu logic sang Arrow |
| `silver.py` | `TableSchema` của 14 bảng silver |
| `reference.py` | `TableSchema` của 5 bảng reference |
| `gold.py` | `TableSchema` của 5 bảng gold |
| `enums.py` | Giá trị hợp lệ của các cột enum (mục 8 của data model) |
| `registry.py` | `get_schema(name)`, `list_schemas(layer=None)` |

Bảng metadata Postgres không nằm ở đây mà ở `metadata/tables.py` (SQLAlchemy).

## Định nghĩa

```python
@dataclass(frozen=True)
class Column:
    name: str
    type: str                  # kiểu logic: "string", "int64", "float64", "bool", "date", "timestamp", "list<string>"
    nullable: bool = False
    enum: str | None = None    # tên enum trong enums.py

@dataclass(frozen=True)
class TableSchema:
    name: str
    layer: str                 # "silver" | "reference" | "gold"
    columns: list[Column]      # không gồm cột bắt buộc của tầng (source, ingested_at / built_at): tự thêm
    key: list[str]
    partition: list[str]       # cột tạo thư mục k=v; có thể là cột suy ra (year, month từ ts_utc)
    sort: list[str]
    version: int = 1           # tăng khi thêm cột, ghi vào table_catalog.schema_version

    def arrow_schema(self) -> pa.Schema: ...
    def path_pattern(self) -> str: ...      # ví dụ "silver/crypto_bars_1m/symbol=*/year=*/month=*/part.parquet"
```

## Hành vi

- `conform(df, schema) -> DataFrame`: sắp xếp cột theo thứ tự trong schema, ép kiểu, thêm cột partition suy ra. Thiếu cột không null hoặc kiểu không ép được thì raise `SchemaMismatch`. Cột thừa thì raise (không âm thầm bỏ).
- Pipeline gọi `conform` trước, rồi `storage.write_parquet_atomic(df, path, schema=table.arrow_schema())`. `storage` kiểm lại hợp đồng bằng Arrow schema nhưng không import `schemas` (đúng hướng phụ thuộc `storage ◄─ schemas`). `SchemaMismatch` nằm trong `core.errors` để hai module dùng chung.
- `quality.schema_match` dùng `schema` để kiểm tra null và enum.
- `access.views` dùng `path_pattern()` để tạo view DuckDB cho mọi bảng có file.

## Done when

- Mọi bảng trong [04-data-model.md](../04-data-model.md) mục 4, 5, 6 có `TableSchema`.
- Có test sinh bảng markdown (cột, kiểu, null, khoá) từ code và so với data model; lệch thì test fail.
- `conform` có test cho thiếu cột, thừa cột, sai kiểu, enum lạ.
