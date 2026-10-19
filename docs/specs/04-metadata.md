# Spec: `metadata`

> Code: `src/alphadata/metadata/` · Tầng: xuyên suốt · Phase 0
> Phụ thuộc: `core`, `storage`

## Mục đích

Lưu metadata vận hành để trả lời: lần chạy nào, tải file nào, check nào fail, bảng nào lấy từ nguồn nào. Chạy trên SQLite ở máy dev và Postgres trên server (SQLAlchemy Core, không dùng ORM).

## File

| File | Nội dung |
|---|---|
| `tables.py` | Định nghĩa bảng (SQLAlchemy Core), giá trị enum hợp lệ |
| `schema.py` | `init_schema()`: tạo bảng hoặc chạy migration |
| `migrations/` | File `NNNN_mo_ta.sql` đưa DB cũ lên schema mới (nằm trong package để venv Airflow có sẵn) |
| `store.py` | `MetaStore`: các hàm ghi và đọc |
| `checkpoint.py` | `Checkpoints`: trạng thái backfill |

## Bảng

Năm bảng trong DB `alphadata_meta`. Cột, kiểu, khoá, index ở [04-data-model.md](../04-data-model.md) mục 7.

| Bảng | Một dòng là | Dùng để |
|---|---|---|
| `ingestion_runs` | Một lần `run_partition` | Lịch sử chạy, freshness, debug |
| `raw_files` | Một file raw đã tải | Truy về URL gốc và checksum |
| `dq_results` | Kết quả một check | Báo cáo DQ, alert |
| `table_catalog` | Một (bảng, nguồn) | Trả lời "số này đến từ đâu" |
| `backfill_checkpoints` | Một partition trong một lần backfill | Resume backfill |

Thêm bảng kỹ thuật `schema_migrations` (version, applied_at) ghi migration đã áp dụng.

Kiểu dữ liệu: `uuid` dùng `sa.Uuid` (Postgres `uuid`, SQLite text); `jsonb` dùng `JSON` với biến thể `JSONB` trên Postgres; `timestamptz` dùng `DateTime(timezone=True)`. SQLite trả datetime không có timezone nên mọi hàm đọc gắn lại UTC. SQLite bật `PRAGMA foreign_keys=ON` để khoá ngoại có hiệu lực như trên Postgres.

## Khởi tạo và thay đổi schema

`MetaStore(url)` gọi `init_schema` trong một transaction:

- Trên Postgres, lấy `pg_advisory_xact_lock` trước, vì nhiều task Airflow chạy song song cùng khởi tạo sẽ đụng nhau.
- **DB mới** (chưa có `schema_migrations`): `create_all` từ `tables.py`, rồi ghi mọi file migration hiện có là đã áp dụng. `tables.py` luôn là schema mới nhất.
- **DB đã có**: chạy lần lượt các file migration chưa áp dụng, ghi vào `schema_migrations`.

Thay đổi schema: sửa data model mục 7, sửa `tables.py`, thêm file migration (xem `migrations/README.md`). Không sửa bảng tay trên server. Với SQLite (máy dev), file DB nằm trong `data_root` và tự tạo thư mục cha.

## Hàm ghi

Mỗi hàm là một transaction riêng; lỗi không bị nuốt.

```python
start_run(source, dataset, partition, trigger, *, airflow_run_id=None, backfill_id=None) -> UUID
record_raw(run_id, raw: storage.RawFile) -> int
record_dq(table_name, results: Iterable[CheckLike], run_id=None) -> int
upsert_catalog(entry: CatalogEntry) -> None        # INSERT ... ON CONFLICT DO UPDATE theo (table_name, source)
mark_raw_deleted(run_id) -> int
finish_run(run_id, status, *, rows_in=None, rows_out=None, silver_path=None, error=None) -> None
```

- `trigger`, `status`, `severity`, `layer` được kiểm theo enum ở data model mục 8; sai thì `ValueError`. `finish_run` không nhận `running`; `run_id` không tồn tại thì `KeyError`.
- `record_dq` nhận object bất kỳ có `name`, `severity`, `passed`, `detail` (protocol `CheckLike`), nên `metadata` không cần import `quality` (sai hướng phụ thuộc).
- **Che key:** `record_raw` che key trong URL thêm một lần; `finish_run` và `Checkpoints.mark` che key trong message lỗi (message thường chứa URL) rồi cắt còn 4000 ký tự.
- `partition` được chuẩn hoá: key sắp theo alphabet, ngày dạng ISO. `{"date": date(2026, 10, 11)}` và `{"date": "2026-10-11"}` là cùng một partition.

## Hàm đọc (dùng cho CLI và alert)

```python
get_run(run_id) -> RunRecord
recent_runs(limit=20, dataset=None, status=None) -> list[RunRecord]   # mới nhất trước
raw_files_for(run_id) -> list[dict]
catalog(layer=None) -> list[CatalogEntry]
failed_checks(since) -> list[DqRecord]
last_success(dataset, partition) -> datetime | None                    # dùng cho check freshness
```

`last_success` so khớp đúng partition: trên Postgres so `jsonb`, trên SQLite so chuỗi JSON đã chuẩn hoá.

## Checkpoint backfill

```python
cp = Checkpoints(store)
cp.add(backfill_id, dataset, partitions) -> int    # số partition mới; partition đã có giữ nguyên trạng thái
cp.remaining(backfill_id) -> list[dict]           # pending + failed, thứ tự ổn định
cp.mark(backfill_id, partition, "done" | "failed", error=None)   # tăng attempts
cp.summary(backfill_id) -> dict[status, count]
```

`partition_hash` là SHA-256 của partition đã chuẩn hoá. Lập lại kế hoạch với cùng `backfill_id` không làm mất tiến độ.

## Test

- Mọi test của module chạy trên SQLite và, khi có biến `ALPHADATA_TEST_PG_URL`, trên Postgres thật. CI chạy Postgres 16 bằng service container. Fixture chỉ chịu xoá DB có tên kết thúc bằng `_test`.
- `test_schema.py` so `tables.py` với data model mục 7 (tên cột, thứ tự, kiểu, null).
- `test_postgres_concurrency.py`: 4 process cùng khởi tạo `MetaStore` trên DB trống và cùng ghi catalog 30 lần; không lỗi, còn đúng 1 dòng.

## Done when

- Test toàn bộ vòng đời một run trên SQLite.
- Nhiều process ghi catalog cùng lúc trên Postgres không lỗi (tự động trong CI).
