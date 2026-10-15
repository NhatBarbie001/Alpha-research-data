# Spec: `metadata`

> Code: `src/alphadata/metadata/` · Tầng: xuyên suốt · Phase 0
> Phụ thuộc: `core`, `storage`

## Mục đích

Lưu metadata vận hành để trả lời: lần chạy nào, tải file nào, check nào fail, bảng nào lấy từ nguồn nào. Chạy trên SQLite ở máy dev và Postgres trên server (SQLAlchemy Core, không dùng ORM).

## File

| File | Nội dung |
|---|---|
| `tables.py` | Định nghĩa bảng |
| `store.py` | `MetaStore`: các hàm ghi và đọc |
| `checkpoint.py` | Trạng thái backfill |

## Bảng

**`ingestion_runs`**: một dòng cho mỗi lần `run_partition`.

| Cột | Kiểu | Ghi chú |
|---|---|---|
| run_id | uuid PK | |
| source, dataset | text | |
| partition | text (JSON, sort keys) | |
| trigger | text | `cli`, `airflow`, `backfill` |
| airflow_run_id | text null | |
| started_at, finished_at | timestamptz | |
| status | text | `running`, `success`, `failed`, `skipped_not_available` |
| rows_in, rows_out | int | Dòng mới / tổng dòng của file silver sau merge |
| silver_path | text | |
| error | text | Tối đa 4000 ký tự |

**`raw_files`**: `id`, `run_id`, `path`, `url` (đã che key), `sha256`, `bytes`, `fetched_at`, `deleted` (bool).

**`dq_results`**: `id`, `run_id` (null với check sau khi ghi), `table_name`, `check_name`, `severity` (`error` / `warn`), `passed`, `detail`, `checked_at`.

**`table_catalog`**: khoá (`table_name`, `source`). Cột: `layer` (`silver` / `gold`), `dataset`, `description`, `source_url`, `license_note`, `key_cols`, `path_pattern`, `refresh_schedule`, `notes`, `updated_at`. Đây là chỗ trả lời câu hỏi "số này đến từ đâu".

**`backfill_checkpoints`**: `backfill_id`, `dataset`, `partition` (JSON), `status` (`pending` / `done` / `failed`), `attempts`, `last_error`, `updated_at`.

## Hành vi

- Tạo bảng khi khởi tạo `MetaStore`. Trên Postgres, khoá `pg_advisory_xact_lock` khi tạo bảng, vì nhiều task Airflow chạy song song cùng tạo sẽ đụng nhau.
- Ghi catalog bằng `INSERT ... ON CONFLICT DO UPDATE`.
- Mỗi hàm ghi là một transaction riêng. Lỗi ghi metadata không được nuốt im lặng.
- Thay đổi schema bảng metadata: thêm file migration SQL có số thứ tự trong `deploy/migrations/`, không sửa `create_all` rồi bỏ qua dữ liệu cũ.

## Hàm đọc (dùng cho CLI và alert)

- `recent_runs(limit, dataset=None, status=None)`
- `catalog(layer=None)`
- `failed_checks(since)`
- `last_success(dataset, partition)`: dùng cho check freshness.

## Done when

- Test toàn bộ vòng đời một run trên SQLite.
- Hai process ghi catalog cùng lúc trên Postgres không lỗi (test thủ công trên Docker).
