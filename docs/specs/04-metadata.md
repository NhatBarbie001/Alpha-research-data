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

Năm bảng trong DB `alphadata_meta`. Cột, kiểu, khoá, index ở [04-data-model.md](../04-data-model.md) mục 7.

| Bảng | Một dòng là | Dùng để |
|---|---|---|
| `ingestion_runs` | Một lần `run_partition` | Lịch sử chạy, freshness, debug |
| `raw_files` | Một file raw đã tải | Truy về URL gốc và checksum |
| `dq_results` | Kết quả một check | Báo cáo DQ, alert |
| `table_catalog` | Một (bảng, nguồn) | Trả lời "số này đến từ đâu" |
| `backfill_checkpoints` | Một partition trong một lần backfill | Resume backfill |

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
