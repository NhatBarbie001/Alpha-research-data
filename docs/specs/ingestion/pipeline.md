# Spec: `ingestion.pipeline`

> Code: `src/alphadata/ingestion/pipeline/` · Tầng: ingestion · Phase 0
> Phụ thuộc: `core`, `storage`, `metadata`, `quality`, `schemas`, `universe`

## Mục đích

Khung chung để chạy mọi nguồn: hợp đồng `Dataset` mà mỗi connector phải tuân theo, hàm `run_partition` điều phối một lần chạy, backfill có checkpoint, và registry tra dataset theo tên. Được gọi từ cả CLI và Airflow, không import Airflow.

## File

| File | Nội dung |
|---|---|
| `dataset.py` | `Dataset` (abstract), `FetchContext` |
| `run.py` | `run_partition()`, `RunResult` |
| `backfill.py` | `plan_backfill()`, `run_backfill()` |
| `registry.py` | `register()`, `get_dataset(name)`, `list_datasets()` |

## Hợp đồng `Dataset`

Mỗi dataset ghi vào **đúng một bảng silver**. Một nguồn có nhiều bảng thì có nhiều dataset.

```python
class Dataset(ABC):
    name: str                  # id duy nhất, dùng trong CLI, universe, schedules
    source: str                # ghi vào cột `source`
    table: str                 # tên bảng silver, phải có trong schemas
    description: str
    source_url: str
    license_note: str          # tóm tắt điều khoản, ghi vào catalog
    partition_keys: list[str]  # ví dụ ["symbol", "date"]
    keep_raw: bool = True      # False: xoá raw sau khi ghi silver (giữ manifest)
    requires: list[str] = []   # tên secret cần có, ví dụ ["FRED_API_KEY"]

    @abstractmethod
    def fetch(self, ctx: FetchContext, partition: dict) -> list[RawFile]: ...
    @abstractmethod
    def normalize(self, raws: list[RawFile], partition: dict) -> pd.DataFrame: ...
    @abstractmethod
    def silver_path(self, partition: dict) -> Path: ...

    def checks(self, df, partition) -> list[CheckResult]: ...   # mặc định: not_empty, no_null_keys, unique_keys, schema_match
    def merge(self, existing, new) -> pd.DataFrame: ...          # mặc định: storage.upsert theo khoá của schema
    def backfill_partitions(self, member, start, end) -> list[dict]: ...  # chia lịch sử thành các partition
```

Quy tắc cho connector:
- `fetch` chỉ tải và lưu nguyên bản qua `storage.save_raw`. Không parse, không biến đổi.
- `normalize` chỉ đọc từ raw đã lưu, không gọi mạng. Nhờ vậy chạy lại ETL được mà không cần crawl lại.
- Nguồn chưa có dữ liệu thì raise `NotAvailableYet`. Format lạ thì raise `SourceFormatChanged`.
- Mọi hằng số của nguồn (URL, giới hạn rate, độ trễ công bố) đặt trong sub package của nguồn đó.

```python
@dataclass
class FetchContext:
    settings: Settings
    session: requests.Session
    run_date: str               # ngày UTC lúc chạy, dùng làm <fetch_date> của raw
    def raw_dir(self, dataset, partition) -> Path: ...
```

## `run_partition`

```python
run_partition(dataset_name, partition, *, trigger="cli", settings=None, meta=None, session=None) -> RunResult
```

Các bước:
1. Tra dataset, kiểm tra đủ `partition_keys`, kiểm tra đủ secret trong `requires`.
2. `storage.check_disk`.
3. `meta.start_run`.
4. `fetch` → `meta.record_raw`.
5. `normalize`, thêm `source` và `ingested_at`.
6. `checks` → `meta.record_dq`. Có lỗi `error` thì raise `DataQualityError`.
7. Đọc file silver hiện có, `merge`, `storage.write_parquet_atomic`.
8. `meta.upsert_catalog`.
9. Nếu `keep_raw = False`: xoá file raw, `meta.mark_raw_deleted`.
10. `meta.finish_run("success")`.

Lỗi ở bất kỳ bước nào: `meta.finish_run("failed", error=...)` rồi raise lại. `NotAvailableYet` ghi status `skipped_not_available`, vẫn raise để orchestration retry.

**Đồng thời:** hai run cùng ghi một file silver sẽ mất dữ liệu. Partition phải được thiết kế sao cho mỗi file silver chỉ do một partition ghi tại một thời điểm (ví dụ Binance: file theo tháng, mỗi DAG run chỉ ghi ngày của mình, `max_active_runs=1`). Ngoài ra `run_partition` lấy file lock (`<silver_path>.lock`) trước bước 7.

## Backfill

```python
run_backfill(dataset_name, universe_name, start, end, *, backfill_id=None, max_workers=1)
```

- `plan_backfill` gọi `dataset.backfill_partitions` cho từng member, ghi danh sách vào `backfill_checkpoints` với status `pending`.
- Chạy lần lượt (hoặc `max_workers` luồng nếu nguồn cho phép), đánh dấu `done` / `failed` sau từng partition.
- Truyền lại `backfill_id` cũ thì chỉ chạy các partition chưa `done`. Đây là cơ chế resume khi crash.
- Gọi `check_disk` trước mỗi partition. Hết disk thì dừng, không đánh dấu `failed`.
- Nguồn có file tháng (Binance) thì backfill theo tháng, không theo ngày.

## Registry

Mỗi sub package nguồn gọi `register(MyDataset())` trong `__init__.py`. `ingestion/sources/__init__.py` import tất cả sub package. Tên dataset trùng thì lỗi lúc import.

## Thêm một nguồn mới

1. Viết spec `docs/specs/ingestion/sources/<nguồn>.md` theo mẫu của các spec nguồn hiện có.
2. Thêm bảng vào `schemas` nếu cần bảng mới.
3. Tạo sub package `src/alphadata/ingestion/sources/<nguồn>/`, viết class con của `Dataset`, đăng ký.
4. Test với dữ liệu giả trong `tests/ingestion/sources/<nguồn>/` (không gọi mạng).
5. Chạy thử: `alphadata run <dataset> key=value`.
6. Thêm universe trong `config/universe/` và lịch trong `orchestration/schedules.py`.
7. Cập nhật bảng nguồn trong [architecture.md](../../architecture.md) mục 7.

## Done when

- Một dataset giả (đọc file local) chạy qua đủ 10 bước, có test.
- Chạy lại cùng partition cho file silver giống hệt (idempotent).
- Backfill bị kill giữa chừng, chạy lại với cùng `backfill_id` chỉ làm phần còn lại.
