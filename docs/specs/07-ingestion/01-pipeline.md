# Spec: `ingestion.pipeline`

> Code: `src/alphadata/ingestion/pipeline/` · Tầng: ingestion · Phase 0
> Phụ thuộc: `core`, `storage`, `metadata`, `quality`, `schemas`, `universe`

## Mục đích

Khung chung để chạy mọi nguồn: hợp đồng `Dataset` mà mỗi connector phải tuân theo, hàm `run_partition` điều phối một lần chạy, backfill có checkpoint, và registry tra dataset theo tên. Được gọi từ cả CLI và Airflow, không import Airflow.

## File

| File | Nội dung |
|---|---|
| `dataset.py` | `Dataset` (abstract), `FetchContext`, kiểu `Partition` |
| `run.py` | `run_partition()`, `reprocess_partition()`, `RunResult`, `RawUnavailable` |
| `backfill.py` | `plan_backfill()`, `run_backfill()` (P0-13) |
| `registry.py` | `register()`, `get_dataset(name)`, `list_datasets()` |

## Hợp đồng `Dataset`

Mỗi dataset ghi vào **đúng một bảng silver**. Một nguồn có nhiều bảng thì có nhiều dataset.

```python
Partition = dict[str, str]     # giá trị luôn là chuỗi; ngày dạng ISO

class Dataset(ABC):
    name: str                       # id duy nhất, dùng trong CLI, universe, schedules
    source: str                     # ghi vào cột `source`
    table: str                      # tên bảng silver, phải có trong schemas
    description: str
    source_url: str
    license_note: str               # tóm tắt điều khoản, ghi vào catalog
    partition_keys: tuple[str, ...] # ví dụ ("symbol", "date")
    keep_raw: bool = True           # False: xoá raw sau khi ghi silver (giữ manifest)
    requires: tuple[str, ...] = ()  # tên secret cần có, ví dụ ("FRED_API_KEY",)
    refresh_schedule: str = "manual"  # cron UTC, khớp orchestration/schedules.py; ghi vào catalog
    catalog_notes: str = ""         # lưu ý ghi vào catalog

    schema -> TableSchema           # get_schema(table)

    @abstractmethod
    def fetch(self, ctx: FetchContext, partition: Partition) -> list[RawFile]: ...
    @abstractmethod
    def normalize(self, raws: list[RawFile], partition: Partition) -> pd.DataFrame: ...

    def checks(self, partition) -> list[Check]: ...           # mặc định: quality.default_checks(schema)
    def merge(self, existing, new) -> pd.DataFrame: ...       # mặc định: storage.upsert theo khoá của schema
    def read(self, raw: RawFile) -> bytes: ...                # storage.read_raw: đọc có kiểm checksum
    def backfill_partitions(self, member, start, end) -> list[dict]: ...  # thêm ở P0-13
```

**Connector không quyết định đường dẫn silver.** Pipeline chia dữ liệu theo partition của bảng (`schemas.split_partitions`) và lấy đường dẫn từ `storage.layout.table_file`. Một partition chạy có thể ghi nhiều file, ví dụ dữ liệu vắt qua hai tháng.

Quy tắc cho connector:
- `fetch` chỉ tải và lưu nguyên bản qua `ctx.save` (vào đúng thư mục raw của partition). Không parse, không biến đổi. Phải trả ít nhất một file; raw nằm ngoài `data_root` thì lỗi.
- `normalize` chỉ đọc raw đã lưu qua `self.read`, không gọi mạng. Nhờ vậy chạy lại ETL được mà không cần crawl lại. Trả đủ cột của bảng **trừ** `source`, `ingested_at` (pipeline thêm; connector tự thêm thì lỗi).
- Nguồn chưa có dữ liệu thì raise `NotAvailableYet`. Format lạ thì raise `SourceFormatChanged`.
- Mọi hằng số của nguồn (URL, giới hạn rate, độ trễ công bố) đặt trong sub package của nguồn đó.

```python
@dataclass(frozen=True)
class FetchContext:
    settings: Settings
    session: TimeoutSession
    run_date: date              # ngày UTC lúc chạy, là <fetch_date> của raw
    raw_dir: Path               # raw/<source>/<dataset>/<k=v>.../<fetch_date>/
    def get(self, url, *, params=None, headers=None, host_rate=None) -> HttpResponse: ...  # core.http.get_bytes
    def save(self, content, filename, url) -> RawFile: ...                                 # storage.save_raw
```

## `run_partition`

```python
run_partition(dataset: str | Dataset, partition, *, trigger="cli", settings=None, meta=None,
              session=None, airflow_run_id=None, backfill_id=None) -> RunResult
```

Không truyền `settings`, `meta`, `session` thì tự tạo (và tự đóng `meta`, `session` khi xong).

Các bước:
1. Tra dataset. Partition phải có **đúng** bộ khoá `partition_keys` (thiếu, thừa hoặc giá trị rỗng thì `ValueError`); giá trị được đổi thành chuỗi. Kiểm tra đủ secret trong `requires` (`MissingCredential`).
2. `storage.check_disk`. Lỗi ở bước 1 và 2 chưa ghi run nào.
3. `meta.start_run`.
4. `fetch` → `meta.record_raw` cho từng file. Đường dẫn raw trong metadata là tương đối với `data_root`.
5. `normalize`, thêm `source` và `ingested_at`. **`ingested_at` = `fetched_at` muộn nhất của các raw**: tải lại thì `ingested_at` mới (đúng sự thật), còn chạy lại từ cùng raw thì cho đúng cùng một file silver.
6. `quality.run_checks(df, dataset.checks(partition), table_name=..., meta=meta, run_id=...)`: ghi `dq_results`, có check `error` fail thì raise `DataQualityError` (chưa ghi silver).
7. `schemas.conform`, `split_partitions`. Với từng file, theo thứ tự đường dẫn: lấy lock `<file>.lock` (thư viện `filelock`, timeout 600 giây), đọc file hiện có, `merge`, `conform`, sắp theo `schema.sort` (không có thì theo khoá), `storage.write_parquet_atomic`.
8. `meta.upsert_catalog` (layer silver, khoá, `path_pattern`, `refresh_schedule`, `schema_version`, `catalog_notes`).
9. Nếu `keep_raw = False`: xoá file raw, `meta.mark_raw_deleted`.
10. `meta.finish_run("success", rows_in=<dòng mới>, rows_out=<tổng dòng các file đã ghi>, silver_path=<các file, cách nhau dấu phẩy>)`.

Lỗi ở bước 3 đến 10: `meta.finish_run("failed", error="<Loại lỗi>: <message>")` (đã che key) rồi raise lại nguyên lỗi gốc. `NotAvailableYet` ghi status `skipped_not_available`, vẫn raise để orchestration retry. Nếu chính việc ghi metadata lỗi thì chỉ log, không che mất lỗi gốc.

```python
@dataclass(frozen=True)
class RunResult:
    run_id: UUID
    dataset: str
    partition: Partition
    status: str
    rows_in: int
    rows_out: int
    silver_paths: list[Path]
    raw_files: list[RawFile]
    checks: list[CheckResult]
```

**Đồng thời:** lock theo từng file ở bước 7, lấy theo thứ tự đường dẫn để hai run không chờ nhau vòng tròn. Hai run ghi hai ngày khác nhau vào cùng file tháng không mất dữ liệu (có test với 6 luồng). Orchestration vẫn đặt `max_active_runs=1` cho mỗi DAG.

## `reprocess_partition`: chạy lại từ raw, không gọi mạng

```python
reprocess_partition(dataset, partition, *, trigger="cli", settings=None, meta=None) -> RunResult
```

Lấy đúng các file raw của **lần chạy thành công gần nhất** của (dataset, partition) từ `raw_files`, đọc manifest, rồi chạy bước 5 đến 10 như một run mới (ghi lại `raw_files` cho run này để giữ lineage). Vì `ingested_at` lấy từ `fetched_at`, kết quả giống hệt file silver ban đầu. Dùng khi sửa lỗi `normalize` hoặc dựng lại silver.

Raise `RawUnavailable` khi: chưa có lần chạy thành công; raw đã bị xoá (`keep_raw=False`, phải tải lại bằng `run_partition`); file raw không còn trên disk. File raw bị sửa thì `read_raw` báo sai checksum và run ghi `failed`.

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

Mỗi sub package nguồn gọi `register(MyDataset())` trong `__init__.py`. `ingestion/sources/__init__.py` import tất cả sub package; `get_dataset` và `list_datasets` import nó ở lần gọi đầu.

`register` kiểm khai báo và báo lỗi ngay lúc import: thiếu thuộc tính bắt buộc; `name`, `source` không phải snake_case; `table` không có trong `schemas` hoặc không phải bảng silver; `partition_keys` rỗng hoặc trùng; secret không có trong danh sách của `core.settings`; tên dataset trùng.

## Thêm một nguồn mới

1. Viết spec `docs/specs/07-ingestion/02-sources/NN-<nguồn>.md` theo mẫu của các spec nguồn hiện có.
2. Thêm bảng vào `schemas` nếu cần bảng mới.
3. Tạo sub package `src/alphadata/ingestion/sources/<nguồn>/`, viết class con của `Dataset`, đăng ký.
4. Test với dữ liệu giả trong `tests/ingestion/sources/<nguồn>/` (không gọi mạng).
5. Chạy thử: `alphadata run <dataset> key=value`.
6. Thêm universe trong `config/universe/` và lịch trong `orchestration/schedules.py`.
7. Cập nhật bảng nguồn trong [02-architecture.md](../../02-architecture.md) mục 6.

## Done when

- Một dataset giả (đọc file local) chạy qua đủ 10 bước, có test.
- Chạy lại cùng partition cho file silver giống hệt (idempotent): tải lại thì dữ liệu giống hệt, chỉ `ingested_at` mới; `reprocess_partition` từ raw (mạng bị chặn) thì giống hệt cả file.
- Backfill bị kill giữa chừng, chạy lại với cùng `backfill_id` chỉ làm phần còn lại.
