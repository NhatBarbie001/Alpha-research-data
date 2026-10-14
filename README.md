# alphadata: tầng data cho Alpha research agent

Crawl dữ liệu thị trường và macro → lưu bản gốc (raw) → chuẩn hoá, kiểm tra chất lượng → bảng silver (Parquet) → query bằng DuckDB. Airflow chạy theo lịch trên server bee.

Tài liệu:
- [docs/data-layer-spec.md](docs/data-layer-spec.md): spec tổng thể và danh sách task
- [docs/scope.md](docs/scope.md): phạm vi prototype, ràng buộc server, dung lượng
- [docs/sources.md](docs/sources.md): danh sách nguồn dữ liệu và trạng thái

## Nguồn đang chạy

| Dataset | Nguồn | Bảng silver | Granularity |
|---|---|---|---|
| `binance_spot_klines_1m` | Binance public data | `crypto_bars_1m` | 1 phút |
| `yahoo_daily` | Yahoo Finance | `equity_bars_1d` | Daily |
| `fred_series` | FRED/ALFRED API (cần key miễn phí) | `macro_observations` | Daily → quý, có vintage |

Universe pilot nằm trong [src/alphadata/universe.py](src/alphadata/universe.py).

## Chạy trên máy dev

Cần Python ≥ 3.11 và [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env          # điền FRED_API_KEY nếu muốn chạy FRED
uv sync
uv run pytest                 # test offline, không cần mạng

uv run alphadata run binance_spot_klines_1m symbol=BTCUSDT date=2026-10-11
uv run alphadata run yahoo_daily symbol=SPY
uv run alphadata run fred_series series_id=DGS10
uv run alphadata backfill-binance --symbols BTCUSDT,ETHUSDT --start 2024-01 --end 2024-03
uv run alphadata pilot        # toàn bộ universe pilot

uv run alphadata catalog      # bảng nào, từ nguồn nào, ghi chú
uv run alphadata runs         # lịch sử chạy
uv run alphadata query "SELECT symbol, count(*) FROM crypto_bars_1m GROUP BY 1"
```

Dữ liệu được ghi vào `./data` (đổi bằng `ALPHADATA_ROOT`).

### Ví dụ query

```sql
-- Nến 1 giờ từ nến 1 phút
SELECT time_bucket(INTERVAL 1 hour, ts_utc) AS hour,
       first(open ORDER BY ts_utc) AS open, max(high) AS high,
       min(low) AS low, last(close ORDER BY ts_utc) AS close, sum(volume) AS volume
FROM crypto_bars_1m WHERE symbol = 'BTCUSDT'
GROUP BY 1 ORDER BY 1;
```

```python
# Macro point-in-time: CPI đúng như những gì đã biết vào ngày 2024-06-01
from alphadata.query import connect, macro_as_of
macro_as_of(connect(), "CPIAUCSL", "2024-06-01")
```

## Pipeline hoạt động thế nào

Mỗi lần chạy xử lý một **partition** (ví dụ: BTCUSDT ngày 2026-10-11), theo các bước trong [pipeline.py](src/alphadata/pipeline.py):

1. **Disk guard**: dừng nếu disk còn dưới `ALPHADATA_MIN_FREE_GB`.
2. **fetch**: tải về, lưu nguyên bản vào `data/raw/<nguồn>/<dataset>/<partition>/<ngày tải>/` kèm manifest (URL, SHA-256).
3. **normalize**: đưa về schema chuẩn, thời gian UTC, thêm `source` và `ingested_at`.
4. **checks**: kiểm tra chất lượng. Lỗi mức `error` (trùng khoá, sai khoảng thời gian, rỗng) thì **không ghi**. Mức `warn` (thiếu nến, OHLC bất thường) vẫn ghi nhưng được lưu lại.
5. **merge + ghi silver**: gộp với dữ liệu cũ, ghi Parquet một cách atomic (không bao giờ có file ghi dở).
6. **Ghi metadata**: `ingestion_runs`, `raw_files`, `dq_results`, `table_catalog`.

Chạy lại cùng một partition cho cùng kết quả (idempotent).

## Cấu trúc thư mục

```
src/alphadata/
  sources/        mỗi nguồn một file: fetch / normalize / checks / merge
  pipeline.py     điều phối một lần chạy
  quality.py      các hàm kiểm tra chất lượng
  meta.py         metadata (SQLite trên máy dev, Postgres trên server)
  storage.py      raw, Parquet, disk guard
  query.py        view DuckDB trên silver, macro_as_of
  universe.py     danh sách mã pilot
  cli.py
dags/             Airflow DAG (mỏng, chỉ gọi run_partition)
infra/            Dockerfile + docker-compose cho Airflow 3 + Postgres
tests/
```

Thêm nguồn mới: xem cuối [docs/sources.md](docs/sources.md).

## Triển khai trên server bee

```bash
git clone <repo> && cd <repo>
cp .env.example .env    # điền FRED_API_KEY, đổi mọi giá trị change-me
sudo mkdir -p /data/alphadata && sudo chown -R 50000:0 /data/alphadata

cd infra
docker compose --env-file ../.env up -d --build
docker builder prune -f   # xoá cache build để tiết kiệm disk
```

Web UI chỉ mở trên localhost của server. Từ máy mình: `ssh -L 8080:localhost:8080 <user>@<server-bee>`, rồi mở http://localhost:8080. DAG mặc định bị tạm dừng; bật từng DAG trên UI.

| DAG | Lịch (UTC) | Giờ VN |
|---|---|---|
| `binance_klines_1m_daily` | 01:00 hằng ngày | 08:00 |
| `yahoo_daily` | 23:30 thứ Hai–thứ Sáu | 06:30 |
| `fred_series_daily` | 22:00 hằng ngày | 05:00 |
| `maintenance_cleanup_logs` | 03:00 hằng ngày | 10:00 |

Đã chạy thử toàn bộ stack bằng Docker Desktop (RAM 8 GB): Binance và Yahoo chạy song song thành công, metadata ghi vào Postgres. Chưa chạy trên server bee.

## Quyết định thiết kế

- **Parquet + DuckDB cho mọi dữ liệu giá**, kể cả daily. Postgres chỉ giữ metadata. Lý do: disk server hạn chế, Parquet nén tốt (zstd mức 9 + byte_stream_split), và DuckDB đọc trực tiếp không cần nạp vào DB.
- **Airflow gọi pipeline qua virtualenv riêng** (`@task.external_python`), để dependency của pipeline không xung đột với Airflow.
- **Pipeline không phụ thuộc Airflow**: mọi việc chạy được bằng CLI, dễ debug và test.
- **Raw của Binance bị xoá sau khi ghi silver** (vẫn giữ manifest), vì file gốc tải lại được và minute chiếm nhiều chỗ. Raw của Yahoo và FRED được giữ.
- **Dùng `symbol` làm khoá**, chưa có `security_id` như spec. Sẽ thêm khi làm security master (T10).
