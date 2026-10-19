# Spec: `core`

> Code: `src/alphadata/core/` · Tầng: xuyên suốt · Phase 0
> Phụ thuộc: không (module gốc). Mọi module khác được phép import `core`.

## Mục đích

Gom những thứ mọi module đều cần: cấu hình, HTTP client, các lỗi dùng chung, tiện ích thời gian. `core` không biết gì về nguồn, bảng hay Airflow.

## File

| File | Nội dung |
|---|---|
| `settings.py` | `Settings` (dataclass frozen) và `get_settings()` đọc từ biến môi trường / `.env` |
| `http.py` | `make_session()`, `get_bytes()` có retry, timeout, rate limit theo host |
| `errors.py` | Các exception dùng chung |
| `timeutil.py` | Chuyển epoch sang UTC, xác định đơn vị epoch, ngày UTC hôm trước |

## `Settings`

| Trường | Biến môi trường | Mặc định | Ghi chú |
|---|---|---|---|
| `data_root` | `ALPHADATA_ROOT` | `./data` | Server: `/data/alphadata` |
| `meta_db_url` | `ALPHADATA_META_DB_URL` | `sqlite:///<data_root>/meta.db` | Server: Postgres DB `alphadata_meta` |
| `min_free_gb` | `ALPHADATA_MIN_FREE_GB` | `3` | Ngưỡng disk guard |
| `http_timeout` | `ALPHADATA_HTTP_TIMEOUT` | `60` | Giây |
| `universe_dir` | `ALPHADATA_UNIVERSE_DIR` | `./config/universe` | |
| `user_agent` | `ALPHADATA_USER_AGENT` | `alphadata/<version> (internal research)` | SEC bắt buộc có email liên hệ: trên server phải đặt, ví dụ `alphadata/0.1 (internal research; ops@company.com)` |
| API key | `FRED_API_KEY`, `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY`, `EIA_API_KEY`, `BEA_API_KEY`, `BLS_API_KEY` | trống | Đọc qua `settings.secret(name)`, báo `MissingCredential` nếu trống. Tên không thuộc danh sách thì `ValueError` |

Thuộc tính suy ra: `raw_dir`, `silver_dir`, `gold_dir` (đều nằm dưới `data_root`). Checkpoint backfill nằm trong bảng `backfill_checkpoints` của metadata, không phải file.

Quy tắc đọc: biến môi trường thật ưu tiên hơn `.env`; giá trị rỗng hoặc toàn khoảng trắng coi như không đặt (để `.env.example` có thể để trống); số không hợp lệ hoặc ≤ 0 thì `ValueError` ghi rõ tên biến. Giá trị key không xuất hiện trong `repr(settings)`.

Key không bao giờ được ghi vào log, manifest hay URL lưu trong metadata. `http.py` phải che (`***`) query param tên `api_key`, `UserID`, `registrationkey` trước khi trả URL cho tầng trên.

## HTTP

```python
make_session(settings) -> TimeoutSession          # requests.Session có User-Agent và timeout từ settings
get_bytes(session, url, *, params=None, headers=None, host_rate: float | None = None) -> HttpResponse

@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    content: bytes
    headers: Mapping[str, str]   # không phân biệt hoa thường
    url: str                     # đã che key
```

- Retry tối đa 5 lần (tổng 6 lần gọi), backoff mũ 2, 4, 8, 16, 32 giây, với mã 429, 500, 502, 503, 504 và với lỗi mạng (kết nối, timeout). Có header `Retry-After` (số giây hoặc ngày giờ HTTP) thì dùng giá trị đó, trần 300 giây.
- Rate limit theo host: mỗi connector khai báo số request/giây tối đa (ví dụ SEC 10/s, Alpaca 200/phút = 3.33/s). Token bucket trong process, an toàn khi nhiều luồng; sức chứa `max(1, rate)` request.
- Kết quả trả về: 2xx, 3xx và **404** trả `HttpResponse` (404 không retry; connector tự quyết 404 nghĩa là `NotAvailableYet` hay lỗi thật). Mã lỗi khác, hoặc hết lượt retry, thì raise `HttpStatusError(status_code, url)`. Lỗi mạng sau khi hết lượt thì raise `HttpRequestError`.
- **Không trả `requests.Response` thô**, vì nó chứa URL thật có key. `HttpResponse.url` và message của mọi lỗi HTTP đều đã che key; `HttpRequestError` không giữ exception gốc của `requests` (`from None`) vì traceback gốc chứa URL chưa che.
- Hàm `mask_secrets(text)` che giá trị của `api_key`, `UserID`, `registrationkey` (không phân biệt hoa thường) trong chuỗi bất kỳ. Tầng trên dùng hàm này nếu cần ghi một chuỗi có thể chứa URL.

## Lỗi dùng chung

| Exception | Khi nào | Orchestration xử lý |
|---|---|---|
| `NotAvailableYet` | Nguồn chưa đăng dữ liệu cho partition (ví dụ file ngày hôm qua) | Retry sau |
| `MissingCredential` | Thiếu API key | Fail ngay, không retry |
| `SourceFormatChanged` | Format trả về khác mong đợi (thiếu cột, JSON khác cấu trúc) | Fail, alert |
| `DiskFullError` | Disk dưới ngưỡng | Fail, alert, không retry |
| `DataQualityError` | Check mức `error` không qua | Fail, alert |
| `SchemaMismatch` | DataFrame không khớp hợp đồng bảng (thiếu/thừa cột, sai kiểu, null ở cột không null). Raise bởi `schemas.conform` và `storage.write_parquet_atomic` | Fail, alert (lỗi code hoặc nguồn đổi format) |
| `HttpStatusError` | HTTP trả mã lỗi không retry được (khác 404), hoặc hết lượt retry | Theo cấu hình retry của DAG |
| `HttpRequestError` | Lỗi mạng sau khi hết lượt retry | Theo cấu hình retry của DAG |

Mọi lỗi trên kế thừa `AlphadataError`.

## Thời gian

- `epoch_to_utc(values, unit=None) -> Series`: nếu `unit` không truyền thì suy theo độ lớn: < 1e11 là giây, < 1e14 là mili-giây, < 1e17 là micro-giây (đúng với mốc từ 1973-03 trở đi). Một lô có giá trị thuộc hai đơn vị khác nhau thì `ValueError`, không đoán. Giữ index của Series đầu vào; giá trị null thành `NaT`. Dùng cho Binance (đổi sang micro-giây từ 2025-01-01).
- `previous_utc_day(ref=None) -> date`: ngày UTC liền trước `ref` (mặc định: bây giờ). `ref` là `datetime` thì phải có timezone, được đổi sang UTC trước khi lấy ngày; `datetime` không có timezone thì `ValueError`.
- Mọi timestamp trả ra là `datetime64[us, UTC]`.

## Done when

- `get_settings()` đọc đúng `.env`, có test với biến môi trường giả.
- Test chứng minh URL có `api_key` bị che trong giá trị trả về và trong message lỗi.
- Test retry với server giả trả 429 rồi 200.
