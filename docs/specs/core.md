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
| `user_agent` | `ALPHADATA_USER_AGENT` | `alphadata/0.2 (internal research; <email>)` | SEC bắt buộc có email liên hệ |
| API key | `FRED_API_KEY`, `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY`, `EIA_API_KEY`, `BEA_API_KEY`, `BLS_API_KEY` | trống | Đọc qua `settings.secret(name)`, báo `MissingCredential` nếu trống |

Thuộc tính suy ra: `raw_dir`, `silver_dir`, `gold_dir`, `checkpoint_dir` (đều nằm dưới `data_root`).

Key không bao giờ được ghi vào log, manifest hay URL lưu trong metadata. `http.py` phải che (`***`) query param tên `api_key`, `UserID`, `registrationkey` trước khi trả URL cho tầng trên.

## HTTP

```python
make_session(settings) -> requests.Session
get_bytes(session, url, *, params=None, headers=None, host_rate: float | None = None) -> Response
```

- Retry 5 lần, backoff mũ (2, 4, 8, ... giây) với mã 429, 500, 502, 503, 504. Tôn trọng header `Retry-After`.
- Rate limit theo host: mỗi connector khai báo số request/giây tối đa (ví dụ SEC 10/s, Alpaca 200/phút). Dùng token bucket đơn giản trong process.
- Trả về cả URL đã che key để ghi manifest.
- Mã 404 không retry. Connector tự quyết 404 nghĩa là `NotAvailableYet` hay lỗi thật.

## Lỗi dùng chung

| Exception | Khi nào | Orchestration xử lý |
|---|---|---|
| `NotAvailableYet` | Nguồn chưa đăng dữ liệu cho partition (ví dụ file ngày hôm qua) | Retry sau |
| `MissingCredential` | Thiếu API key | Fail ngay, không retry |
| `SourceFormatChanged` | Format trả về khác mong đợi (thiếu cột, JSON khác cấu trúc) | Fail, alert |
| `DiskFullError` | Disk dưới ngưỡng | Fail, alert, không retry |
| `DataQualityError` | Check mức `error` không qua | Fail, alert |

## Thời gian

- `epoch_to_utc(values, unit=None)`: nếu `unit` không truyền thì suy theo độ lớn (giây / mili-giây / micro-giây). Dùng cho Binance (đổi sang micro-giây từ 2025-01-01).
- `previous_utc_day(ref)`: ngày UTC liền trước `ref`.
- Mọi timestamp trả ra là `datetime64[us, UTC]`.

## Done when

- `get_settings()` đọc đúng `.env`, có test với biến môi trường giả.
- Test chứng minh URL có `api_key` bị che trong giá trị trả về.
- Test retry với server giả trả 429 rồi 200.
