# Spec: `orchestration`

> Code: `src/alphadata/orchestration/` · Tầng: vận hành · Phase 1
> Phụ thuộc: `ingestion.pipeline`, `universe`, `quality`, `gold`, `metadata`. Không module nào được import `orchestration`.

## Mục đích

Lên lịch, retry và cảnh báo cho mọi job. Orchestration **không chứa logic dữ liệu**: mọi task chỉ gọi hàm của các module khác (`run_partition`, job dựng gold, check sau khi ghi). Trước đây DAG nằm ở `dags/` ngoài package và tự viết danh sách lịch; nay nó là một module như các tầng khác, và lịch được khai báo một chỗ.

## File

| File | Nội dung |
|---|---|
| `schedules.py` | Khai báo lịch: dataset nào, universe nào, cron nào, retry thế nào, freshness bao lâu |
| `factory.py` | Sinh DAG Airflow từ `schedules.py` |
| `alerts.py` | Callback khi task fail: gửi Telegram (hoặc Slack) |
| `dags/alphadata_dags.py` | Entrypoint duy nhất Airflow đọc: gọi `factory.build_all()` |

Thư mục `dags/` được mount vào `/opt/airflow/dags` trong container (xem [deploy.md](deploy.md)). Airflow import file này bằng Python của Airflow, không có package `alphadata` (package nằm trong virtualenv riêng). Vì vậy `factory.py` và `schedules.py` **chỉ dùng thư viện chuẩn và Airflow**, không import gì khác của `alphadata` ở top-level; task gọi vào `alphadata` qua `@task.external_python`.

## Khai báo lịch

```python
@dataclass(frozen=True)
class Schedule:
    dag_id: str
    kind: str              # "ingest" | "gold" | "post_check" | "maintenance"
    cron: str              # UTC
    dataset: str | None    # với kind="ingest"
    universe: str | None
    partition: str         # cách tạo partition: "prev_utc_day", "trading_day", "member_only", "month"
    retries: int = 4
    retry_delay_min: int = 30
    freshness_hours: int = 36
```

## Lịch (UTC)

| DAG | Cron | Giờ VN | Nội dung |
|---|---|---|---|
| `binance_daily` | `0 1 * * *` | 08:00 | klines 1m, metrics (ngày UTC hôm trước) |
| `binance_funding_monthly` | `0 4 2 * *` | 11:00 ngày 2 | funding rate tháng trước |
| `alpaca_daily` | `30 23 * * 1-5` | 06:30 | bars 1m + 1d ngày giao dịch vừa xong |
| `yahoo_daily` | `30 23 * * 1-5` | 06:30 | daily + corporate actions |
| `fred_daily` | `0 22 * * *` | 05:00 | series có cập nhật |
| `treasury_daily` | `0 23 * * 1-5` | 06:00 | par + real yield |
| `nyfed_daily` | `0 14 * * 1-5` | 21:00 | SOFR, EFFR, OBFR |
| `sec_daily` | `0 7 * * *` | 14:00 | company tickers (+ companyfacts khi có disk) |
| `listings_daily` | `0 2 * * 2-6` | 09:00 | Nasdaq Trader |
| `sp500_weekly` | `0 2 * * 1` | 09:00 thứ Hai | GitHub + Wikipedia |
| `french_monthly` | `0 3 5 * *` | 10:00 ngày 5 | factor |
| `p3_p4_macro` | theo spec từng nguồn | | EIA, BLS, BEA, ECB, World Bank, IMF, OECD |
| `reference_rebuild` | `0 5 * * *` | 12:00 | dựng lại `reference.*` |
| `gold_rebuild` | `30 5 * * *` | 12:30 | dựng lại `gold.*` |
| `dq_post_checks` | `0 6 * * *` | 13:00 | freshness, thiếu nến, đối chiếu nguồn |
| `maintenance_cleanup_logs` | `0 3 * * *` | 10:00 | xoá log Airflow cũ hơn 7 ngày |

Các DAG mặc định **tạm dừng** khi deploy; bật từng DAG trên UI sau khi chạy thử.

## Cấu hình Airflow

- Airflow 3, `LocalExecutor`, `parallelism = 2` (RAM hạn chế).
- `catchup = False`, `max_active_runs = 1` cho mọi DAG (tránh hai run cùng ghi một file silver).
- Một task cho mỗi member của universe (dynamic task mapping `.expand`).
- `NotAvailableYet` → retry. `MissingCredential`, `DiskFullError`, `SourceFormatChanged`, `DataQualityError` → fail ngay (dùng `AirflowFailException`), alert.

## Cảnh báo

- Kênh: Telegram bot (miễn phí). Đổi sang Slack nếu team dùng Slack (chờ leader).
- Gửi khi: task fail hẳn (hết retry hoặc lỗi không retry), `dq_post_checks` có warn mới, disk dùng quá 85%.
- Nội dung: DAG, task, partition, lỗi (dòng đầu), link log (qua SSH tunnel).

## Done when

- Thêm một dataset mới vào `schedules.py` là có DAG mới, không viết DAG tay.
- Tắt mạng giả lập, task retry đúng số lần rồi alert.
- Chạy ổn định 1 tuần trên server bee với các DAG Phase 1.
