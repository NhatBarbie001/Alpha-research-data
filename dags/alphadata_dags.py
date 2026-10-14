"""Airflow DAG cho tầng data.

DAG chỉ là lớp điều phối mỏng: mọi logic nằm trong alphadata.pipeline.run_partition
(cũng là hàm mà CLI gọi). Package alphadata được cài trong một virtualenv riêng
(ALPHADATA_PYTHON) để không xung đột dependency với Airflow, nên các task gọi nó
qua @task.external_python và chỉ import alphadata BÊN TRONG hàm.

Lịch (UTC):
  binance_klines_1m_daily  01:00 hằng ngày        (08:00 giờ VN)  tải ngày UTC hôm trước
  yahoo_daily              23:30 thứ Hai-thứ Sáu  (06:30 giờ VN)
  fred_series_daily        22:00 hằng ngày        (05:00 giờ VN)
  maintenance_cleanup_logs 03:00 hằng ngày        xoá log Airflow cũ hơn 7 ngày
"""

import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import dag, get_current_context, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context

ALPHADATA_PYTHON = os.getenv("ALPHADATA_PYTHON", "/home/airflow/alphadata-venv/bin/python")
LOG_RETENTION_DAYS = int(os.getenv("ALPHADATA_LOG_RETENTION_DAYS", "7"))

DEFAULT_ARGS = {
    "owner": "data",
    "retries": 4,
    "retry_delay": timedelta(minutes=30),  # file của nguồn có thể đăng muộn, retry đến ~2 giờ sau
}
COMMON = dict(start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
              default_args=DEFAULT_ARGS)
VENV_TASK = dict(python=ALPHADATA_PYTHON, expect_airflow=False)


@task
def previous_utc_day() -> str:
    """Ngày UTC trước thời điểm DAG được lên lịch chạy."""
    import pendulum

    ctx = get_current_context()
    ref = ctx.get("data_interval_end") or ctx.get("logical_date") or pendulum.now("UTC")
    return (ref - timedelta(days=1)).strftime("%Y-%m-%d")


# ---------- Binance: nến 1 phút ----------

@task.external_python(**VENV_TASK)
def list_crypto() -> list:
    from alphadata import universe
    return universe.PILOT_CRYPTO


@task.external_python(**VENV_TASK)
def ingest_binance(symbol: str, day: str) -> int:
    from alphadata.pipeline import run_partition
    return run_partition("binance_spot_klines_1m", {"symbol": symbol, "date": day}).rows_in


@dag(dag_id="binance_klines_1m_daily", schedule="0 1 * * *", tags=["alphadata", "minute", "crypto"], **COMMON)
def binance_klines_1m_daily():
    ingest_binance.partial(day=previous_utc_day()).expand(symbol=list_crypto())


# ---------- Yahoo: daily ----------

@task.external_python(**VENV_TASK)
def list_yahoo() -> list:
    from alphadata import universe
    return universe.PILOT_YAHOO


@task.external_python(**VENV_TASK)
def ingest_yahoo(symbol: str) -> int:
    from alphadata.pipeline import run_partition
    return run_partition("yahoo_daily", {"symbol": symbol}).rows_in


@dag(dag_id="yahoo_daily", schedule="30 23 * * 1-5", tags=["alphadata", "daily", "equity"], **COMMON)
def yahoo_daily():
    ingest_yahoo.expand(symbol=list_yahoo())


# ---------- FRED: macro ----------

@task.external_python(**VENV_TASK)
def list_fred() -> list:
    from alphadata import universe
    return list(universe.PILOT_FRED)


@task.external_python(**VENV_TASK)
def ingest_fred(series_id: str) -> int:
    from alphadata.pipeline import run_partition
    return run_partition("fred_series", {"series_id": series_id}).rows_in


@dag(dag_id="fred_series_daily", schedule="0 22 * * *", tags=["alphadata", "macro"], **COMMON)
def fred_series_daily():
    ingest_fred.expand(series_id=list_fred())


# ---------- Bảo trì: giữ disk không bị log làm đầy ----------

@task.bash
def cleanup_logs() -> str:
    return f"find /opt/airflow/logs -type f -mtime +{LOG_RETENTION_DAYS} -delete && find /opt/airflow/logs -type d -empty -delete"


@dag(dag_id="maintenance_cleanup_logs", schedule="0 3 * * *", tags=["alphadata", "maintenance"], **COMMON)
def maintenance_cleanup_logs():
    cleanup_logs()


binance_klines_1m_daily()
yahoo_daily()
fred_series_daily()
maintenance_cleanup_logs()
