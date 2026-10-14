-- DB riêng cho metadata của pipeline (ingestion_runs, dq_results, table_catalog...).
-- DB "airflow" (metadata của Airflow) được tạo sẵn bởi POSTGRES_DB.
CREATE DATABASE alphadata;
