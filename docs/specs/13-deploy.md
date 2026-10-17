# Spec: `deploy`

> Code: `deploy/` (ngoài package) · Tầng: vận hành · Phase 0
> Phụ thuộc: package `alphadata`, `orchestration/dags/`

## Mục đích

Dựng toàn bộ stack trên server bee bằng Docker Compose: Airflow 3 + Postgres, package `alphadata` trong virtualenv riêng, lake trên disk của host.

## File

| File | Nội dung |
|---|---|
| `Dockerfile` | Image Airflow 3 chính thức + virtualenv `/home/airflow/alphadata-venv` cài package `alphadata` (bằng `uv`) |
| `docker-compose.yml` | Service `postgres`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor` |
| `init-db.sql` | Tạo DB `alphadata_meta` và user riêng của pipeline |
| `migrations/` | File SQL có số thứ tự cho thay đổi schema của metadata |
| `backup.sh` | `pg_dump` DB `alphadata_meta` hằng ngày, giữ 7 bản |

## Mount

| Host | Container | Ghi chú |
|---|---|---|
| `/data/alphadata` | `/data/alphadata` | Lake. Quyền `50000:0` (user airflow) |
| `src/alphadata/orchestration/dags/` | `/opt/airflow/dags` | Read-only |
| volume `airflow-logs` | `/opt/airflow/logs` | Dọn bởi DAG `maintenance_cleanup_logs` |
| volume `pgdata` | `/var/lib/postgresql/data` | |

## Biến môi trường

Từ `.env` ở repo root (không commit, có `.env.example`): `ALPHADATA_ROOT=/data/alphadata`, `ALPHADATA_META_DB_URL`, `ALPHADATA_MIN_FREE_GB`, các API key (xem [01-core.md](01-core.md)), mật khẩu Postgres và Airflow (đổi mọi giá trị `change-me`), token Telegram.

## Giới hạn tài nguyên

| Service | `mem_limit` |
|---|---|
| postgres | 1 GB |
| airflow (scheduler + apiserver + dag-processor) | 2.5 GB tổng |
| task ETL (chạy trong scheduler qua LocalExecutor) | DuckDB `memory_limit='2GB'` |

## Bảo mật

- Web UI chỉ bind `127.0.0.1:8080`. Truy cập từ máy dev qua `ssh -L 8080:localhost:8080 <user>@<server-bee>`.
- Postgres không mở cổng ra ngoài host.
- Repo private, không commit key, không commit data.

## Các bước deploy

```bash
git clone <repo> && cd <repo>
cp .env.example .env                     # điền key, đổi mọi change-me
sudo mkdir -p /data/alphadata && sudo chown -R 50000:0 /data/alphadata
cd deploy
docker compose --env-file ../.env up -d --build
docker builder prune -f                  # xoá cache build, tiết kiệm disk
```

## Việc cần làm trên server trước khi deploy

1. Kiểm tra 33 GB disk đang dùng là gì, có dọn được không (`sudo du -xh --max-depth=1 /`, `docker system df`).
2. Hỏi leader các service đang chạy trên server, để không ảnh hưởng chúng.
3. Kiểm tra: SSD hay HDD, đã có Docker / Compose chưa, có sudo không, ra Internet được không.
4. Thử lại từ server các nguồn bị lỗi mạng ở máy dev (Dukascopy, Cboe), ghi kết quả vào [02-architecture.md](../02-architecture.md) mục 6.2.

## Done when

- `docker compose up` từ repo sạch lên được stack, DAG hiện trên UI.
- Image Airflow + venv dưới 3.5 GB.
- Backup Postgres restore thử thành công.
