# alphadata: tầng data cho Alpha research agent

Crawl dữ liệu thị trường và macro từ nguồn miễn phí → lưu bản gốc (bronze) → chuẩn hoá, kiểm tra chất lượng (silver, Parquet) → bảng sẵn cho research, point-in-time (gold) → agent đọc qua API read-only. Airflow chạy theo lịch trên server bee.

## Đọc theo thứ tự

| # | Tài liệu | Trả lời câu hỏi |
|---|---|---|
| 1 | [docs/01-problem.md](docs/01-problem.md) | Bài toán là gì, vì sao khó, ràng buộc gì, thế nào là xong |
| 2 | [docs/02-architecture.md](docs/02-architecture.md) | Có những tầng và module nào, đặt ở đâu, dùng nguồn dữ liệu nào |
| 3 | [docs/03-how-it-works.md](docs/03-how-it-works.md) | Các module gọi nhau thế nào khi chạy, dữ liệu đi đâu, lỗi xử lý ra sao |
| 4 | [docs/04-data-model.md](docs/04-data-model.md) | Thiết kế database: mọi bảng, cột, kiểu, khoá, quan hệ, lưu trữ vật lý, phân quyền |
| 5 | [docs/05-implementation-plan.md](docs/05-implementation-plan.md) | Làm theo thứ tự nào, chia task ra sao, mỗi phase phải đạt gì mới được sang phase sau |
| 6 | [docs/specs/](docs/specs/) | Chi tiết từng module, đọc theo số thứ tự bên dưới |

### Spec

| # | Nhóm | Spec |
|---|---|---|
| 01–06 | Xuyên suốt | [01 core](docs/specs/01-core.md) · [02 storage](docs/specs/02-storage.md) · [03 schemas](docs/specs/03-schemas.md) · [04 metadata](docs/specs/04-metadata.md) · [05 quality](docs/specs/05-quality.md) · [06 universe](docs/specs/06-universe.md) |
| 07 | Ingestion | [01 pipeline](docs/specs/07-ingestion/01-pipeline.md) |
| 07.02 | Nguồn P1 | [01 binance](docs/specs/07-ingestion/02-sources/01-binance.md) · [02 alpaca](docs/specs/07-ingestion/02-sources/02-alpaca.md) · [03 yahoo](docs/specs/07-ingestion/02-sources/03-yahoo.md) · [04 fred](docs/specs/07-ingestion/02-sources/04-fred.md) |
| 07.02 | Nguồn P2 | [05 sec_edgar](docs/specs/07-ingestion/02-sources/05-sec_edgar.md) · [06 sp500_constituents](docs/specs/07-ingestion/02-sources/06-sp500_constituents.md) · [07 nasdaq_trader](docs/specs/07-ingestion/02-sources/07-nasdaq_trader.md) · [08 french](docs/specs/07-ingestion/02-sources/08-french.md) · [09 treasury](docs/specs/07-ingestion/02-sources/09-treasury.md) · [10 nyfed](docs/specs/07-ingestion/02-sources/10-nyfed.md) |
| 07.02 | Nguồn P3, P4 | [11 eia](docs/specs/07-ingestion/02-sources/11-eia.md) · [12 bls](docs/specs/07-ingestion/02-sources/12-bls.md) · [13 bea](docs/specs/07-ingestion/02-sources/13-bea.md) · [14 ecb](docs/specs/07-ingestion/02-sources/14-ecb.md) · [15 worldbank](docs/specs/07-ingestion/02-sources/15-worldbank.md) · [16 imf](docs/specs/07-ingestion/02-sources/16-imf.md) · [17 oecd](docs/specs/07-ingestion/02-sources/17-oecd.md) |
| 08 | Reference | [01 trading_calendar](docs/specs/08-reference/01-trading_calendar.md) · [02 security_master](docs/specs/08-reference/02-security_master.md) · [03 corporate_actions](docs/specs/08-reference/03-corporate_actions.md) · [04 index_membership](docs/specs/08-reference/04-index_membership.md) |
| 09 | Gold | [01 bars](docs/specs/09-gold/01-bars.md) · [02 universe_daily](docs/specs/09-gold/02-universe_daily.md) · [03 macro_pit](docs/specs/09-gold/03-macro_pit.md) |
| 10–13 | Access, vận hành | [10 access](docs/specs/10-access.md) · [11 orchestration](docs/specs/11-orchestration.md) · [12 cli](docs/specs/12-cli.md) · [13 deploy](docs/specs/13-deploy.md) |

Thứ tự spec theo hướng phụ thuộc: module được module khác dùng thì đứng trước. Nếu chỉ cần viết một connector mới: đọc ba tài liệu đầu, rồi [docs/specs/07-ingestion/01-pipeline.md](docs/specs/07-ingestion/01-pipeline.md) và một spec nguồn mẫu như [docs/specs/07-ingestion/02-sources/01-binance.md](docs/specs/07-ingestion/02-sources/01-binance.md).

## Phát triển

Cần [uv](https://docs.astral.sh/uv/) và Python ≥ 3.12.

```bash
uv sync                      # tạo .venv, cài package + công cụ dev
cp .env.example .env         # điền API key; không commit .env
uv run pytest                # test (bỏ qua test gọi mạng thật)
uv run pytest -m live        # chỉ test gọi nguồn thật
uv run ruff check && uv run ruff format --check
uv run mypy
```

Chạy thử pipeline (dữ liệu ghi vào `ALPHADATA_ROOT`, mặc định `./data`):

```bash
uv run alphadata --help
uv run alphadata run binance_spot_klines_1m symbol=BTCUSDT period=2026-10-18
uv run alphadata run-universe crypto_pilot period=2026-10-18
uv run alphadata runs --limit 10
uv run alphadata query "SELECT symbol, count(*) FROM crypto_bars_1m GROUP BY symbol"
```

CI (`.github/workflows/ci.yml`) chạy các bước trên cùng quét secret bằng gitleaks cho mọi PR và mọi push lên `main`. Mỗi task trong [docs/05-implementation-plan.md](docs/05-implementation-plan.md) là một PR, nhánh đặt tên theo ID task (ví dụ `p0-02-core`).

## Quy ước

- Thêm module mới thì viết spec trước, đặt trong `docs/specs/` theo đúng vị trí của module trong `src/alphadata/`, tên file `NN-<tên module>.md`.
- Thêm nguồn mới: xem [docs/03-how-it-works.md](docs/03-how-it-works.md) mục 11 và [docs/specs/07-ingestion/01-pipeline.md](docs/specs/07-ingestion/01-pipeline.md).
- Chỉ dùng nguồn miễn phí, gọi được bằng chương trình, điều khoản cho phép dùng nội bộ. Nguồn bị loại và lý do ở [docs/02-architecture.md](docs/02-architecture.md) mục 6.2.
- Không commit API key, không commit data.
