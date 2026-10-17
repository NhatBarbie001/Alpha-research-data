# 05. Kế hoạch triển khai

> Đọc sau [04-data-model.md](04-data-model.md). Tài liệu này trả lời: làm theo thứ tự nào, chia thành những task nào, mỗi task thế nào là xong, và mỗi phase phải đạt gì trước khi sang phase sau.
> Chi tiết kỹ thuật của từng task nằm trong spec được dẫn ở cột "Spec". Plan không lặp lại nội dung spec.

---

## 1. Nguyên tắc chia việc

| Nguyên tắc | Cụ thể | Vì sao |
|---|---|---|
| **Lát cắt mỏng trước** | Phase 0 làm một nguồn (Binance klines 1m) chạy hết đường: tải → bronze → silver → check → metadata → DAG trên server bee | Kiểm thiết kế trên dữ liệu thật ngay tuần đầu. Các nguồn sau nhân bản theo mẫu thay vì dựng framework suông |
| **Task nhỏ** | Mỗi task 0.5–2 ngày công, gói trong **một PR** | PR nhỏ review kỹ được, lỗi khoanh vùng nhanh |
| **Xong = có bằng chứng** | Mỗi task có cột "Done when" là test hoặc lệnh kiểm tra cụ thể. "Code xong" không phải là xong | Tránh tranh luận chủ quan khi review |
| **Gate cuối phase** | Mỗi phase có danh sách điều kiện (mục 4–7). Chưa qua gate thì không bắt đầu task phụ thuộc vào phase đó | Lỗi ở tầng dưới (silver sai) phát hiện muộn ở gold thì sửa rất đắt |
| **Đo thật rồi mới mở rộng** | Disk, RAM, thời gian chạy được đo ở gate 0 và 1, cập nhật vào [02-architecture.md](02-architecture.md) mục 5.2 trước khi backfill lớn | Disk là ràng buộc chặt nhất (16 GB) |
| **Spec đi trước code** | Task làm thay đổi hành vi so với spec thì sửa spec trong cùng PR | Tài liệu không lệch khỏi code |

---

## 2. Tổng quan thời gian

**Giả định:** 2 người làm song song (track A: pipeline và nguồn, track B: hạ tầng, vận hành, reference). Ước lượng tính bằng ngày công (nc). Nếu chỉ có 1 người, thời gian gần gấp đôi.

| Phase | Tổng công | Lịch (2 người) | Kết quả |
|---|---|---|---|
| 0. Nền + lát cắt Binance | ~13 nc | Tuần 1 → giữa tuần 2 | Binance klines 1m chạy hằng ngày trên bee, có backfill, có alert |
| 1. Nguồn P1 | ~9.5 nc | Giữa tuần 2 → giữa tuần 3 | Binance (đủ), Alpaca, Yahoo, FRED chạy hằng ngày; backfill pilot xong |
| 2. Reference | ~8 nc | Giữa tuần 3 → đầu tuần 4 | `security_id`, lịch sàn, thành phần S&P 500 theo ngày, corporate actions |
| 3. Gold + access | ~9.5 nc | Tuần 4 | Agent gọi `get_*` với `as_of`; test PIT qua |
| Ổn định | | Tuần 5 | Chạy 1 tuần không sửa tay, sửa lỗi phát sinh |
| 4. Mở rộng | Theo nguồn | Sau prototype | Nguồn P3, P4, fundamentals, minute từ 2016 |

```
Tuần            1           2           3           4           5
Track A   [P0 core/storage/pipeline/binance][P1 alpaca, yahoo, fred ][P3 gold, access      ][ổn định]
Track B   [P0 deploy, orchestration, CLI   ][P1 vận hành][P2 nguồn P2, reference   ][P3 test PIT][ổn định]
Gate                            G0 ▲              G1 ▲         G2 ▲            G3 ▲
```

Tổng ~40 nc, với 2 người là vừa đúng 4 tuần, **không có dự phòng**. Nếu trễ, cắt theo thứ tự ở mục 9.

---

## 3. Đảm bảo chất lượng chung (áp dụng cho mọi phase)

### 3.1 Definition of Done của một task

Một PR chỉ được merge khi:

- [ ] Các mục "Done when" của task có test tự động, hoặc có lệnh kiểm tra và output dán vào PR (với việc phải chạy trên server).
- [ ] CI xanh: lint (`ruff`), kiểm kiểu (`mypy` cho `core`, `storage`, `schemas`, `ingestion.pipeline`), `pytest`, quét secret (`gitleaks`).
- [ ] Test không gọi mạng. Connector test bằng fixture là raw thật đã ghi lại (mục 3.2).
- [ ] Không có key, URL có key, hay file data trong diff.
- [ ] Spec liên quan đã được cập nhật nếu hành vi khác spec.
- [ ] Có ít nhất 1 người khác review.

### 3.2 Các lớp test

| Lớp | Kiểm cái gì | Có từ | Chạy ở đâu |
|---|---|---|---|
| Unit | Hàm thuần: check, đổi timezone, tính `adj_factor`, ... | P0 | CI |
| Contract schema | Định nghĩa bảng trong `schemas` khớp [04-data-model.md](04-data-model.md) (tên cột, kiểu, nullable, khoá) | P0 | CI |
| Fixture connector | `normalize` trên raw thật đã ghi lại (cắt nhỏ, lưu ở `tests/fixtures/<nguồn>/`) cho silver đúng mong đợi. Mỗi nguồn có ít nhất 1 fixture bình thường và 1 fixture ca khó (DST, ngày nghỉ, epoch đổi đơn vị, số liệu bị sửa) | P0 | CI |
| Idempotent | Chạy lại cùng partition cho silver giống hệt (so hash nội dung đã sắp xếp) | P0 | CI |
| Rebuild offline | Xoá silver, chạy lại từ raw với mạng bị chặn, kết quả giống hệt | P0 | CI |
| PIT | `as_of = t` không bao giờ thấy dữ liệu có `available_from > t`. Ca mẫu: CPI, GDP, một công ty | P1 (macro), P3 (API) | CI |
| Live smoke | Gọi nguồn thật, 1 partition nhỏ, kiểm format chưa đổi. Đánh dấu `@pytest.mark.live`, không chạy trong CI | P1 | Tay hoặc DAG hằng tuần |
| Tích hợp Docker | Stack lên được, DAG chạy được 1 partition | P0 | Máy dev, trước mỗi lần deploy |
| Đối chiếu tay | Chọn ngẫu nhiên vài điểm dữ liệu, so với trang web của nguồn | P1 | Tại gate |

### 3.3 Môi trường

| Môi trường | Metadata | Lake | Dùng cho |
|---|---|---|---|
| Máy dev | SQLite | `./data` | Code, test, chạy thử CLI |
| Docker trên máy dev | Postgres | Thư mục tạm | Thử stack trước khi deploy |
| Server bee | Postgres `alphadata_meta` | `/data/alphadata` | Chạy thật |

Chỉ có một server, nên mọi thay đổi phải qua bước Docker trên máy dev trước. Deploy lên bee chỉ từ commit trên `main` đã qua CI.

### 3.4 Review tại gate

- Người phụ trách phase chuẩn bị bằng chứng cho từng điều kiện của gate (output lệnh, ảnh UI Airflow, số đo).
- Leader duyệt. Điều kiện chưa đạt thì hoặc sửa, hoặc ghi rõ là chấp nhận rủi ro (lý do, ai quyết) vào mục 10 của tài liệu này.
- Sau gate: cập nhật số đo thật (disk, RAM, thời gian chạy) vào [02-architecture.md](02-architecture.md) mục 5.

### 3.5 Theo dõi công việc

- Mỗi task là một issue GitHub, tiêu đề bắt đầu bằng ID (ví dụ `P0-07 ingestion.pipeline`). Milestone = phase.
- PR ghi `Closes #<issue>`.
- Gate là một issue riêng, nội dung là checklist copy từ mục gate tương ứng.

---

## 4. Phase 0: nền + lát cắt Binance

**Mục tiêu:** một dataset thật (`binance_spot_klines_1m`, universe `crypto_pilot`) chạy hết đường trên server bee. Mọi module nền được dựng ở mức đủ cho lát cắt này, chưa cần đủ tính năng.

### 4.1 Task

| ID | Task | Spec | Phụ thuộc | Track | nc | Done when |
|---|---|---|---|---|---|---|
| P0-01 | Khung repo: `pyproject.toml` (uv), `src/alphadata/`, `tests/`, `.env.example`, `.gitignore` (chặn `data/`, `.env`), CI GitHub Actions (ruff, mypy, pytest, gitleaks) | [02-architecture](02-architecture.md) mục 4 | | B | 0.5 | CI chạy trên PR rỗng và xanh; commit thử một chuỗi giống key thì gitleaks chặn |
| P0-02 | `core`: settings, errors, timeutil, http (retry, rate limit, che key) | [01-core](specs/01-core.md) | P0-01 | A | 1 | Các mục "Done when" của spec |
| P0-03 | `storage`: layout, `save_raw` + manifest SHA-256, Parquet ghi atomic + upsert, disk guard | [02-storage](specs/02-storage.md) | P0-02 | A | 1 | Các mục "Done when" của spec |
| P0-04 | `schemas`: `TableSchema`, `Column`, `conform()`; định nghĩa `crypto_bars_1m`; test contract với data model | [03-schemas](specs/03-schemas.md) | P0-03 | A | 1 | Contract test chạy trong CI; cố tình sửa một kiểu trong code thì test fail |
| P0-05 | `metadata`: `MetaStore` (SQLite + Postgres), 5 bảng, migration `0001` | [04-metadata](specs/04-metadata.md) | P0-02 | B | 1 | Test vòng đời một run trên SQLite; migration chạy được trên Postgres trong Docker |
| P0-06 | `quality`: runner + các check trước khi ghi cho bars (`not_empty`, `no_null_keys`, `unique_keys`, `schema_match`, `ts_in_range`, `ohlc_consistent`, `non_negative`, `row_count`) | [05-quality](specs/05-quality.md) | P0-04, P0-05 | A | 1 | Mỗi check có test đúng/sai; check `error` fail thì silver không đổi |
| P0-07 | `ingestion.pipeline`: `Dataset`, `run_partition` (10 bước), registry | [07-ingestion/01-pipeline](specs/07-ingestion/01-pipeline.md) | P0-03 → P0-06 | A | 1.5 | Dataset giả chạy đủ 10 bước; test idempotent; test rebuild offline |
| P0-08 | `universe`: loader YAML, `crypto_pilot.yaml` | [06-universe](specs/06-universe.md) | P0-02 | B | 0.5 | Load được; test file thiếu trường, member trùng |
| P0-09 | Connector `binance_spot_klines_1m` + fixture (ngày thường, ngày đổi epoch sang micro-giây 2025-01-01) | [07-ingestion/02-sources/01-binance](specs/07-ingestion/02-sources/01-binance.md) | P0-07 | A | 1 | Fixture test; `ts_in_range` bắt được nếu đọc sai đơn vị epoch |
| P0-10 | `cli` tối thiểu: `run`, `run-universe`, `datasets`, `universes`, `runs`, `catalog`, `disk`, `query` | [12-cli](specs/12-cli.md) | P0-07, P0-08 | B | 1 | Mỗi lệnh có test `CliRunner`; exit code đúng quy ước |
| P0-11 | `deploy`: Dockerfile (Airflow 3 + venv), compose, `init-db.sql`, giới hạn tài nguyên; dựng stack trên bee | [13-deploy](specs/13-deploy.md) | P0-01 | B | 1.5 | Stack lên từ repo sạch; image < 3.5 GB; UI chỉ vào được qua SSH tunnel |
| P0-12 | `orchestration`: `schedules.py`, `factory.py`, `dags/alphadata_dags.py`, alert Telegram; DAG `binance_daily` | [11-orchestration](specs/11-orchestration.md) | P0-07, P0-11 | B | 1 | Thêm dataset vào `schedules.py` là có DAG; giả lập mất mạng thì retry rồi alert |
| P0-13 | Backfill: `plan_backfill`, `run_backfill`, checkpoint, lệnh `alphadata backfill` | [07-ingestion/01-pipeline](specs/07-ingestion/01-pipeline.md) mục Backfill | P0-07, P0-10 | A | 1 | Kill giữa chừng, chạy lại cùng `backfill_id` chỉ làm phần còn lại |

**Thứ tự:** P0-11 (deploy) bắt đầu ngay ngày đầu ở track B, vì Airflow 3 + `external_python` là phần nhiều rủi ro nhất về hạ tầng. Phát hiện vấn đề sớm thì còn thời gian đổi cách.

```
P0-01 ─┬─► P0-02 ─┬─► P0-03 ─► P0-04 ─┐
       │          ├─► P0-05 ──────────┴─► P0-06 ─► P0-07 ─┬─► P0-09
       │          └─► P0-08 ──────────────────────────────┼─► P0-10 ─► P0-13
       └─► P0-11 ─────────────────────────────────────────┴─► P0-12
```

### 4.2 Gate G0

- [ ] CI xanh; độ phủ test ≥ 80% cho `core`, `storage`, `schemas`, `quality`, `ingestion.pipeline`.
- [ ] `alphadata run binance_spot_klines_1m symbol=BTCUSDT date=<hôm qua>` trên bee tạo đủ: raw + manifest, file silver, 1 dòng `ingestion_runs`, các dòng `dq_results`, dòng `table_catalog`.
- [ ] Chạy lại lệnh trên: silver giống hệt, không sinh dòng trùng.
- [ ] Xoá silver, chạy lại từ raw với mạng bị chặn: silver giống hệt.
- [ ] DAG `binance_daily` chạy 3 ngày liên tiếp cho cả `crypto_pilot`, không sửa tay.
- [ ] Alert Telegram đến được khi cố tình làm một task fail.
- [ ] Backfill 1 tháng cho 10 cặp, có một lần kill giữa chừng rồi resume.
- [ ] **Số đo:** dung lượng silver cho 1 cặp × 1 tháng, RAM đỉnh của 1 task, thời gian 1 partition. Lệch ước tính ở [02-architecture.md](02-architecture.md) mục 5.2 quá 20% thì cập nhật ước tính và xem lại kế hoạch backfill trước khi sang phase 1.

---

## 5. Phase 1: nguồn P1

**Mục tiêu:** bốn nguồn giá và macro chính chạy hằng ngày, backfill pilot xong, dữ liệu đối chiếu được với nguồn.

### 5.1 Task

| ID | Task | Spec | Phụ thuộc | Track | nc | Done when |
|---|---|---|---|---|---|---|
| P1-01 | `binance_funding_rate`, `binance_futures_metrics`; universe `crypto_futures_pilot` | [01-binance](specs/07-ingestion/02-sources/01-binance.md) | G0 | A | 1 | Fixture test; `binance_funding_monthly` chạy được |
| P1-02 | `alpaca_bars_1m`, `alpaca_bars_1d`, `alpaca_corporate_actions` (phân trang, `adjustment=raw`, pre/post market) | [02-alpaca](specs/07-ingestion/02-sources/02-alpaca.md) | G0 | A | 1.5 | Fixture có ngày đổi giờ mùa hè và ngày đóng cửa sớm; `session` gán đúng |
| P1-03 | `yahoo_daily`, `yahoo_corporate_actions` | [03-yahoo](specs/07-ingestion/02-sources/03-yahoo.md) | G0 | A | 1 | Fixture có một mã từng split; ghi đúng `close` đã điều chỉnh split và `adj_close` |
| P1-04 | `fred_series` (vintage từ ALFRED), `fred_series_current`, `fred_series_meta`, `fred_release_dates`; check `vintage_order` | [04-fred](specs/07-ingestion/02-sources/04-fred.md) | G0 | A | 2 | Test PIT: CPI và GDP tại 3 ngày cho đúng giá trị đã biết lúc đó (so với ALFRED web) |
| P1-05 | Universe: `us_etf_core`, `us_stock_pilot` (chốt danh sách, ghi ngày chốt), `us_index`, `fred_core`, `fred_wide` | [06-universe](specs/06-universe.md) | P0-08 | B | 0.5 | Load được; leader duyệt danh sách |
| P1-06 | `reference.trading_calendar` (offline từ `exchange_calendars`), làm sớm để có check thiếu nến | [08-reference/01-trading_calendar](specs/08-reference/01-trading_calendar.md) | G0 | B | 0.5 | Có ngày đóng cửa sớm và ngày nghỉ đặc biệt đúng (ví dụ 2025-01-09) |
| P1-07 | Check sau khi ghi: `freshness`, `missing_bars_vs_calendar`, `cross_source_close`; lệnh `dq post-checks`, `dq report` | [05-quality](specs/05-quality.md) | P1-06 | B | 1 | Xoá vài nến trong silver thì `missing_bars_vs_calendar` báo |
| P1-08 | Lịch trong `schedules.py` cho 4 nguồn + ngưỡng freshness; DAG `dq_post_checks` gửi alert khi có warn mới | [11-orchestration](specs/11-orchestration.md) | P1-01 → P1-04, P1-07 | B | 0.5 | Các DAG hiện trên UI với đúng cron |
| P1-09 | Vận hành: xoá raw minute sau khi ghi silver, lưu giữ raw theo [04-data-model.md](04-data-model.md) mục 9.3, `maintenance_cleanup_logs`, `backup.sh` | [13-deploy](specs/13-deploy.md), [02-storage](specs/02-storage.md) | G0 | B | 0.5 | Restore thử backup Postgres thành công |
| P1-10 | Backfill pilot trên bee: crypto từ 2024, Alpaca minute 50 mã từ 2024, daily (Alpaca + Yahoo) từ 2005, FRED core đủ vintage | [07-ingestion/01-pipeline](specs/07-ingestion/01-pipeline.md) | P1-01 → P1-05, P1-09 | A | 1 + thời gian chạy | Mọi checkpoint `done`; disk theo dõi hằng ngày |

### 5.2 Gate G1

- [ ] DAG của 4 nguồn chạy **5 ngày giao dịch liên tiếp** không sửa tay. Mọi lần fail đều có alert và được xử lý.
- [ ] Không có check `error` fail còn mở. Mọi `warn` còn lại có giải thích (ví dụ: mã ít thanh khoản thiếu nến là bình thường).
- [ ] Backfill pilot xong. Disk đã dùng không vượt ước tính quá 20%; disk trống còn ≥ ngưỡng guard + 3 GB.
- [ ] Test PIT cho CPI và GDP qua.
- [ ] Đối chiếu tay 10 điểm ngẫu nhiên (mã × ngày hoặc series × ngày) với trang web của nguồn: khớp.
- [ ] `cross_source_close` giữa Alpaca và Yahoo từ 2016: tỷ lệ ngày lệch > 0.5% dưới 1%, các ca lệch đã xem.
- [ ] Chạy rebuild offline cho toàn bộ một dataset (ví dụ `alpaca_bars_1d`): kết quả giống hệt.
- [ ] Live smoke test cho 4 nguồn chạy được.

---

## 6. Phase 2: reference

**Mục tiêu:** mọi chứng khoán có `security_id` ổn định; biết thành phần S&P 500 tại mọi ngày từ 2005, kể cả mã đã bị loại; corporate actions đã đối chiếu giữa các nguồn.

### 6.1 Task

| ID | Task | Spec | Phụ thuộc | Track | nc | Done when |
|---|---|---|---|---|---|---|
| P2-01 | Nguồn danh sách mã: `sec_company_tickers`, `nasdaq_listings` | [05-sec_edgar](specs/07-ingestion/02-sources/05-sec_edgar.md), [07-nasdaq_trader](specs/07-ingestion/02-sources/07-nasdaq_trader.md) | G1 | B | 0.5 | Fixture test; User-Agent có email liên hệ (SEC bắt buộc) |
| P2-02 | Nguồn thành phần index: `sp500_history_github`, `sp500_wikipedia` | [06-sp500_constituents](specs/07-ingestion/02-sources/06-sp500_constituents.md) | G1 | B | 1 | Fixture có bảng thay đổi của Wikipedia với dòng gộp ô |
| P2-03 | Nguồn macro/factor còn lại của P2: `french_factors`, `treasury_par_yield`, `treasury_real_yield`, `nyfed_reference_rates` | [08-french](specs/07-ingestion/02-sources/08-french.md), [09-treasury](specs/07-ingestion/02-sources/09-treasury.md), [10-nyfed](specs/07-ingestion/02-sources/10-nyfed.md) | G1 | A | 1 | Fixture test; DAG theo lịch |
| P2-04 | `reference.security_master` + `security_symbols`: cấp `security_id`, đổi mã, mã bị dùng lại, `config/reference/security_overrides.yaml` | [08-reference/02-security_master](specs/08-reference/02-security_master.md) | P2-01, P2-02 | B | 2 | FB → META (2022-06-09) cùng một `security_id`; một ticker bị dùng lại cho hai công ty ra hai `security_id` |
| P2-05 | `reference.corporate_actions`: gộp Yahoo + Alpaca, trạng thái `confirmed` / `single_source` / `conflict` | [08-reference/03-corporate_actions](specs/08-reference/03-corporate_actions.md) | P2-04 | A | 1.5 | Danh sách split mẫu ở gate G2 đúng ngày, đúng tỷ lệ |
| P2-06 | `reference.index_membership` + sinh `sp500_daily.yaml` | [08-reference/04-index_membership](specs/08-reference/04-index_membership.md) | P2-04 | B | 1.5 | Số thành phần mỗi ngày nằm trong 500–505 |
| P2-07 | Check `referential`; DAG `reference_rebuild`; lệnh `build reference` | [05-quality](specs/05-quality.md), [11-orchestration](specs/11-orchestration.md) | P2-04 → P2-06 | A | 0.5 | Cố tình thêm `security_id` lạ vào reference thì check `error` |
| P2-08 | Backfill Yahoo daily cho `sp500_daily` (mọi mã từng thuộc, từ 2005) | | P2-06 | A | 0.5 + thời gian chạy | Mọi checkpoint `done` hoặc `failed` có lý do (mã không có trên Yahoo); danh sách mã không tải được giá dùng cho bước đo độ phủ ở G2 |

### 6.2 Gate G2

- [ ] Số thành phần S&P 500 nằm trong 500–505 với **mọi** ngày giao dịch từ 2005.
- [ ] Đúng với một số sự kiện đã biết, ví dụ Lehman Brothers bị loại tháng 9/2008, TSLA vào ngày 2020-12-21.
- [ ] **Độ phủ giá đã được đo** theo định nghĩa ở [specs/09-gold/02-universe_daily.md](specs/09-gold/02-universe_daily.md) (query trên silver `yahoo_daily` + `alpaca_bars_1d` và `index_membership`, vì `universe_daily` đến phase 3 mới có). Con số theo năm từ 2005 và danh sách mã không có giá ngày nào được ghi vào issue của gate. **Không phải điều kiện chặn:** độ phủ thấp không dừng phase 3. Năm nào dưới 70% thì báo leader để biết, pipeline vẫn tiếp tục. Lý do: nguồn miễn phí khuyết giá mã đã hủy niêm yết, không sửa được trong ngân sách 0 USD; tắt Yahoo còn mất hết daily trước 2016 và index. Cách xử lý là đánh dấu và cảnh báo (làm ở P3-03, P3-05).
- [ ] Split mẫu đúng ngày và tỷ lệ: NVDA 2024-06-10 (10:1), AAPL 2020-08-31 (4:1), TSLA 2020-08-31 (5:1) và 2022-08-25 (3:1), AMZN 2022-06-06 (20:1), GOOGL 2022-07-18 (20:1).
- [ ] Mọi corporate action trạng thái `conflict` đã được xem; ca cần sửa đi qua `security_overrides.yaml`, không sửa tay dữ liệu.
- [ ] Check `referential` qua.

---

## 7. Phase 3: gold + access

**Mục tiêu:** agent đọc được dữ liệu qua API với `as_of` bắt buộc, và có test chứng minh không thấy dữ liệu tương lai. Đây là gate cuối của prototype: phải đạt toàn bộ tiêu chí ở [01-problem.md](01-problem.md) mục 5.

### 7.1 Task

| ID | Task | Spec | Phụ thuộc | Track | nc | Done when |
|---|---|---|---|---|---|---|
| P3-01 | `gold.bars_1d`: ghép Alpaca (từ 2016) + Yahoo (trước 2016, bỏ điều chỉnh split), crypto gộp từ 1m, tính `split_factor`, `adj_factor` | [09-gold/01-bars](specs/09-gold/01-bars.md) | G2 | A | 2 | Không có ngày nhảy giá tại các split mẫu; điểm nối 2016 lệch < 0.5% |
| P3-02 | `gold.bars_1m` | [09-gold/01-bars](specs/09-gold/01-bars.md) | P3-01 | A | 1 | Số dòng bằng tổng silver tương ứng; `split_factor` khớp `bars_1d` |
| P3-03 | `gold.universe_daily` + check `price_coverage` | [09-gold/02-universe_daily](specs/09-gold/02-universe_daily.md), [05-quality](specs/05-quality.md) | G2, P3-01 | B | 1 | Universe S&P 500 ngày 2008-09-12 có Lehman; mã thiếu giá vẫn có dòng `has_price = false`; `price_coverage` ghi độ phủ theo năm, dưới 70% có alert |
| P3-04 | `gold.macro_pit` | [09-gold/03-macro_pit](specs/09-gold/03-macro_pit.md) | G2 | B | 1.5 | Test PIT CPI, GDP trên gold cho kết quả như trên silver |
| P3-05 | `access`: `views.connect()`, `get_bars`, `get_macro`, `get_universe`, `get_coverage`, `get_factors`, `SurvivorshipWarning`; kết nối read-only | [10-access](specs/10-access.md) | P3-01 → P3-04 | A | 2 | Các mục "Done when" của spec |
| P3-06 | Bộ test PIT đầu-cuối: CPI, GDP, một công ty mẫu; `get_*` thiếu `as_of` thì lỗi; `end > as_of` bị cắt | [10-access](specs/10-access.md) | P3-05 | B | 1 | Chạy trong CI trên lake fixture nhỏ |
| P3-07 | Check `adj_vs_source`; DAG `gold_rebuild`; lệnh `build gold` | [05-quality](specs/05-quality.md), [11-orchestration](specs/11-orchestration.md) | P3-01 → P3-04 | A | 0.5 | DAG chạy sau `reference_rebuild` |
| P3-08 | Runbook truy vết một con số (gold → silver → raw → URL) theo [03-how-it-works.md](03-how-it-works.md) mục 10, thử với 3 con số | [03-how-it-works](03-how-it-works.md) | P3-05 | B | 0.5 | Người không viết code làm theo runbook truy được trong < 10 phút |

### 7.2 Gate G3 (kết thúc prototype)

- [ ] Mọi tiêu chí ở [01-problem.md](01-problem.md) mục 5 được đánh dấu, mỗi tiêu chí có bằng chứng.
- [ ] Lợi nhuận tổng của SPY 2016–2025 tính từ `bars_1d` lệch < 0.1%/năm so với `adj_close` của Yahoo.
- [ ] `get_bars(["BTCUSDT"], ..., freq="1h")` khớp tự resample bằng pandas.
- [ ] Agent gọi được API từ môi trường của agent (một notebook mẫu chạy đầu-cuối). Notebook in kèm `get_coverage("SP500", ...)` cho giai đoạn backtest.
- [ ] Query `get_bars` 1 năm daily cho 500 mã chạy < 5 giây trên bee với `memory_limit = 2GB`.
- [ ] Tuần ổn định: mọi DAG chạy 1 tuần liền không sửa tay.

---

## 8. Phase 4: mở rộng (sau prototype)

Mỗi nguồn hoặc mỗi mở rộng là một task riêng, theo mẫu của nguồn đã có. Gate của từng task: fixture test, DAG chạy 5 ngày, đối chiếu tay 5 điểm, disk đo lại.

| ID | Task | Spec | Điều kiện |
|---|---|---|---|
| P4-01 | `eia_series` | [11-eia](specs/07-ingestion/02-sources/11-eia.md) | |
| P4-02 | `bls_series` | [12-bls](specs/07-ingestion/02-sources/12-bls.md) | |
| P4-03 | `bea_nipa` | [13-bea](specs/07-ingestion/02-sources/13-bea.md) | |
| P4-04 | `ecb_series` | [14-ecb](specs/07-ingestion/02-sources/14-ecb.md) | |
| P4-05 | `worldbank_indicator` | [15-worldbank](specs/07-ingestion/02-sources/15-worldbank.md) | |
| P4-06 | `imf_sdmx`, `imf_weo` | [16-imf](specs/07-ingestion/02-sources/16-imf.md) | |
| P4-07 | `oecd_series` | [17-oecd](specs/07-ingestion/02-sources/17-oecd.md) | |
| P4-08 | `sec_companyfacts` + `gold.fundamentals_pit` | [05-sec_edgar](specs/07-ingestion/02-sources/05-sec_edgar.md), [04-data-model](04-data-model.md) mục 6 | Cần thêm disk |
| P4-09 | Minute US cho toàn bộ S&P 500 từ 2016 | [02-alpaca](specs/07-ingestion/02-sources/02-alpaca.md) | Cần thêm ~100 GB disk |
| P4-10 | Crypto từ 2018 | [01-binance](specs/07-ingestion/02-sources/01-binance.md) | Đo disk sau P1-10 |

Thứ tự trong phase 4 theo nhu cầu của agent.

---

## 9. Cắt phạm vi nếu trễ

Theo thứ tự, cắt cái trên trước. Phần bị cắt chuyển sang phase 4, không bỏ.

| Thứ tự | Cắt | Ảnh hưởng |
|---|---|---|
| 1 | `fred_wide` (~500 series không vintage) | Agent chỉ có ~30 series macro chính |
| 2 | `nyfed_reference_rates`, `treasury_real_yield` | Lãi suất vẫn có qua FRED (không có vintage riêng của nguồn gốc) |
| 3 | `get_factors`, `french_factors` | Agent tự tính factor đơn giản từ giá |
| 4 | `gold.bars_1m` | Agent đọc minute qua daily trước; minute dùng sau |
| 5 | Tự động sinh `sp500_daily.yaml` | Dùng danh sách tĩnh lấy từ `index_membership` một lần |

Không cắt: các test PIT, check `referential`, rebuild offline, alert. Đây là phần đảm bảo agent không học từ dữ liệu sai.

---

## 10. Rủi ro

| Rủi ro | Dấu hiệu sớm | Phương án |
|---|---|---|
| Airflow 3 + `external_python` khó dựng, hoặc tốn RAM hơn dự tính | P0-11 trễ quá 1 ngày | Tạm chạy bằng cron + CLI (CLI đã đủ để chạy mọi việc), giữ Airflow cho sau |
| Disk thực tế vượt ước tính | Số đo ở G0, G1 | Giảm universe minute pilot, rút lịch sử minute về 2025, xin thêm disk sớm |
| Yahoo thiếu giá nhiều mã đã hủy niêm yết (dữ liệu khuyết, không phải sai) | Độ phủ đo ở G2, check `price_coverage` hằng ngày từ phase 3 | Mặc định: vẫn dùng Yahoo, đánh dấu `has_price = false`, ghi độ phủ vào catalog, API phát `SurvivorshipWarning`. Năm nào dưới 70% thì alert leader để cân nhắc tìm nguồn bổ sung ở phase 4. Không chặn pipeline |
| Nguồn đổi format hoặc chặn bot | `SourceFormatChanged`, live smoke test fail | Fixture giữ hành vi cũ; sửa connector; nếu nguồn chết hẳn thì tìm nguồn thay ở [02-architecture.md](02-architecture.md) mục 6 |
| ALFRED giới hạn request khi backfill vintage | Lỗi 429 dày | Giảm `max_workers`, backfill theo nhiều đêm |
| Task OOM trên bee | Task bị kill, RAM đỉnh đo ở G0 | Giới hạn kích thước file Parquet theo tháng (đã có), giảm `parallelism` |

### Rủi ro đã chấp nhận tại gate

Ghi lại ở đây khi một điều kiện gate không đạt nhưng vẫn quyết định đi tiếp.

| Gate | Điều kiện chưa đạt | Lý do chấp nhận | Người quyết | Ngày |
|---|---|---|---|---|
| | | | | |
