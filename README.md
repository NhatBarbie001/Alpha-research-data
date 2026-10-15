# alphadata: tầng data cho Alpha research agent

Crawl dữ liệu thị trường và macro từ nguồn miễn phí → lưu bản gốc (bronze) → chuẩn hoá, kiểm tra chất lượng (silver, Parquet) → bảng sẵn cho research, point-in-time (gold) → agent đọc qua API read-only. Airflow chạy theo lịch trên server bee.

> Trạng thái: **đang viết lại theo kiến trúc mới.** Code cũ (commit `68636b0`) đã bị xoá; repo hiện chỉ có tài liệu.

## Tài liệu

- [docs/architecture.md](docs/architecture.md): kiến trúc tổng thể, ràng buộc, cấu trúc thư mục, **danh sách nguồn đã chốt**, lộ trình, câu hỏi cho leader.
- [docs/specs/](docs/specs/): mỗi module hoặc sub module một spec, cấu trúc giống `src/alphadata/`.

| Nhóm | Spec |
|---|---|
| Xuyên suốt | [core](docs/specs/core.md) · [storage](docs/specs/storage.md) · [metadata](docs/specs/metadata.md) · [quality](docs/specs/quality.md) · [schemas](docs/specs/schemas.md) · [universe](docs/specs/universe.md) |
| Ingestion | [pipeline](docs/specs/ingestion/pipeline.md) |
| Nguồn P1 | [binance](docs/specs/ingestion/sources/binance.md) · [alpaca](docs/specs/ingestion/sources/alpaca.md) · [yahoo](docs/specs/ingestion/sources/yahoo.md) · [fred](docs/specs/ingestion/sources/fred.md) |
| Nguồn P2 | [sec_edgar](docs/specs/ingestion/sources/sec_edgar.md) · [sp500_constituents](docs/specs/ingestion/sources/sp500_constituents.md) · [nasdaq_trader](docs/specs/ingestion/sources/nasdaq_trader.md) · [french](docs/specs/ingestion/sources/french.md) · [treasury](docs/specs/ingestion/sources/treasury.md) · [nyfed](docs/specs/ingestion/sources/nyfed.md) |
| Nguồn P3, P4 | [eia](docs/specs/ingestion/sources/eia.md) · [bls](docs/specs/ingestion/sources/bls.md) · [bea](docs/specs/ingestion/sources/bea.md) · [ecb](docs/specs/ingestion/sources/ecb.md) · [worldbank](docs/specs/ingestion/sources/worldbank.md) · [imf](docs/specs/ingestion/sources/imf.md) · [oecd](docs/specs/ingestion/sources/oecd.md) |
| Reference | [security_master](docs/specs/reference/security_master.md) · [trading_calendar](docs/specs/reference/trading_calendar.md) · [corporate_actions](docs/specs/reference/corporate_actions.md) · [index_membership](docs/specs/reference/index_membership.md) |
| Gold | [bars](docs/specs/gold/bars.md) · [universe_daily](docs/specs/gold/universe_daily.md) · [macro_pit](docs/specs/gold/macro_pit.md) |
| Access, vận hành | [access](docs/specs/access.md) · [orchestration](docs/specs/orchestration.md) · [cli](docs/specs/cli.md) · [deploy](docs/specs/deploy.md) |

## Quy ước

- Thêm module mới thì viết spec trước, đặt trong `docs/specs/` theo đúng vị trí của module trong `src/alphadata/`.
- Thêm nguồn mới: xem mục "Thêm một nguồn mới" trong [ingestion/pipeline.md](docs/specs/ingestion/pipeline.md).
- Chỉ dùng nguồn miễn phí, gọi được bằng chương trình, điều khoản cho phép dùng nội bộ. Nguồn bị loại và lý do ở [architecture.md](docs/architecture.md) mục 7.2.
- Không commit API key, không commit data.
