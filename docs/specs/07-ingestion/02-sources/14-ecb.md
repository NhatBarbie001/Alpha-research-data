# Spec nguồn: ECB Data Portal

> Code: `src/alphadata/ingestion/sources/ecb/` · Ưu tiên: **P3** · Trạng thái: **Chốt dùng**
> Kiểm tra 2026-10-15: ✅ `data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata` trả CSV tỷ giá.

## Vai trò

Tỷ giá tham chiếu EUR chính thức (~30 đồng tiền) và lãi suất khu vực euro (€STR, lãi suất điều hành ECB). FRED có một số cặp `DEX*` nhưng ít đồng tiền hơn.

## Truy cập

| Mục | Giá trị |
|---|---|
| Endpoint | `https://data-api.ecb.europa.eu/service/data/<flow>/<key>?format=csvdata&startPeriod=YYYY-MM-DD` |
| Key | Không cần |
| Định dạng | SDMX, chọn `csvdata` để dễ parse |
| Điều khoản | Dữ liệu công khai, được dùng lại khi ghi nguồn ECB |

## Dataset

| Dataset | Bảng silver | Partition |
|---|---|---|
| `ecb_series` | `macro_observations` | series_key |

Universe `ecb_core.yaml` (đề xuất):

| Series key | Nội dung |
|---|---|
| `EXR/D.<CCY>.EUR.SP00.A` | Tỷ giá tham chiếu daily, CCY ∈ USD, JPY, GBP, CHF, CNY, AUD, CAD, ... |
| `EST/B.EU000A2X2A25.WT` | €STR (lãi suất qua đêm khu vực euro) |
| `FM/D.U2.EUR.4F.KR.DFR.LEV` | Lãi suất deposit facility của ECB |

Mã series chính xác kiểm tra lại trên data.ecb.europa.eu khi viết connector.

## Normalize

- Cột dùng: `KEY`, `TIME_PERIOD`, `OBS_VALUE`, `OBS_STATUS`. `series_id = KEY`.
- `vintage_kind = "observed"`. Tỷ giá tham chiếu công bố khoảng 16:00 CET cùng ngày: khi backfill `available_from = obs_date` (cuối ngày). An toàn hơn thì dùng `+1 ngày`; chọn `+1 ngày`.

## Lịch

16:00 UTC thứ Hai–thứ Sáu.

## Done when

- Tỷ giá USD/EUR từ 1999 khớp FRED `DEXUSEU` (nghịch đảo, sai khác do giờ chốt khác nhau được ghi nhận).
