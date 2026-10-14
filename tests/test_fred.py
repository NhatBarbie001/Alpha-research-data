import json
from datetime import date

from alphadata.query import connect, macro_as_of
from alphadata.sources.fred import FredSeries
from alphadata.storage import save_raw, write_parquet_atomic

# GDP quý 1/2026: công bố lần đầu 100.0 ngày 2026-04-30, revise thành 101.0 ngày 2026-05-28
PAYLOAD = {"observations": [
    {"realtime_start": "2026-04-30", "realtime_end": "2026-05-27", "date": "2026-01-01", "value": "100.0"},
    {"realtime_start": "2026-05-28", "realtime_end": "9999-12-31", "date": "2026-01-01", "value": "101.0"},
    {"realtime_start": "2026-07-30", "realtime_end": "9999-12-31", "date": "2026-04-01", "value": "."},
]}


def _normalize(tmp_path):
    raw = save_raw(tmp_path / "raw", json.dumps(PAYLOAD).encode(), "GDP.json", "http://test")
    return FredSeries().normalize([raw], {"series_id": "gdp"})


def test_normalize_keeps_vintages_and_drops_missing(tmp_path):
    df = _normalize(tmp_path)
    assert len(df) == 2  # giá trị '.' bị bỏ
    assert df["available_to"].iloc[1] == date(9999, 12, 31)
    assert df["series_id"].unique().tolist() == ["GDP"]


def test_point_in_time_query(tmp_path, settings):
    df = _normalize(tmp_path)
    write_parquet_atomic(df, FredSeries().silver_path(settings.silver_dir, {"series_id": "GDP"}))
    con = connect(settings)

    assert macro_as_of(con, "GDP", "2026-04-29").empty               # chưa công bố
    assert macro_as_of(con, "GDP", "2026-05-01")["value"].tolist() == [100.0]  # bản đầu tiên
    assert macro_as_of(con, "GDP", "2026-06-01")["value"].tolist() == [101.0]  # sau revise
