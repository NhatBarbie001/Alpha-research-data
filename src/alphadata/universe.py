"""Universe của giai đoạn pilot. Sửa danh sách ở đây, CLI và Airflow DAG đều đọc từ file này."""

PILOT_CRYPTO = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

PILOT_YAHOO = [
    "SPY", "QQQ", "IWM", "TLT", "GLD",  # ETF
    "^GSPC", "^VIX",                    # index
    "AAPL", "MSFT", "NVDA",             # cổ phiếu
]

# series_id -> mô tả ngắn (tên đầy đủ, đơn vị xem trên fred.stlouisfed.org/series/<ID>)
PILOT_FRED = {
    "DGS10": "Lợi suất trái phiếu chính phủ Mỹ 10 năm, %, daily",
    "DGS2": "Lợi suất trái phiếu chính phủ Mỹ 2 năm, %, daily",
    "T10Y2Y": "Chênh lệch lợi suất 10 năm - 2 năm, %, daily",
    "FEDFUNDS": "Lãi suất Fed funds hiệu dụng, %, monthly",
    "CPIAUCSL": "Chỉ số giá tiêu dùng CPI, đã điều chỉnh mùa vụ, monthly",
    "UNRATE": "Tỷ lệ thất nghiệp, %, monthly",
    "GDP": "GDP danh nghĩa, tỷ USD, quarterly",
    "DCOILWTICO": "Giá dầu WTI spot, USD/thùng, daily",
    "DTWEXBGS": "Chỉ số USD broad (trade-weighted), daily",
    "VIXCLS": "Chỉ số VIX đóng cửa, daily",
}
