import re

BASE_URL = "https://data.binance.vision/data/"
HOST_RATE = 5.0  # request/giây; data.binance.vision không công bố giới hạn nên tự giới hạn
SOURCE_URL = "https://data.binance.vision"
LICENSE_NOTE = "Dữ liệu công khai Binance phát hành để tải về; chỉ dùng nội bộ"

# Mã cặp chỉ gồm chữ hoa và số: đi thẳng vào URL nên không nhận ký tự khác.
SYMBOL_RE = re.compile(r"^[A-Z0-9]{5,20}$")

# File klines không có header (một số file cũ có): 12 cột theo thứ tự này.
KLINE_COLUMNS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_base",
    "taker_buy_quote",
    "ignore",
)
MINUTES_PER_DAY = 1440

# File tháng được đăng đầu tháng sau; chờ thêm vài ngày rồi mới dùng file tháng khi backfill.
MONTHLY_PUBLISH_DELAY_DAYS = 5
