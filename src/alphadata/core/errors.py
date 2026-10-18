class AlphadataError(Exception):
    """Gốc của mọi lỗi do alphadata chủ động raise."""


class NotAvailableYet(AlphadataError):
    """Nguồn chưa đăng dữ liệu cho partition. Orchestration retry sau."""


class MissingCredential(AlphadataError):
    """Thiếu API key. Fail ngay, không retry."""


class SourceFormatChanged(AlphadataError):
    """Dữ liệu nguồn trả về khác cấu trúc mong đợi. Fail và alert."""


class DiskFullError(AlphadataError):
    """Disk trống dưới ngưỡng. Fail và alert, không retry."""


class DataQualityError(AlphadataError):
    """Check mức error không qua. Fail và alert."""


class SchemaMismatch(AlphadataError):
    """DataFrame không khớp hợp đồng bảng: thiếu/thừa cột, sai kiểu, null ở cột không null."""


class HttpStatusError(AlphadataError):
    """HTTP trả mã lỗi không retry được, hoặc hết lượt retry. `url` đã che key."""

    def __init__(self, status_code: int, url: str) -> None:
        super().__init__(f"HTTP {status_code} for {url}")
        self.status_code = status_code
        self.url = url


class HttpRequestError(AlphadataError):
    """Lỗi mạng (kết nối, timeout) sau khi hết lượt retry. Message đã che key."""
