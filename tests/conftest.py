import io
import zipfile

import pytest

from alphadata.config import Settings


@pytest.fixture
def settings(tmp_path):
    return Settings(
        data_root=tmp_path,
        meta_db_url=f"sqlite:///{(tmp_path / 'meta.db').as_posix()}",
        min_free_gb=0,
        http_timeout=5,
    )


def binance_zip(start_ms: int, n: int, unit: str = "ms", name: str = "X.csv") -> bytes:
    """Tạo file zip giống định dạng của data.binance.vision (không header)."""
    mult = 1000 if unit == "us" else 1
    lines = []
    for i in range(n):
        t = (start_ms + i * 60_000) * mult
        price = 100 + i
        lines.append(f"{t},{price},{price + 1},{price - 1},{price + 0.5},1.5,{t + 59_999 * mult},"
                     f"150.0,10,0.7,70.0,0")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(name, "\n".join(lines) + "\n")
    return buf.getvalue()
