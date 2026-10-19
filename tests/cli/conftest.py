import hashlib
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
import responses
from click.testing import CliRunner, Result

from alphadata.cli.main import cli
from alphadata.core import http
from alphadata.ingestion.sources.binance.klines import kline_url

BINANCE_FIXTURES = (
    Path(__file__).resolve().parents[1] / "ingestion" / "sources" / "binance" / "fixtures"
)


@pytest.fixture(autouse=True)
def lake_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Lake, metadata SQLite và thư mục universe tạm; chdir để không đọc .env của repo."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALPHADATA_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("ALPHADATA_META_DB_URL", f"sqlite:///{(tmp_path / 'meta.db').as_posix()}")
    monkeypatch.setenv("ALPHADATA_MIN_FREE_GB", "0.001")
    monkeypatch.setenv("ALPHADATA_UNIVERSE_DIR", str(tmp_path / "universe"))
    monkeypatch.setattr(http, "_sleep", lambda seconds: None)
    return tmp_path


@pytest.fixture
def invoke() -> Callable[..., Result]:
    runner = CliRunner()

    def run(*args: str) -> Result:
        return runner.invoke(cli, list(args), catch_exceptions=False)

    return run


@pytest.fixture
def http_mock() -> Iterator[responses.RequestsMock]:
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        yield mock


@pytest.fixture
def serve_day(http_mock: responses.RequestsMock) -> Callable[..., None]:
    """Phục vụ file ngày của Binance: dữ liệu thật cắt ngắn, hoặc 404 / checksum sai."""

    def serve(symbol: str, day: str, *, status: int = 200, bad_checksum: bool = False) -> None:
        url = kline_url(symbol, day)
        if status != 200:
            http_mock.get(url, status=status)
            return
        content = (BINANCE_FIXTURES / "BTCUSDT-1m-2025-01-01.zip").read_bytes()
        digest = "0" * 64 if bad_checksum else hashlib.sha256(content).hexdigest()
        http_mock.get(url, body=content)
        http_mock.get(url + ".CHECKSUM", body=f"{digest}  {url.rsplit('/', 1)[1]}\n")

    return serve


@pytest.fixture
def write_universe(lake_env: Path) -> Callable[[str, str], Path]:
    def write(name: str, text: str) -> Path:
        directory = lake_env / "universe"
        directory.mkdir(exist_ok=True)
        path = directory / f"{name}.yaml"
        path.write_text(text, encoding="utf-8")
        return path

    return write
