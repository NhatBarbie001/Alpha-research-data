import io
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from alphadata.core.errors import NotAvailableYet
from alphadata.core.settings import Settings
from alphadata.core.timeutil import epoch_to_utc
from alphadata.ingestion.pipeline import Dataset, FetchContext, Partition
from alphadata.metadata import MetaStore
from alphadata.quality import Check, default_checks, ohlc_consistent, row_count, ts_in_range
from alphadata.storage.raw import RawFile

MINUTES_PER_DAY = 1440


def write_remote_day(remote: Path, symbol: str, day: str, minutes: int = MINUTES_PER_DAY) -> Path:
    """File CSV giả kiểu Binance: open_time tính bằng mili-giây."""
    start = datetime.fromisoformat(day).replace(tzinfo=UTC)
    open_ms = [int((start + timedelta(minutes=i)).timestamp() * 1000) for i in range(minutes)]
    df = pd.DataFrame(
        {
            "open_time": open_ms,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 2.0,
            "quote_volume": 201.0,
            "trades": 10,
            "taker_buy_base": 1.0,
            "taker_buy_quote": 100.5,
        }
    )
    remote.mkdir(parents=True, exist_ok=True)
    path = remote / f"{symbol}-{day}.csv"
    df.to_csv(path, index=False)
    return path


class FakeBars(Dataset):
    name = "fake_klines"
    source = "fake"
    table = "crypto_bars_1m"
    description = "Nến 1 phút giả để test pipeline"
    source_url = "https://fake.example"
    license_note = "test only"
    partition_keys = ("symbol", "date")

    def __init__(self, remote: Path, *, keep_raw: bool = True) -> None:
        self.remote = remote
        self.keep_raw = keep_raw
        self.fetch_calls = 0
        self.normalize_hook: Callable[[pd.DataFrame], pd.DataFrame] | None = None
        self.interrupt_at: int | None = None  # giả lập bị kill (Ctrl-C) ở lần fetch thứ N

    def backfill_partitions(self, member: str, start: date, end: date) -> list[Partition]:
        days = (end - start).days + 1
        return [
            {"symbol": member, "date": (start + timedelta(days=i)).isoformat()} for i in range(days)
        ]

    def fetch(self, ctx: FetchContext, partition: Partition) -> list[RawFile]:
        self.fetch_calls += 1
        if self.interrupt_at == self.fetch_calls:
            raise KeyboardInterrupt
        path = self.remote / f"{partition['symbol']}-{partition['date']}.csv"
        if not path.is_file():
            raise NotAvailableYet(f"{path.name} not published yet")
        url = f"https://fake.example/{path.name}?api_key=SECRET123"
        return [ctx.save(path.read_bytes(), path.name, url)]

    def normalize(self, raws: list[RawFile], partition: Partition) -> pd.DataFrame:
        frames = [pd.read_csv(io.BytesIO(self.read(raw))) for raw in raws]
        df = pd.concat(frames, ignore_index=True)
        df.insert(0, "symbol", partition["symbol"])
        df.insert(1, "ts_utc", epoch_to_utc(df.pop("open_time")))
        return self.normalize_hook(df) if self.normalize_hook else df

    def checks(self, partition: Partition) -> list[Check]:
        start = datetime.fromisoformat(partition["date"]).replace(tzinfo=UTC)
        return [
            *default_checks(self.schema),
            ts_in_range("ts_utc", start, start + timedelta(days=1)),
            row_count(MINUTES_PER_DAY),
            ohlc_consistent(),
        ]


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    root = tmp_path / "lake"
    return Settings(
        data_root=root,
        meta_db_url=f"sqlite:///{(tmp_path / 'meta.db').as_posix()}",
        min_free_gb=0.001,
    )


@pytest.fixture
def meta(settings: Settings) -> Iterator[MetaStore]:
    store = MetaStore(settings.meta_db_url)
    yield store
    store.close()


@pytest.fixture
def remote(tmp_path: Path) -> Path:
    return tmp_path / "remote"


@pytest.fixture
def dataset(remote: Path) -> FakeBars:
    return FakeBars(remote)


@pytest.fixture
def write_day(remote: Path) -> Callable[..., Path]:
    def write(symbol: str, day: str, minutes: int = MINUTES_PER_DAY) -> Path:
        return write_remote_day(remote, symbol, day, minutes)

    return write


@pytest.fixture
def fake_cls() -> type[FakeBars]:
    return FakeBars
