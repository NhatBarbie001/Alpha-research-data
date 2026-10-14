from alphadata.sources.base import Dataset
from alphadata.sources.binance import BinanceSpotKlines1m
from alphadata.sources.fred import FredSeries
from alphadata.sources.yahoo import YahooDaily

DATASETS: dict[str, Dataset] = {d.name: d for d in [BinanceSpotKlines1m(), FredSeries(), YahooDaily()]}


def get_dataset(name: str) -> Dataset:
    try:
        return DATASETS[name]
    except KeyError:
        raise KeyError(f"Không có dataset '{name}'. Có: {sorted(DATASETS)}") from None
