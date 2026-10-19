from alphadata.ingestion.pipeline import register
from alphadata.ingestion.sources.binance.klines import BinanceSpotKlines1m

register(BinanceSpotKlines1m())
