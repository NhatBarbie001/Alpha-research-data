"""Import mọi sub package nguồn để chúng tự đăng ký dataset (xem pipeline/registry.py)."""

from alphadata.ingestion.sources import binance

__all__ = ["binance"]
