# Giá trị hợp lệ của cột enum trong lake, theo docs/04-data-model.md mục 8.
# Enum của bảng metadata Postgres nằm ở module metadata.
ENUMS: dict[str, frozenset[str]] = {
    "session": frozenset({"pre", "regular", "post"}),
    "action_type": frozenset({"split", "dividend", "spinoff", "symbol_change"}),
    "frequency": frozenset({"D", "W", "M", "Q", "A"}),
    "vintage_kind": frozenset({"official", "observed"}),
    "asset_class": frozenset({"equity", "etf", "index", "crypto"}),
    "security_status": frozenset({"active", "delisted", "unknown"}),
    "symbol_source": frozenset({"canonical", "yahoo", "alpaca", "binance"}),
    "corporate_action_status": frozenset({"confirmed", "single_source", "conflict"}),
    "universe_code": frozenset({"SP500", "US_ETF_CORE", "US_MINUTE_PILOT", "CRYPTO_PILOT"}),
}
