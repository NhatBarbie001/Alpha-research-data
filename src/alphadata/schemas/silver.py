from alphadata.schemas.types import Column, TableSchema

CRYPTO_BARS_1M = TableSchema(
    name="crypto_bars_1m",
    layer="silver",
    columns=(
        Column("symbol", "string"),
        Column("ts_utc", "timestamp"),
        Column("open", "float64"),
        Column("high", "float64"),
        Column("low", "float64"),
        Column("close", "float64"),
        Column("volume", "float64"),
        Column("quote_volume", "float64"),
        Column("trades", "int64"),
        Column("taker_buy_base", "float64"),
        Column("taker_buy_quote", "float64"),
    ),
    key=("symbol", "ts_utc"),
    partition=("symbol", "year", "month"),
    partition_time="ts_utc",
    sort=("ts_utc",),
)

SCHEMAS: tuple[TableSchema, ...] = (CRYPTO_BARS_1M,)
