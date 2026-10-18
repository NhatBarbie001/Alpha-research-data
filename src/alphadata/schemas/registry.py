from alphadata.schemas import silver
from alphadata.schemas.types import Layer, TableSchema

_ALL: tuple[TableSchema, ...] = (*silver.SCHEMAS,)
_BY_NAME: dict[str, TableSchema] = {s.name: s for s in _ALL}
if len(_BY_NAME) != len(_ALL):
    raise RuntimeError("duplicate table names in schemas")


def get_schema(name: str) -> TableSchema:
    try:
        return _BY_NAME[name]
    except KeyError:
        raise KeyError(f"unknown table {name!r}; known: {sorted(_BY_NAME)}") from None


def list_schemas(layer: Layer | None = None) -> list[TableSchema]:
    return [s for s in _ALL if layer is None or s.layer == layer]
