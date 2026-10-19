from collections.abc import Sequence
from datetime import date, datetime


def cell(value: object, max_width: int = 80) -> str:
    if value is None:
        text = ""
    elif isinstance(value, datetime):
        text = value.isoformat(sep=" ", timespec="seconds")
    elif isinstance(value, date):
        text = value.isoformat()
    elif isinstance(value, float):
        text = f"{value:.10g}"
    elif isinstance(value, (list, tuple)):
        text = ",".join(str(v) for v in value)
    else:
        text = str(value)
    text = text.replace("\n", " ")
    return text if len(text) <= max_width else text[: max_width - 1] + "…"


def format_table(
    columns: Sequence[str], rows: Sequence[Sequence[object]], max_width: int = 80
) -> str:
    """Bảng chữ căn cột cho stdout (dễ đọc và dễ grep, không cần thư viện)."""
    cells = [[cell(v, max_width) for v in row] for row in rows]
    widths = [len(c) for c in columns]
    for row in cells:
        widths = [max(w, len(v)) for w, v in zip(widths, row, strict=True)]
    lines = [
        "  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True)).rstrip(),
        "  ".join("-" * w for w in widths),
    ]
    lines += [
        "  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)).rstrip() for row in cells
    ]
    return "\n".join(lines)
