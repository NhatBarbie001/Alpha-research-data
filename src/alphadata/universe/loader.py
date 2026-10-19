"""Phạm vi crawl: mã/series nào, cho dataset nào. Đọc từ config/universe/<tên>.yaml."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from alphadata.core.errors import AlphadataError
from alphadata.core.settings import get_settings

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_TOP_FIELDS = {"name", "datasets", "key", "start", "members", "note"}
_REQUIRED_TOP = {"name", "datasets", "key", "members"}
_MEMBER_FIELDS = {"id", "start", "end", "note"}


class InvalidUniverse(AlphadataError):
    """File universe sai format. Message có tên file."""


@dataclass(frozen=True)
class Member:
    id: str
    start: date | None = None
    end: date | None = None  # cả hai đầu đều tính
    note: str = ""

    def active_on(self, day: date) -> bool:
        return (self.start is None or self.start <= day) and (self.end is None or day <= self.end)


@dataclass(frozen=True)
class Universe:
    name: str
    datasets: tuple[str, ...]
    key: str  # khoá partition mà member điền vào, ví dụ "symbol"
    members: tuple[Member, ...]
    start: date | None = None  # ngày sớm nhất cần backfill (member có thể ghi đè)
    note: str = ""

    def member_ids(self, on: date | None = None) -> list[str]:
        return [m.id for m in self.members if on is None or m.active_on(on)]

    def partitions(
        self, extra: Mapping[str, str] | None = None, *, on: date | None = None
    ) -> list[dict[str, str]]:
        """Mỗi member một partition: {key: id, **extra}. Có `on` thì bỏ member không hoạt động."""
        extra = dict(extra or {})
        if self.key in extra:
            raise ValueError(f"{self.key!r} comes from the universe members; do not pass it")
        return [{self.key: member_id, **extra} for member_id in self.member_ids(on)]


def universe_dir(directory: Path | None = None) -> Path:
    return directory if directory is not None else get_settings().universe_dir


def list_universes(directory: Path | None = None) -> list[str]:
    root = universe_dir(directory)
    return sorted(p.stem for p in root.glob("*.yaml")) if root.is_dir() else []


def load(name: str, directory: Path | None = None) -> Universe:
    if not _NAME_RE.fullmatch(name):
        raise ValueError(f"invalid universe name {name!r} (expected snake_case)")
    path = universe_dir(directory) / f"{name}.yaml"
    if not path.is_file():
        known = list_universes(directory)
        raise KeyError(f"unknown universe {name!r} in {path.parent}; known: {known}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (yaml.YAMLError, ValueError) as exc:
        # ValueError: YAML gặp chuỗi giống ngày nhưng sai (ví dụ 2024-13-01).
        raise InvalidUniverse(f"{path.name}: invalid YAML ({exc})") from exc
    return parse(data, path.name, expected_name=name)


def partitions(
    name: str,
    extra: Mapping[str, str] | None = None,
    *,
    on: date | None = None,
    directory: Path | None = None,
) -> list[dict[str, str]]:
    return load(name, directory).partitions(extra, on=on)


def parse(data: Any, source: str, expected_name: str | None = None) -> Universe:
    """Kiểm và dựng Universe từ nội dung YAML đã đọc. `source` dùng trong message lỗi."""

    def fail(message: str) -> InvalidUniverse:
        return InvalidUniverse(f"{source}: {message}")

    if not isinstance(data, Mapping):
        raise fail("top level must be a mapping")
    missing = sorted(_REQUIRED_TOP - set(data))
    unknown = sorted(set(data) - _TOP_FIELDS)
    if missing or unknown:
        raise fail(f"missing fields {missing}, unknown fields {unknown}")

    name = data["name"]
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise fail(f"name must be snake_case, got {name!r}")
    if expected_name is not None and name != expected_name:
        raise fail(f"name {name!r} must match the file name {expected_name!r}")
    datasets = data["datasets"]
    if (
        not isinstance(datasets, list)
        or not datasets
        or not all(isinstance(d, str) and _NAME_RE.fullmatch(d) for d in datasets)
        or len(set(datasets)) != len(datasets)
    ):
        raise fail(
            f"datasets must be a non-empty list of unique snake_case names, got {datasets!r}"
        )
    key = data["key"]
    if not isinstance(key, str) or not _NAME_RE.fullmatch(key):
        raise fail(f"key must be snake_case, got {key!r}")

    raw_members = data["members"]
    if not isinstance(raw_members, list) or not raw_members:
        raise fail("members must be a non-empty list")
    members = tuple(_member(m, i, fail) for i, m in enumerate(raw_members))
    ids = [m.id for m in members]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    if duplicates:
        raise fail(f"duplicate members {duplicates}")

    return Universe(
        name=name,
        datasets=tuple(datasets),
        key=key,
        members=members,
        start=_date(data.get("start"), "start", fail),
        note=_text(data.get("note"), "note", fail),
    )


def _member(raw: Any, index: int, fail: Any) -> Member:
    where = f"members[{index}]"
    if isinstance(raw, str):
        raw = {"id": raw}
    if isinstance(raw, bool | int | float | date):
        raise fail(
            f'{where} = {raw!r} is not text: YAML converted it; quote the id, e.g. "ON" or "0700"'
        )
    if not isinstance(raw, Mapping):
        raise fail(f"{where} must be a string or a mapping with 'id'")
    unknown = sorted(set(raw) - _MEMBER_FIELDS)
    if unknown:
        raise fail(f"{where} has unknown fields {unknown}")
    member_id = raw.get("id")
    if not isinstance(member_id, str) or not member_id.strip() or member_id != member_id.strip():
        raise fail(f"{where}.id must be a non-empty string without surrounding spaces")
    start = _date(raw.get("start"), f"{where}.start", fail)
    end = _date(raw.get("end"), f"{where}.end", fail)
    if start is not None and end is not None and start > end:
        raise fail(f"{where} ({member_id}): start {start} is after end {end}")
    return Member(member_id, start, end, _text(raw.get("note"), f"{where}.note", fail))


def _date(value: Any, field: str, fail: Any) -> date | None:
    # YAML tự đổi 2024-01-01 thành date; chuỗi trong ngoặc kép thì tự parse.
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    raise fail(f"{field} must be a date YYYY-MM-DD, got {value!r}")


def _text(value: Any, field: str, fail: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise fail(f"{field} must be text, got {value!r}")
    return value
