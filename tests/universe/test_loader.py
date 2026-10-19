from datetime import date
from pathlib import Path

import pytest

from alphadata.universe import InvalidUniverse, list_universes, load, partitions

VALID = """
name: demo
datasets: [binance_spot_klines_1m]
key: symbol
start: 2024-01-01
members:
  - BTCUSDT
  - {id: ETHUSDT, note: "Ethereum"}
  - {id: LUNAUSDT, start: 2024-03-01, end: "2024-05-31"}
"""
HEAD = "name: demo\ndatasets: [d]\nkey: symbol\n"


def write(directory: Path, name: str, text: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_load_valid(tmp_path: Path) -> None:
    write(tmp_path, "demo", VALID)
    u = load("demo", tmp_path)
    assert (u.name, u.datasets, u.key, u.start) == (
        "demo",
        ("binance_spot_klines_1m",),
        "symbol",
        date(2024, 1, 1),
    )
    assert u.member_ids() == ["BTCUSDT", "ETHUSDT", "LUNAUSDT"]
    luna = u.members[2]
    assert (luna.start, luna.end) == (date(2024, 3, 1), date(2024, 5, 31))
    assert u.members[1].note == "Ethereum"


def test_partitions_filter_by_day_inclusive(tmp_path: Path) -> None:
    write(tmp_path, "demo", VALID)
    u = load("demo", tmp_path)
    assert u.partitions({"period": "2026-10-11"}) == [
        {"symbol": "BTCUSDT", "period": "2026-10-11"},
        {"symbol": "ETHUSDT", "period": "2026-10-11"},
        {"symbol": "LUNAUSDT", "period": "2026-10-11"},
    ]
    assert u.member_ids(on=date(2024, 2, 29)) == ["BTCUSDT", "ETHUSDT"]
    assert "LUNAUSDT" in u.member_ids(on=date(2024, 3, 1))  # đầu start tính
    assert "LUNAUSDT" in u.member_ids(on=date(2024, 5, 31))  # đầu end tính
    assert "LUNAUSDT" not in u.member_ids(on=date(2024, 6, 1))
    assert partitions(
        "demo", {"period": "2026-01-01"}, on=date(2026, 1, 1), directory=tmp_path
    ) == [
        {"symbol": "BTCUSDT", "period": "2026-01-01"},
        {"symbol": "ETHUSDT", "period": "2026-01-01"},
    ]


def test_partitions_reject_member_key_in_extra(tmp_path: Path) -> None:
    write(tmp_path, "demo", VALID)
    with pytest.raises(ValueError, match="'symbol' comes from the universe"):
        load("demo", tmp_path).partitions({"symbol": "X"})


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("- a\n- b\n", "top level must be a mapping"),
        (HEAD, r"missing fields \['members'\]"),
        (HEAD + "member: [A]\nmembers: [A]\n", r"unknown fields \['member'\]"),
        ("name: other\ndatasets: [d]\nkey: symbol\nmembers: [A]\n", "must match the file name"),
        ("name: Bad Name\ndatasets: [d]\nkey: symbol\nmembers: [A]\n", "name must be snake_case"),
        ("name: demo\ndatasets: d\nkey: symbol\nmembers: [A]\n", "datasets must be"),
        ("name: demo\ndatasets: []\nkey: symbol\nmembers: [A]\n", "datasets must be"),
        ("name: demo\ndatasets: [d, d]\nkey: symbol\nmembers: [A]\n", "datasets must be"),
        ("name: demo\ndatasets: [Bad-Name]\nkey: symbol\nmembers: [A]\n", "datasets must be"),
        ("name: demo\ndatasets: [d]\nkey: Symbol\nmembers: [A]\n", "key must be snake_case"),
        (HEAD + "members: []\n", "non-empty list"),
        (HEAD + "members: [A, B, A]\n", r"duplicate members \['A'\]"),
        (HEAD + "members: [{id: A, sart: 2024-01-01}]\n", r"unknown fields \['sart'\]"),
        (HEAD + "members: [{note: x}]\n", "id must be"),
        (HEAD + "members: [' A']\n", "surrounding spaces"),
        (HEAD + "members: [[A]]\n", "string or a mapping"),
        (
            HEAD + "members: [{id: A, start: 2024-05-01, end: 2024-01-01}]\n",
            "start 2024-05-01 is after end 2024-01-01",
        ),
        (HEAD + "start: 2024-13-01\nmembers: [A]\n", "invalid YAML"),
        (HEAD + "start: soon\nmembers: [A]\n", "start must be a date"),
        (HEAD + "note: 5\nmembers: [A]\n", "note must be text"),
        ("name: demo\nkey: [unclosed\n", "invalid YAML"),
    ],
)
def test_invalid_files(tmp_path: Path, text: str, message: str) -> None:
    write(tmp_path, "demo", text)
    with pytest.raises(InvalidUniverse, match=message) as exc_info:
        load("demo", tmp_path)
    assert str(exc_info.value).startswith("demo.yaml")


@pytest.mark.parametrize("member", ["ON", "0700", "2024-01-01"])
def test_yaml_coercion_of_tickers_is_caught(tmp_path: Path, member: str) -> None:
    # YAML biến ON thành True, 0700 thành số, 2024-01-01 thành date: phải báo, không crawl sai mã.
    write(tmp_path, "demo", f"{HEAD}members: [{member}]\n")
    with pytest.raises(InvalidUniverse, match="quote the id"):
        load("demo", tmp_path)
    write(tmp_path, "demo", f'{HEAD}members: ["{member}"]\n')
    assert load("demo", tmp_path).member_ids() == [member]


def test_unknown_and_unsafe_names(tmp_path: Path) -> None:
    write(tmp_path, "demo", VALID)
    with pytest.raises(KeyError, match=r"known: \['demo'\]"):
        load("nope", tmp_path)
    with pytest.raises(ValueError, match="invalid universe name"):
        load("../demo", tmp_path)


def test_list_universes(tmp_path: Path) -> None:
    assert list_universes(tmp_path / "missing") == []
    write(tmp_path, "b_set", VALID.replace("demo", "b_set"))
    write(tmp_path, "a_set", VALID.replace("demo", "a_set"))
    (tmp_path / "notes.txt").write_text("x", encoding="utf-8")
    assert list_universes(tmp_path) == ["a_set", "b_set"]


def test_default_directory_comes_from_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(tmp_path, "demo", VALID)
    monkeypatch.setenv("ALPHADATA_UNIVERSE_DIR", str(tmp_path))
    assert load("demo").name == "demo"
    assert list_universes() == ["demo"]
