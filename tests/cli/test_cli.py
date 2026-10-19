import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from click.testing import Result

from alphadata.cli import main as cli_main

DAY = "2025-01-01"
Invoke = Callable[..., Result]


def test_help_lists_commands(invoke: Invoke) -> None:
    result = invoke("--help")
    assert result.exit_code == 0
    for command in [
        "run",
        "reprocess",
        "run-universe",
        "datasets",
        "universes",
        "runs",
        "catalog",
        "disk",
        "query",
    ]:
        assert f"  {command} " in result.stdout
    assert "Tầng data" in result.stdout


def test_version(invoke: Invoke) -> None:
    assert invoke("--version").stdout.strip() == "alphadata, version 0.1.0"


def test_python_m_entrypoint() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "alphadata", "--version"], capture_output=True, text=True, check=True
    )
    assert proc.stdout.strip() == "alphadata, version 0.1.0"


# --- run, reprocess ----------------------------------------------------------------


def test_run_success(invoke: Invoke, serve_day: Callable[..., None]) -> None:
    serve_day("BTCUSDT", DAY)
    result = invoke("run", "binance_spot_klines_1m", "symbol=BTCUSDT", f"period={DAY}")
    assert result.exit_code == 0, result.stderr
    out = dict(line.split(None, 1) for line in result.stdout.splitlines())
    assert out["status"] == "success"
    assert out["rows_in"] == "4"
    assert out["silver"].endswith("symbol=BTCUSDT/year=2025/month=1/part.parquet")
    assert out["warnings"].startswith("row_count")


def test_run_not_published_exits_3(invoke: Invoke, serve_day: Callable[..., None]) -> None:
    serve_day("BTCUSDT", DAY, status=404)
    result = invoke("run", "binance_spot_klines_1m", "symbol=BTCUSDT", f"period={DAY}")
    assert result.exit_code == 3
    assert "NotAvailableYet" in result.stderr


@pytest.mark.parametrize(
    "args",
    [
        ["run", "binance_spot_klines_1m", "symbol=btc", f"period={DAY}"],
        ["run", "binance_spot_klines_1m", "symbol=BTCUSDT"],
        ["run", "binance_spot_klines_1m", "symbol", f"period={DAY}"],
        ["run", "binance_spot_klines_1m", "symbol=A", "symbol=B", f"period={DAY}"],
        ["run", "no_such_dataset", "symbol=BTCUSDT"],
        ["reprocess", "no_such_dataset", "symbol=BTCUSDT"],
    ],
    ids=["bad-symbol", "missing-key", "not-kv", "duplicate-key", "unknown-dataset", "reprocess"],
)
def test_bad_arguments_exit_2_without_recording_runs(invoke: Invoke, args: list[str]) -> None:
    result = invoke(*args)
    assert result.exit_code == 2
    assert "Error:" in result.stderr
    assert "binance" not in invoke("runs").stdout


def test_reprocess_minute_data_explains_raw_deleted(
    invoke: Invoke, serve_day: Callable[..., None]
) -> None:
    serve_day("BTCUSDT", DAY)
    invoke("run", "binance_spot_klines_1m", "symbol=BTCUSDT", f"period={DAY}")
    result = invoke("reprocess", "binance_spot_klines_1m", "symbol=BTCUSDT", f"period={DAY}")
    assert result.exit_code == 1
    assert "RawUnavailable" in result.stderr and "keep_raw=False" in result.stderr


def test_unexpected_error_masked_and_verbose_traceback(
    invoke: Invoke, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("failed at https://x?api_key=SECRET123")

    monkeypatch.setattr(cli_main, "run_partition", boom)
    result = invoke("run", "binance_spot_klines_1m", "symbol=BTCUSDT", f"period={DAY}")
    assert result.exit_code == 1
    assert "api_key=***" in result.stderr and "SECRET123" not in result.stderr
    assert "Traceback" not in result.stderr
    verbose = invoke("-v", "run", "binance_spot_klines_1m", "symbol=BTCUSDT", f"period={DAY}")
    assert "Traceback" in verbose.stderr and "SECRET123" not in verbose.stderr


# --- run-universe ------------------------------------------------------------------

PILOT = """
name: pilot
datasets: [binance_spot_klines_1m]
key: symbol
members:
  - BTCUSDT
  - ETHUSDT
  - {id: LUNAUSDT, end: 2022-05-31}
"""


def table(result: Result) -> dict[str, list[str]]:
    rows = [line.split() for line in result.stdout.splitlines()[2:]]
    return {row[0]: row[1:] for row in rows}


def test_run_universe_all_success(
    invoke: Invoke, serve_day: Callable[..., None], write_universe: Callable[[str, str], Path]
) -> None:
    write_universe("pilot", PILOT)
    serve_day("BTCUSDT", DAY)
    serve_day("ETHUSDT", DAY)
    result = invoke("run-universe", "pilot", f"period={DAY}")
    assert result.exit_code == 0, result.stdout + result.stderr
    rows = table(result)
    assert set(rows) == {"BTCUSDT", "ETHUSDT"}  # LUNAUSDT đã hết hoạt động từ 2022
    assert rows["BTCUSDT"][0] == "success"


def test_run_universe_only_not_available_exits_3(
    invoke: Invoke, serve_day: Callable[..., None], write_universe: Callable[[str, str], Path]
) -> None:
    write_universe("pilot", PILOT)
    serve_day("BTCUSDT", DAY)
    serve_day("ETHUSDT", DAY, status=404)
    result = invoke("run-universe", "pilot", f"period={DAY}")
    assert result.exit_code == 3
    assert table(result)["ETHUSDT"][0] == "skipped_not_available"


def test_run_universe_failure_continues_and_exits_1(
    invoke: Invoke, serve_day: Callable[..., None], write_universe: Callable[[str, str], Path]
) -> None:
    write_universe("pilot", PILOT)
    serve_day("BTCUSDT", DAY, bad_checksum=True)
    serve_day("ETHUSDT", DAY)
    result = invoke("run-universe", "pilot", f"period={DAY}")
    assert result.exit_code == 1
    rows = table(result)
    assert rows["BTCUSDT"][0] == "failed"
    assert rows["ETHUSDT"][0] == "success"


def test_run_universe_dataset_choice(
    invoke: Invoke, write_universe: Callable[[str, str], Path]
) -> None:
    write_universe(
        "multi",
        PILOT.replace("pilot", "multi").replace(
            "[binance_spot_klines_1m]", "[binance_spot_klines_1m, other_dataset]"
        ),
    )
    assert invoke("run-universe", "multi", f"period={DAY}").exit_code == 2
    assert invoke("run-universe", "multi", "--dataset", "nope", f"period={DAY}").exit_code == 2
    assert invoke("run-universe", "missing", f"period={DAY}").exit_code == 2
    assert (
        invoke(
            "run-universe", "multi", "--dataset", "binance_spot_klines_1m", "--on", "bad-date"
        ).exit_code
        == 2
    )


# --- danh sách, lịch sử, catalog, disk, query ---------------------------------------


def test_datasets(invoke: Invoke) -> None:
    result = invoke("datasets")
    assert result.exit_code == 0
    assert "binance_spot_klines_1m  binance  crypto_bars_1m  symbol,period" in result.stdout


def test_universes_reports_broken_files(
    invoke: Invoke, write_universe: Callable[[str, str], Path]
) -> None:
    write_universe("pilot", PILOT)
    assert invoke("universes").exit_code == 0
    assert "pilot" in invoke("universes").stdout
    write_universe("broken", "name: broken\nkey: symbol\n")
    result = invoke("universes")
    assert result.exit_code == 1
    assert "missing fields" in result.stdout


def test_runs_and_catalog_after_run(invoke: Invoke, serve_day: Callable[..., None]) -> None:
    serve_day("BTCUSDT", DAY)
    serve_day("ETHUSDT", DAY, status=404)
    invoke("run", "binance_spot_klines_1m", "symbol=BTCUSDT", f"period={DAY}")
    invoke("run", "binance_spot_klines_1m", "symbol=ETHUSDT", f"period={DAY}")

    runs = invoke("runs")
    assert runs.exit_code == 0
    assert "period=2025-01-01,symbol=BTCUSDT  success" in runs.stdout
    skipped = invoke("runs", "--status", "skipped_not_available")
    assert "ETHUSDT" in skipped.stdout and "BTCUSDT" not in skipped.stdout
    assert len(invoke("runs", "--limit", "1").stdout.splitlines()) == 3
    assert invoke("runs", "--status", "bogus").exit_code == 2

    catalog = invoke("catalog")
    assert "crypto_bars_1m  binance  silver" in catalog.stdout
    assert "crypto_bars_1m" not in invoke("catalog", "--layer", "gold").stdout


def test_disk(invoke: Invoke, monkeypatch: pytest.MonkeyPatch) -> None:
    result = invoke("disk")
    assert result.exit_code == 0
    assert "silver" in result.stdout and "OK" in result.stdout
    monkeypatch.setenv("ALPHADATA_MIN_FREE_GB", "1000000000")
    low = invoke("disk")
    assert low.exit_code == 1
    assert "LOW" in low.stdout


def test_query(invoke: Invoke, serve_day: Callable[..., None]) -> None:
    serve_day("BTCUSDT", DAY)
    invoke("run", "binance_spot_klines_1m", "symbol=BTCUSDT", f"period={DAY}")
    result = invoke("query", "SELECT symbol, count(*) AS n FROM crypto_bars_1m GROUP BY symbol")
    assert result.exit_code == 0, result.stderr
    assert result.stdout.splitlines()[2].split() == ["BTCUSDT", "4"]

    limited = invoke("query", "SELECT * FROM crypto_bars_1m", "--limit", "2")
    assert len(limited.stdout.splitlines()) == 4
    assert "showing first 2 rows" in limited.stderr

    bad = invoke("query", "SELECT * FROM no_such_table")
    assert bad.exit_code == 1
    assert "available views: ['crypto_bars_1m']" in bad.stderr
