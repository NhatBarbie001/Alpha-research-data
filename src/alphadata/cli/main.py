"""Lệnh `alphadata`. Kết quả ra stdout, log ra stderr. Exit code: 0 ok, 1 lỗi, 2 sai tham số,
3 NotAvailableYet (script bên ngoài biết mà thử lại)."""

import functools
import io
import logging
import sys
import traceback
import uuid
from collections.abc import Callable, Iterable, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any, NoReturn

import click
import duckdb

from alphadata import __version__
from alphadata.access.views import connect, view_names
from alphadata.cli.output import format_table
from alphadata.core.errors import NotAvailableYet
from alphadata.core.http import make_session, mask_secrets
from alphadata.core.settings import get_settings
from alphadata.ingestion.pipeline import (
    Dataset,
    Partition,
    RunResult,
    get_dataset,
    list_datasets,
    reprocess_partition,
    run_backfill,
    run_partition,
    validate_partition,
)
from alphadata.metadata import MetaStore
from alphadata.metadata.tables import LAYERS, RUN_STATUSES
from alphadata.storage.disk import free_gb
from alphadata.universe import InvalidUniverse, list_universes, load

EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_NOT_AVAILABLE = 3
logger = logging.getLogger("alphadata.cli")


def exit_codes[F: Callable[..., Any]](fn: F) -> F:
    """Đổi exception thành exit code theo quy ước. Lỗi tham số dùng click.UsageError (exit 2)."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except click.exceptions.ClickException:
            raise
        except click.exceptions.Exit:
            raise
        except NotAvailableYet as exc:
            _fail(exc, EXIT_NOT_AVAILABLE)
        except Exception as exc:
            _fail(exc, EXIT_ERROR)

    return wrapper  # type: ignore[return-value]


def _fail(exc: BaseException, code: int) -> NoReturn:
    ctx = click.get_current_context()
    click.echo(f"error: {type(exc).__name__}: {mask_secrets(_message(exc))}", err=True)
    if ctx.find_root().obj and ctx.find_root().obj.get("verbose"):
        click.echo(mask_secrets(traceback.format_exc()), err=True)
    ctx.exit(code)


def _message(exc: BaseException) -> str:
    # str(KeyError("x")) có thêm ngoặc kép; lấy nguyên message.
    return str(exc.args[0]) if isinstance(exc, KeyError) and exc.args else str(exc)


def parse_params(tokens: Iterable[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for token in tokens:
        key, sep, value = token.partition("=")
        if not sep or not key:
            raise click.UsageError(f"expected key=value, got {token!r}")
        if key in params:
            raise click.UsageError(f"duplicate key {key!r}")
        params[key] = value
    return params


def _dataset(name: str) -> Dataset:
    try:
        return get_dataset(name)
    except KeyError as exc:
        raise click.UsageError(_message(exc)) from None


def _partition(ds: Dataset, params: dict[str, str]) -> Partition:
    try:
        return validate_partition(ds, params)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from None


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="alphadata")
@click.option("-v", "--verbose", is_flag=True, help="Log chi tiết và in traceback khi lỗi.")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """Tầng data cho Alpha research agent."""
    ctx.obj = {"verbose": verbose}
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )


@cli.command()
@click.argument("dataset")
@click.argument("params", nargs=-1)
@exit_codes
def run(dataset: str, params: tuple[str, ...]) -> None:
    """Chạy một partition: tải, kiểm tra, ghi silver. Ví dụ: run <dataset> symbol=X period=Y."""
    ds = _dataset(dataset)
    part = _partition(ds, parse_params(params))
    _print_result(run_partition(ds, part, trigger="cli", settings=get_settings()))


@cli.command()
@click.argument("dataset")
@click.argument("params", nargs=-1)
@exit_codes
def reprocess(dataset: str, params: tuple[str, ...]) -> None:
    """Chạy lại một partition từ raw đã lưu, không gọi mạng."""
    ds = _dataset(dataset)
    part = _partition(ds, parse_params(params))
    _print_result(reprocess_partition(ds, part, trigger="cli", settings=get_settings()))


@cli.command("run-universe")
@click.argument("universe_name")
@click.argument("params", nargs=-1)
@click.option("--dataset", "dataset_name", help="Bắt buộc khi universe dùng cho nhiều dataset.")
@click.option(
    "--on",
    "on_text",
    help="Chỉ chạy member hoạt động ngày này (YYYY-MM-DD); mặc định lấy từ period/date.",
)
@exit_codes
def run_universe(
    universe_name: str, params: tuple[str, ...], dataset_name: str | None, on_text: str | None
) -> None:
    """Chạy run cho từng member của một universe; member lỗi không dừng member khác."""
    settings = get_settings()
    try:
        uni = load(universe_name, settings.universe_dir)
    except (KeyError, ValueError) as exc:
        raise click.UsageError(_message(exc)) from None
    if dataset_name is None:
        if len(uni.datasets) != 1:
            raise click.UsageError(f"{uni.name} has datasets {list(uni.datasets)}; pass --dataset")
        dataset_name = uni.datasets[0]
    elif dataset_name not in uni.datasets:
        raise click.UsageError(
            f"{dataset_name!r} is not in {uni.name}.datasets {list(uni.datasets)}"
        )
    ds = _dataset(dataset_name)
    extra = parse_params(params)
    on = _parse_day(on_text, "--on") if on_text else _day_from(extra)
    try:
        partitions = [validate_partition(ds, p) for p in uni.partitions(extra, on=on)]
    except ValueError as exc:
        raise click.UsageError(str(exc)) from None

    rows: list[list[object]] = []
    statuses: list[str] = []
    meta = MetaStore.from_settings(settings)
    try:
        with make_session(settings) as session:
            for part in partitions:
                member = part[uni.key]
                try:
                    r = run_partition(
                        ds, part, trigger="cli", settings=settings, meta=meta, session=session
                    )
                    statuses.append("success")
                    rows.append([member, "success", r.rows_in, r.rows_out, ""])
                except NotAvailableYet as exc:
                    statuses.append("skipped_not_available")
                    rows.append([member, "skipped_not_available", None, None, str(exc)])
                except Exception as exc:
                    logger.exception("%s %s failed", ds.name, part)
                    statuses.append("failed")
                    rows.append([member, "failed", None, None, f"{type(exc).__name__}: {exc}"])
    finally:
        meta.close()

    masked = [[*row[:4], mask_secrets(str(row[4]))] for row in rows]
    click.echo(format_table([uni.key, "status", "rows_in", "rows_out", "error"], masked))
    failures = [s for s in statuses if s != "success"]
    if not failures:
        return
    code = EXIT_NOT_AVAILABLE if set(failures) == {"skipped_not_available"} else EXIT_ERROR
    click.get_current_context().exit(code)


@cli.command()
@click.argument("dataset")
@click.argument("universe_name", required=False)
@click.option("--start", help="YYYY-MM hoặc YYYY-MM-DD; mặc định là start của universe.")
@click.option("--end", help="YYYY-MM (hết tháng) hoặc YYYY-MM-DD; mặc định và tối đa là hôm qua.")
@click.option("--resume", "resume_id", help="backfill_id cũ: chỉ chạy partition chưa xong.")
@click.option("--workers", type=click.IntRange(1, 16), default=1, show_default=True)
@exit_codes
def backfill(
    dataset: str,
    universe_name: str | None,
    start: str | None,
    end: str | None,
    resume_id: str | None,
    workers: int,
) -> None:
    """Tải lịch sử cho mọi member của universe, có checkpoint để resume khi bị ngắt."""
    ds = _dataset(dataset)
    settings = get_settings()
    backfill_id: uuid.UUID | None = None
    if resume_id is not None:
        if universe_name or start or end:
            raise click.UsageError(
                "--resume continues an existing backfill; omit universe/--start/--end"
            )
        try:
            backfill_id = uuid.UUID(resume_id)
        except ValueError:
            raise click.UsageError(
                f"--resume must be a backfill_id (UUID), got {resume_id!r}"
            ) from None
    elif universe_name is None:
        raise click.UsageError("UNIVERSE is required for a new backfill (or pass --resume ID)")

    def announce(bid: uuid.UUID, count: int) -> None:
        # In trước khi chạy: bị kill giữa chừng vẫn còn id để --resume.
        click.echo(f"backfill_id={bid} partitions_to_run={count}", err=True)

    try:
        result = run_backfill(
            ds,
            universe_name,
            _parse_bound(start, "--start", is_end=False),
            _parse_bound(end, "--end", is_end=True),
            backfill_id=backfill_id,
            max_workers=workers,
            settings=settings,
            on_planned=announce,
        )
    except (KeyError, ValueError, NotImplementedError) as exc:
        raise click.UsageError(_message(exc)) from None

    s = result.summary
    rows = [
        ["backfill_id", str(result.backfill_id)],
        ["dataset", result.dataset],
        [
            "this run",
            f"{result.attempted} attempted, {result.succeeded} done, "
            f"{result.failed} failed, {result.not_available} not available yet",
        ],
        [
            "overall",
            f"done={s.get('done', 0)} failed={s.get('failed', 0)} pending={s.get('pending', 0)}",
        ],
        ["stopped", result.stopped or "no"],
    ]
    click.echo(format_table(["item", "value"], rows, max_width=120))
    if result.remaining == 0:
        return
    click.echo(
        f"resume with: alphadata backfill {result.dataset} --resume {result.backfill_id}", err=True
    )
    only_not_available = not result.stopped and result.failed == 0 and result.not_available > 0
    click.get_current_context().exit(EXIT_NOT_AVAILABLE if only_not_available else EXIT_ERROR)


@cli.command()
@exit_codes
def datasets() -> None:
    """Liệt kê dataset đã đăng ký: nguồn, bảng, khoá partition, secret cần có."""
    rows = [
        [d.name, d.source, d.table, list(d.partition_keys), list(d.requires), d.keep_raw]
        for d in list_datasets()
    ]
    columns = ["dataset", "source", "table", "partition_keys", "requires", "keep_raw"]
    click.echo(format_table(columns, rows))


@cli.command()
@exit_codes
def universes() -> None:
    """Liệt kê universe, dataset dùng nó và số member."""
    directory = get_settings().universe_dir
    rows: list[list[object]] = []
    broken = False
    for name in list_universes(directory):
        try:
            u = load(name, directory)
            rows.append([u.name, list(u.datasets), u.key, len(u.members), u.start, ""])
        except InvalidUniverse as exc:
            broken = True
            rows.append([name, None, None, None, None, str(exc)])
    click.echo(format_table(["universe", "datasets", "key", "members", "start", "error"], rows))
    if broken:
        click.get_current_context().exit(EXIT_ERROR)


@cli.command()
@click.option("--layer", type=click.Choice(sorted(LAYERS)), help="Chỉ một tầng.")
@exit_codes
def catalog(layer: str | None) -> None:
    """Bảng nào, từ nguồn nào, khoá gì, nằm ở đâu."""
    meta = MetaStore.from_settings(get_settings())
    try:
        entries = meta.catalog(layer)
    finally:
        meta.close()
    rows = [
        [e.table_name, e.source, e.layer, e.dataset, e.key_cols, e.schema_version, e.updated_at]
        for e in entries
    ]
    columns = ["table", "source", "layer", "dataset", "key", "version", "updated_at"]
    click.echo(format_table(columns, rows))


@cli.command()
@click.option("--dataset", help="Lọc theo dataset.")
@click.option("--status", type=click.Choice(sorted(RUN_STATUSES)), help="Lọc theo trạng thái.")
@click.option("--limit", type=click.IntRange(1, 10_000), default=20, show_default=True)
@exit_codes
def runs(dataset: str | None, status: str | None, limit: int) -> None:
    """Lịch sử chạy gần đây, mới nhất trước."""
    meta = MetaStore.from_settings(get_settings())
    try:
        records = meta.recent_runs(limit=limit, dataset=dataset, status=status)
    finally:
        meta.close()
    rows = [
        [
            r.started_at,
            r.dataset,
            ",".join(f"{k}={v}" for k, v in r.partition.items()),
            r.status,
            r.rows_in,
            r.rows_out,
            r.error,
        ]
        for r in records
    ]
    columns = ["started_at", "dataset", "partition", "status", "rows_in", "rows_out", "error"]
    click.echo(format_table(columns, rows, max_width=60))


@cli.command()
@exit_codes
def disk() -> None:
    """Dung lượng trống và dung lượng từng tầng của lake. Exit 1 nếu dưới ngưỡng."""
    settings = get_settings()
    free = free_gb(settings.data_root)
    layers = [
        ("raw", settings.raw_dir),
        ("silver", settings.silver_dir),
        ("gold", settings.gold_dir),
    ]
    rows: list[list[object]] = [[name, f"{_dir_size(path) / 1e6:.1f} MB"] for name, path in layers]
    status = "OK" if free >= settings.min_free_gb else "LOW"
    rows.append(["free", f"{free:.2f} GB (min {settings.min_free_gb} GB, {status})"])
    click.echo(f"lake: {settings.data_root}")
    click.echo(format_table(["item", "size"], rows))
    if status == "LOW":
        click.get_current_context().exit(EXIT_ERROR)


@cli.command()
@click.argument("sql")
@click.option("--limit", type=click.IntRange(1, 100_000), default=100, show_default=True)
@exit_codes
def query(sql: str, limit: int) -> None:
    """Chạy SQL trên DuckDB với view cho mọi bảng đã có file (ví dụ crypto_bars_1m)."""
    con = connect(get_settings())
    try:
        try:
            result = con.execute(sql)
        except duckdb.Error as exc:
            views = view_names(con)
            raise RuntimeError(f"{exc}\navailable views: {views}") from None
        if result.description is None:
            click.echo("ok")
            return
        columns = [d[0] for d in result.description]
        rows = result.fetchmany(limit + 1)
    finally:
        con.close()
    click.echo(format_table(columns, rows[:limit]))
    if len(rows) > limit:
        click.echo(f"(showing first {limit} rows; use --limit)", err=True)


def _print_result(r: RunResult) -> None:
    warns = [f"{c.name}: {c.detail}" for c in r.checks if not c.passed]
    lines = [
        ("run_id", str(r.run_id)),
        ("dataset", r.dataset),
        ("partition", ",".join(f"{k}={v}" for k, v in r.partition.items())),
        ("status", r.status),
        ("rows_in", str(r.rows_in)),
        ("rows_out", str(r.rows_out)),
        ("silver", ", ".join(p.as_posix() for p in r.silver_paths)),
        ("warnings", "; ".join(warns) if warns else "none"),
    ]
    click.echo("\n".join(f"{k:<10} {v}" for k, v in lines))


def _parse_bound(text: str | None, what: str, *, is_end: bool) -> date | None:
    """YYYY-MM-DD, hoặc YYYY-MM (ngày đầu tháng cho --start, ngày cuối tháng cho --end)."""
    if text is None:
        return None
    if len(text) == 7:
        first = _parse_day(f"{text}-01", what)
        if not is_end:
            return first
        next_month = date(first.year + first.month // 12, first.month % 12 + 1, 1)
        return next_month - timedelta(days=1)
    return _parse_day(text, what)


def _parse_day(text: str, what: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise click.UsageError(f"{what} must be YYYY-MM-DD, got {text!r}") from None


def _day_from(params: dict[str, str]) -> date | None:
    for key in ("period", "date"):
        value = params.get(key, "")
        if len(value) == 10:
            try:
                return date.fromisoformat(value)
            except ValueError:
                return None
    return None


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main(argv: Sequence[str] | None = None) -> None:
    # Console Windows mặc định cp1258/cp1252 không in được tiếng Việt trong help và message.
    for stream in (sys.stdout, sys.stderr):
        if isinstance(stream, io.TextIOWrapper) and stream.encoding.lower() != "utf-8":
            stream.reconfigure(encoding="utf-8", errors="replace")
    cli.main(args=list(argv) if argv is not None else None, prog_name="alphadata")
