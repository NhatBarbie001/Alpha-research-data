"""Backfill có checkpoint: lập kế hoạch partition cho mọi member, chạy, resume khi crash.

Checkpoint chỉ được đánh dấu `done` sau khi silver đã ghi xong. Bị kill trước đó thì partition còn
`pending` và chạy lại ở lần resume; merge theo khoá nên không sinh dòng trùng.
"""

import logging
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from alphadata.core.errors import DiskFullError, NotAvailableYet
from alphadata.core.settings import Settings, get_settings
from alphadata.ingestion.pipeline.dataset import Dataset, Partition
from alphadata.ingestion.pipeline.registry import get_dataset
from alphadata.ingestion.pipeline.run import run_partition, validate_partition
from alphadata.metadata import Checkpoints, MetaStore
from alphadata.universe import Universe, load

logger = logging.getLogger(__name__)

DONE, FAILED, NOT_AVAILABLE, DISK_FULL, CANCELLED = (
    "done",
    "failed",
    "not_available",
    "disk_full",
    "cancelled",
)


@dataclass(frozen=True)
class BackfillResult:
    backfill_id: uuid.UUID
    dataset: str
    attempted: int  # partition đã chạy trong lần này
    succeeded: int
    failed: int  # lỗi khác NotAvailableYet
    not_available: int
    stopped: str | None  # "disk_full" nếu dừng vì hết disk
    summary: dict[str, int]  # đếm theo status của toàn bộ backfill sau lần chạy

    @property
    def remaining(self) -> int:
        return self.summary.get("pending", 0) + self.summary.get("failed", 0)


def utc_today() -> date:
    return datetime.now(UTC).date()


def plan_partitions(
    ds: Dataset,
    universe: Universe,
    start: date | None = None,
    end: date | None = None,
    *,
    today: date | None = None,
) -> list[Partition]:
    """Partition cần chạy cho mọi member, theo khoảng [start, end] đã cắt theo member và hôm qua."""
    if ds.name not in universe.datasets:
        raise ValueError(
            f"{ds.name!r} is not in {universe.name}.datasets {list(universe.datasets)}"
        )
    start = start or universe.start
    if start is None:
        raise ValueError(f"no start date: pass start or set 'start' in {universe.name}.yaml")
    yesterday = (today or utc_today()) - timedelta(days=1)
    end = min(end, yesterday) if end else yesterday
    if start > end:
        raise ValueError(
            f"empty range: start {start} is after end {end} (end is capped at yesterday)"
        )

    partitions: list[Partition] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for member in universe.members:
        member_start = max(start, member.start) if member.start else start
        member_end = min(end, member.end) if member.end else end
        if member_start > member_end:
            continue
        for raw in ds.backfill_partitions(member.id, member_start, member_end):
            part = validate_partition(ds, raw)
            key = tuple(sorted(part.items()))
            if key not in seen:
                seen.add(key)
                partitions.append(part)
    return partitions


def plan_backfill(
    dataset: str | Dataset,
    universe: str | Universe,
    start: date | None = None,
    end: date | None = None,
    *,
    meta: MetaStore,
    settings: Settings | None = None,
    today: date | None = None,
) -> tuple[uuid.UUID, int]:
    """Lập kế hoạch và ghi checkpoint `pending`. Trả (backfill_id, số partition)."""
    ds = _resolve(dataset)
    uni = (
        universe
        if isinstance(universe, Universe)
        else load(universe, (settings or get_settings()).universe_dir)
    )
    partitions = plan_partitions(ds, uni, start, end, today=today)
    if not partitions:
        raise ValueError(f"nothing to backfill for {ds.name} / {uni.name} in this range")
    backfill_id = uuid.uuid4()
    added = Checkpoints(meta).add(backfill_id, ds.name, partitions)
    logger.info("planned backfill %s: %s, %d partitions", backfill_id, ds.name, added)
    return backfill_id, added


def run_backfill(
    dataset: str | Dataset,
    universe: str | Universe | None = None,
    start: date | None = None,
    end: date | None = None,
    *,
    backfill_id: uuid.UUID | None = None,
    max_workers: int = 1,
    settings: Settings | None = None,
    meta: MetaStore | None = None,
    on_planned: Callable[[uuid.UUID, int], None] | None = None,
    today: date | None = None,
) -> BackfillResult:
    """Backfill mới (cần `universe`) hoặc resume (`backfill_id`): chỉ chạy partition chưa `done`.

    `on_planned(backfill_id, số partition sẽ chạy)` được gọi trước khi chạy, để người gọi in ra
    backfill_id phòng khi tiến trình bị kill.
    """
    if max_workers < 1:
        raise ValueError("max_workers must be >= 1")
    ds = _resolve(dataset)
    settings = settings or get_settings()
    with ExitStack() as stack:
        store = meta or _owned(stack, MetaStore.from_settings(settings))
        checkpoints = Checkpoints(store)
        if backfill_id is None:
            if universe is None:
                raise ValueError("universe is required for a new backfill")
            backfill_id, _ = plan_backfill(
                ds, universe, start, end, meta=store, settings=settings, today=today
            )
        else:
            owner = checkpoints.dataset_of(backfill_id)
            if owner is None:
                raise ValueError(f"unknown backfill_id {backfill_id}")
            if owner != ds.name:
                raise ValueError(f"backfill {backfill_id} belongs to {owner!r}, not {ds.name!r}")

        todo = checkpoints.remaining(backfill_id)
        if on_planned is not None:
            on_planned(backfill_id, len(todo))
        outcomes = _execute(ds, todo, backfill_id, max_workers, settings, store, checkpoints)
        return BackfillResult(
            backfill_id=backfill_id,
            dataset=ds.name,
            attempted=sum(o in (DONE, FAILED, NOT_AVAILABLE) for o in outcomes),
            succeeded=outcomes.count(DONE),
            failed=outcomes.count(FAILED),
            not_available=outcomes.count(NOT_AVAILABLE),
            stopped=DISK_FULL if DISK_FULL in outcomes else None,
            summary=checkpoints.summary(backfill_id),
        )
    raise AssertionError("unreachable")


def _execute(
    ds: Dataset,
    todo: list[Partition],
    backfill_id: uuid.UUID,
    max_workers: int,
    settings: Settings,
    meta: MetaStore,
    checkpoints: Checkpoints,
) -> list[str]:
    stop = threading.Event()
    lock = threading.Lock()
    progress = [0]

    def one(part: Partition) -> str:
        if stop.is_set():
            return CANCELLED
        outcome = _run_one(ds, part, backfill_id, settings, meta, checkpoints)
        if outcome == DISK_FULL:
            stop.set()
        with lock:
            progress[0] += 1
            logger.info("[%d/%d] %s %s: %s", progress[0], len(todo), ds.name, part, outcome)
        return outcome

    if max_workers == 1:
        # Tuần tự trong luồng chính: KeyboardInterrupt (Ctrl-C, bị kill) đi thẳng ra ngoài.
        outcomes = []
        for part in todo:
            outcomes.append(one(part))
            if stop.is_set():
                break
        return outcomes
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(one, todo))


def _run_one(
    ds: Dataset,
    part: Partition,
    backfill_id: uuid.UUID,
    settings: Settings,
    meta: MetaStore,
    checkpoints: Checkpoints,
) -> str:
    try:
        # run_partition gọi check_disk trước khi ghi run nào.
        run_partition(
            ds, part, trigger="backfill", settings=settings, meta=meta, backfill_id=backfill_id
        )
    except DiskFullError:
        logger.error(
            "disk below threshold: stopping backfill %s (partition stays pending)", backfill_id
        )
        return DISK_FULL
    except NotAvailableYet as exc:
        checkpoints.mark(backfill_id, part, "failed", error=f"NotAvailableYet: {exc}")
        return NOT_AVAILABLE
    except Exception as exc:
        checkpoints.mark(backfill_id, part, "failed", error=f"{type(exc).__name__}: {exc}")
        logger.warning("%s %s failed: %s: %s", ds.name, part, type(exc).__name__, exc)
        return FAILED
    checkpoints.mark(backfill_id, part, "done")
    return DONE


def _resolve(dataset: str | Dataset) -> Dataset:
    return dataset if isinstance(dataset, Dataset) else get_dataset(dataset)


def _owned(stack: ExitStack, store: MetaStore) -> MetaStore:
    stack.callback(store.close)
    return store
