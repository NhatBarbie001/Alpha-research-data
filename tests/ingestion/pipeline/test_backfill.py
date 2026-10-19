import uuid
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from alphadata.core.errors import DiskFullError
from alphadata.core.settings import Settings
from alphadata.ingestion.pipeline import plan_partitions, run_backfill
from alphadata.metadata import Checkpoints, MetaStore
from alphadata.storage.disk import check_disk as real_check
from alphadata.universe import Member, Universe

TODAY = date(2026, 10, 19)
DAYS = ["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04", "2026-10-05"]


def universe(*members: Member, start: date | None = date(2026, 10, 1)) -> Universe:
    return Universe(
        name="pilot",
        datasets=("fake_klines",),
        key="symbol",
        members=members or (Member("BTCUSDT"),),
        start=start,
    )


def silver_rows(settings: Settings) -> int:
    path = (
        settings.data_root / "silver/crypto_bars_1m/symbol=BTCUSDT/year=2026/month=10/part.parquet"
    )
    return pq.read_table(path).num_rows


@pytest.fixture
def five_days(write_day: Callable[..., Path]) -> None:
    for day in DAYS:
        write_day("BTCUSDT", day)


def backfill(dataset: Any, settings: Settings, meta: MetaStore, **kwargs: Any) -> Any:
    kwargs.setdefault("today", TODAY)
    return run_backfill(dataset, settings=settings, meta=meta, **kwargs)


# --- lập kế hoạch ------------------------------------------------------------------


def test_plan_clips_to_members_and_yesterday(dataset: Any) -> None:
    uni = universe(
        Member("BTCUSDT"),
        Member("NEWUSDT", start=date(2026, 10, 17)),
        Member("OLDUSDT", end=date(2026, 9, 30)),
    )
    parts = plan_partitions(dataset, uni, date(2026, 10, 16), date(2026, 12, 31), today=TODAY)
    assert parts == [
        {"symbol": "BTCUSDT", "date": "2026-10-16"},
        {"symbol": "BTCUSDT", "date": "2026-10-17"},
        {"symbol": "BTCUSDT", "date": "2026-10-18"},  # end bị cắt về hôm qua
        {"symbol": "NEWUSDT", "date": "2026-10-17"},
        {"symbol": "NEWUSDT", "date": "2026-10-18"},
    ]


def test_plan_uses_universe_start_by_default(dataset: Any) -> None:
    parts = plan_partitions(dataset, universe(start=date(2026, 10, 17)), today=TODAY)
    assert [p["date"] for p in parts] == ["2026-10-17", "2026-10-18"]


@pytest.mark.parametrize(
    ("uni", "start", "end", "message"),
    [
        (
            Universe("u", ("other",), "symbol", (Member("BTCUSDT"),)),
            None,
            None,
            "not in u.datasets",
        ),
        (universe(start=None), None, None, "no start date"),
        (universe(), date(2026, 10, 18), date(2026, 10, 2), "empty range"),
        (universe(), date(2026, 10, 19), None, "empty range"),
    ],
    ids=["dataset-not-in-universe", "no-start", "start-after-end", "start-today"],
)
def test_plan_errors(dataset: Any, uni: Universe, start: Any, end: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        plan_partitions(dataset, uni, start, end, today=TODAY)


def test_dataset_without_backfill_support(dataset: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(type(dataset), "backfill_partitions")
    with pytest.raises(NotImplementedError, match="does not support backfill"):
        plan_partitions(dataset, universe(), today=TODAY)


# --- chạy, lỗi, resume -------------------------------------------------------------


def test_full_backfill(dataset: Any, settings: Settings, meta: MetaStore, five_days: None) -> None:
    announced: list[tuple[uuid.UUID, int]] = []
    result = backfill(
        dataset,
        settings,
        meta,
        universe=universe(),
        end=date(2026, 10, 5),
        on_planned=lambda bid, n: announced.append((bid, n)),
    )
    assert announced == [(result.backfill_id, 5)]
    assert (result.attempted, result.succeeded, result.remaining) == (5, 5, 0)
    assert result.summary == {"done": 5}
    assert silver_rows(settings) == 5 * 1440
    runs = meta.recent_runs(limit=10)
    assert {r.trigger for r in runs} == {"backfill"}
    assert {r.backfill_id for r in runs} == {result.backfill_id}


def test_failed_partition_retried_on_resume(
    dataset: Any, settings: Settings, meta: MetaStore, write_day: Callable[..., Path]
) -> None:
    for day in DAYS[:2]:
        write_day("BTCUSDT", day)
    first = backfill(dataset, settings, meta, universe=universe(), end=date(2026, 10, 3))
    assert (first.succeeded, first.not_available, first.remaining) == (2, 1, 1)
    assert first.summary == {"done": 2, "failed": 1}

    write_day("BTCUSDT", DAYS[2])
    calls = dataset.fetch_calls
    second = backfill(dataset, settings, meta, backfill_id=first.backfill_id)
    assert dataset.fetch_calls == calls + 1  # chỉ chạy lại partition còn thiếu
    assert (second.succeeded, second.remaining) == (1, 0)
    assert silver_rows(settings) == 3 * 1440


def test_killed_midway_resume_runs_only_the_rest(
    dataset: Any, settings: Settings, meta: MetaStore, five_days: None
) -> None:
    announced: list[uuid.UUID] = []
    dataset.interrupt_at = 3
    with pytest.raises(KeyboardInterrupt):
        backfill(
            dataset,
            settings,
            meta,
            universe=universe(),
            end=date(2026, 10, 5),
            on_planned=lambda bid, n: announced.append(bid),
        )
    [bid] = announced
    assert Checkpoints(meta).summary(bid) == {"done": 2, "pending": 3}  # không bị đánh dấu failed

    dataset.interrupt_at = None
    calls = dataset.fetch_calls
    result = backfill(dataset, settings, meta, backfill_id=bid)
    assert dataset.fetch_calls == calls + 3
    assert result.summary == {"done": 5}
    assert silver_rows(settings) == 5 * 1440


def test_killed_after_write_before_checkpoint_has_no_duplicates(
    dataset: Any,
    settings: Settings,
    meta: MetaStore,
    five_days: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_mark = Checkpoints.mark
    calls = {"n": 0}

    def mark_then_die(self: Checkpoints, *args: Any, **kwargs: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise KeyboardInterrupt  # silver ngày thứ 2 đã ghi, checkpoint chưa kịp done
        real_mark(self, *args, **kwargs)

    announced: list[uuid.UUID] = []
    monkeypatch.setattr(Checkpoints, "mark", mark_then_die)
    with pytest.raises(KeyboardInterrupt):
        backfill(
            dataset,
            settings,
            meta,
            universe=universe(),
            end=date(2026, 10, 5),
            on_planned=lambda bid, n: announced.append(bid),
        )
    monkeypatch.setattr(Checkpoints, "mark", real_mark)
    assert silver_rows(settings) == 2 * 1440

    result = backfill(dataset, settings, meta, backfill_id=announced[0])
    assert result.summary == {"done": 5}
    assert silver_rows(settings) == 5 * 1440  # ngày thứ 2 chạy lại nhưng không trùng dòng


def test_disk_full_stops_and_keeps_pending(
    dataset: Any,
    settings: Settings,
    meta: MetaStore,
    five_days: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"n": 0}

    def check(s: Settings) -> float:
        calls["n"] += 1
        if calls["n"] >= 3:
            raise DiskFullError("0.1 GB free")
        return real_check(s)

    monkeypatch.setattr("alphadata.ingestion.pipeline.run.check_disk", check)
    result = backfill(dataset, settings, meta, universe=universe(), end=date(2026, 10, 5))
    assert result.stopped == "disk_full"
    assert (result.attempted, result.succeeded) == (2, 2)
    assert result.summary == {"done": 2, "pending": 3}  # không đánh dấu failed


def test_parallel_workers(
    dataset: Any, settings: Settings, meta: MetaStore, five_days: None
) -> None:
    result = backfill(
        dataset, settings, meta, universe=universe(), end=date(2026, 10, 5), max_workers=3
    )
    assert result.summary == {"done": 5}
    assert silver_rows(settings) == 5 * 1440


def test_resume_errors(dataset: Any, settings: Settings, meta: MetaStore, five_days: None) -> None:
    with pytest.raises(ValueError, match="unknown backfill_id"):
        backfill(dataset, settings, meta, backfill_id=uuid.uuid4())
    other = uuid.uuid4()
    Checkpoints(meta).add(other, "another_dataset", [{"symbol": "X", "date": "2026-10-01"}])
    with pytest.raises(ValueError, match="belongs to 'another_dataset'"):
        backfill(dataset, settings, meta, backfill_id=other)
    with pytest.raises(ValueError, match="universe is required"):
        backfill(dataset, settings, meta)
    with pytest.raises(ValueError, match="max_workers"):
        backfill(dataset, settings, meta, universe=universe(), max_workers=0)
