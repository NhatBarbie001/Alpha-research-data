import uuid
from datetime import date

import pytest

from alphadata.metadata import Checkpoints, MetaStore, partition_hash


def months(*ms: int) -> list[dict[str, object]]:
    return [{"symbol": "BTCUSDT", "month": f"2024-{m:02d}"} for m in ms]


def test_resume_only_unfinished(store: MetaStore) -> None:
    cp = Checkpoints(store)
    bid = uuid.uuid4()
    assert cp.add(bid, "binance_spot_klines_1m", months(1, 2, 3)) == 3
    cp.mark(bid, months(1)[0], "done")
    cp.mark(bid, months(2)[0], "failed", error="HTTP 500 https://x?api_key=SECRET")
    remaining = cp.remaining(bid)
    assert sorted(p["month"] for p in remaining) == ["2024-02", "2024-03"]
    assert cp.summary(bid) == {"done": 1, "failed": 1, "pending": 1}

    # Lập lại kế hoạch cùng backfill_id: partition đã có giữ nguyên trạng thái.
    assert cp.add(bid, "binance_spot_klines_1m", months(1, 2, 3, 4)) == 1
    assert cp.summary(bid) == {"done": 1, "failed": 1, "pending": 2}


def test_failed_error_is_masked_and_attempts_counted(store: MetaStore) -> None:
    cp = Checkpoints(store)
    bid = uuid.uuid4()
    cp.add(bid, "d", months(1))
    cp.mark(bid, months(1)[0], "failed", error="https://x?api_key=SECRET")
    cp.mark(bid, months(1)[0], "done")
    with store.engine.connect() as conn:
        row = conn.exec_driver_sql("SELECT attempts, last_error FROM backfill_checkpoints").one()
    assert row.attempts == 2
    assert row.last_error is None


def test_backfills_are_isolated(store: MetaStore) -> None:
    cp = Checkpoints(store)
    a, b = uuid.uuid4(), uuid.uuid4()
    cp.add(a, "d", months(1))
    cp.add(b, "d", months(1))
    cp.mark(a, months(1)[0], "done")
    assert cp.remaining(a) == []
    assert len(cp.remaining(b)) == 1


def test_invalid_mark(store: MetaStore) -> None:
    cp = Checkpoints(store)
    bid = uuid.uuid4()
    cp.add(bid, "d", months(1))
    with pytest.raises(ValueError):
        cp.mark(bid, months(1)[0], "pending")
    with pytest.raises(KeyError):
        cp.mark(bid, months(9)[0], "done")
    assert cp.add(bid, "d", []) == 0


def test_partition_hash_is_canonical() -> None:
    assert partition_hash({"a": 1, "d": date(2026, 1, 1)}) == partition_hash(
        {"d": "2026-01-01", "a": 1}
    )
    assert len(partition_hash({})) == 64
