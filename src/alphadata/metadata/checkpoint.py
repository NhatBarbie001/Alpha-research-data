import hashlib
import uuid
from collections.abc import Iterable, Mapping
from typing import Any

import sqlalchemy as sa

from alphadata.core.http import mask_secrets
from alphadata.metadata import tables as t
from alphadata.metadata.store import (
    ERROR_MAX_CHARS,
    MetaStore,
    canonical_json,
    dialect_insert,
    normalize_partition,
    utcnow,
)

_bc = t.backfill_checkpoints


def partition_hash(partition: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_json(dict(partition)).encode("utf-8")).hexdigest()


class Checkpoints:
    """Trạng thái từng partition của một lần backfill, để chạy lại chỉ làm phần chưa xong."""

    def __init__(self, store: MetaStore) -> None:
        self._engine = store.engine

    def add(
        self, backfill_id: uuid.UUID, dataset: str, partitions: Iterable[Mapping[str, object]]
    ) -> int:
        """Ghi partition ở trạng thái pending; partition đã có (kể cả đã done) giữ nguyên."""
        now = utcnow()
        rows = {
            partition_hash(p): {
                "backfill_id": backfill_id,
                "partition_hash": partition_hash(p),
                "dataset": dataset,
                "partition": normalize_partition(p),
                "status": "pending",
                "attempts": 0,
                "updated_at": now,
            }
            for p in partitions
        }
        if not rows:
            return 0
        # RETURNING thay cho rowcount: psycopg trả rowcount = -1 khi insert nhiều dòng.
        stmt = (
            dialect_insert(self._engine, _bc)
            .on_conflict_do_nothing(index_elements=["backfill_id", "partition_hash"])
            .returning(_bc.c.partition_hash)
        )
        with self._engine.begin() as conn:
            return len(conn.execute(stmt, list(rows.values())).all())

    def remaining(self, backfill_id: uuid.UUID) -> list[dict[str, Any]]:
        """Partition chưa done (pending hoặc failed), thứ tự ổn định."""
        q = (
            sa.select(_bc.c.partition)
            .where(_bc.c.backfill_id == backfill_id, _bc.c.status != "done")
            .order_by(_bc.c.partition_hash)
        )
        with self._engine.connect() as conn:
            return list(conn.scalars(q))

    def mark(
        self,
        backfill_id: uuid.UUID,
        partition: Mapping[str, object],
        status: str,
        error: str | None = None,
    ) -> None:
        if status not in t.CHECKPOINT_STATUSES - {"pending"}:
            raise ValueError(f"invalid checkpoint status {status!r}; use 'done' or 'failed'")
        if error is not None:
            error = mask_secrets(error)[:ERROR_MAX_CHARS]
        with self._engine.begin() as conn:
            result = conn.execute(
                _bc.update()
                .where(
                    _bc.c.backfill_id == backfill_id,
                    _bc.c.partition_hash == partition_hash(partition),
                )
                .values(
                    status=status,
                    attempts=_bc.c.attempts + 1,
                    last_error=error,
                    updated_at=utcnow(),
                )
            )
            if result.rowcount != 1:
                raise KeyError(f"partition {dict(partition)} not in backfill {backfill_id}")

    def summary(self, backfill_id: uuid.UUID) -> dict[str, int]:
        q = (
            sa.select(_bc.c.status, sa.func.count())
            .where(_bc.c.backfill_id == backfill_id)
            .group_by(_bc.c.status)
        )
        with self._engine.connect() as conn:
            return {status: int(n) for status, n in conn.execute(q)}
