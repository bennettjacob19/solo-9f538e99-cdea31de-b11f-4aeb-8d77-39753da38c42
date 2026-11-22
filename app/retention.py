"""Pure retention-boundary calculation.

Given one immutable snapshot of protection points (current WAL segment,
physical standbys, the backup pin and replication slots), compute the highest
*complete* WAL segment that can be safely recycled.

Protection rules implemented here:

1. The segment currently being written is never reclaimable, so the cutoff can
   never pass ``current_segment_start_lsn``.
2. Every physical standby must have replayed past a segment before it may be
   removed: the slowest (lowest replayed LSN) standby is the binding one.
3. The backup task pins WAL from its fixed ``backup_start_lsn``.
4. Active replication slots retain WAL back to their ``restart_lsn``.
5. An invalid/inactive slot can never be ignored: it is reported as a blocker
   and makes the whole snapshot non-reclaimable.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from .lsn import (
    format_lsn,
    highest_reclaimable_segment_start,
    next_segment_start,
)
from .schemas import ReclaimRequest


@dataclass
class RetentionPlan:
    reclaimable: bool
    highest_reclaimable_segment_start_lsn: int | None
    cutoff_lsn: int
    protection_points: dict[str, str]
    blockers: list[dict[str, str]] = field(default_factory=list)


def canonical_snapshot(req: ReclaimRequest) -> dict:
    """Deterministic, order-independent representation of a snapshot."""
    return {
        "segment_size": req.segment_size,
        "current_segment_start_lsn": req.current_segment_start_lsn,
        "current_segment_end_lsn": req.current_segment_end_lsn,
        "backup_start_lsn": req.backup_start_lsn,
        "standbys": sorted(
            (
                {"name": s.name, "replayed_lsn": s.replayed_lsn}
                for s in req.standbys
            ),
            key=lambda s: s["name"],
        ),
        "slots": sorted(
            (
                {
                    "name": s.name,
                    "active": s.active,
                    "restart_lsn": s.restart_lsn,
                }
                for s in req.slots
            ),
            key=lambda s: s["name"],
        ),
    }


def snapshot_hash(req: ReclaimRequest) -> str:
    """Stable hash of a snapshot; identical inputs always map to one plan."""
    blob = json.dumps(
        canonical_snapshot(req), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _validate_current_segment(req: ReclaimRequest) -> None:
    start = req.current_segment_start_lsn
    end = req.current_segment_end_lsn
    size = req.segment_size
    if end <= start:
        raise ValueError(
            "current_segment_end_lsn must be greater than current_segment_start_lsn"
        )
    if start % size != 0:
        raise ValueError("current_segment_start_lsn must be aligned to segment_size")
    if end != start + size:
        raise ValueError(
            "current_segment_end_lsn must equal current_segment_start_lsn + segment_size"
        )


def compute_plan(req: ReclaimRequest) -> RetentionPlan:
    """Compute the reclaim plan for a single snapshot."""
    _validate_current_segment(req)

    blockers: list[dict[str, str]] = []

    # Rule 5: invalidated slots block everything and must never be ignored.
    for slot in req.slots:
        if not slot.active:
            blockers.append(
                {
                    "type": "invalid_slot",
                    "name": slot.name,
                    "detail": (
                        "replication slot is not active; its restart_lsn cannot "
                        "be trusted to advance, so WAL reclaim is blocked"
                    ),
                }
            )
    if blockers:
        return RetentionPlan(
            reclaimable=False,
            highest_reclaimable_segment_start_lsn=None,
            cutoff_lsn=req.current_segment_start_lsn,
            protection_points={},
            blockers=blockers,
        )

    # Every protection point contributes an LSN that WAL must be retained at.
    points: dict[str, int] = {}

    # Rule 1: the in-use segment. Retain from its start (cutoff <= start).
    points["current_segment"] = req.current_segment_start_lsn

    # Rule 2: slowest physical standby.
    for standby in req.standbys:
        points[f"standby:{standby.name}"] = standby.replayed_lsn

    # Rule 3: backup pin.
    points["backup"] = req.backup_start_lsn

    # Rule 4: active replication slots.
    for slot in req.slots:
        if slot.restart_lsn is not None:
            points[f"slot:{slot.name}"] = slot.restart_lsn

    cutoff_lsn = min(points.values())
    highest = highest_reclaimable_segment_start(cutoff_lsn, req.segment_size)

    return RetentionPlan(
        reclaimable=highest is not None,
        highest_reclaimable_segment_start_lsn=highest,
        cutoff_lsn=cutoff_lsn,
        protection_points={name: format_lsn(lsn) for name, lsn in points.items()},
        blockers=[],
    )


def describe_boundary(lsn: int | None, segment_size: int) -> str | None:
    """Human-readable segment boundary, e.g. '000000010000000000000003'."""
    if lsn is None:
        return None
    # PostgreSQL segment file name: 8 hex timeline + 16 hex logical segment no.
    logical = lsn // segment_size
    return f"00000001{logical:016X}"


def segment_after(lsn: int, segment_size: int) -> int:
    return next_segment_start(lsn, segment_size)
