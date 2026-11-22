"""FastAPI application: PostgreSQL WAL retention-boundary rehearsal API."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from .lsn import format_lsn
from .retention import (
    canonical_snapshot,
    compute_plan,
    describe_boundary,
    snapshot_hash,
)
from .schemas import (
    BoundaryResponse,
    CommitRequest,
    CommitResponse,
    PlanResponse,
    PlanSummary,
    ReclaimRequest,
)
from .storage import (
    BoundaryRegressionError,
    PlanNotFoundError,
    StaleVersionError,
    Storage,
)

DB_PATH = os.environ.get("WAL_REHEARSAL_DB", "/data/wal_retention.db")
_storage: Storage | None = None


def get_storage() -> Storage:
    if _storage is None:
        raise RuntimeError("storage not initialised")
    return _storage


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _storage
    _storage = Storage(DB_PATH)
    yield
    _storage.close()
    _storage = None


app = FastAPI(
    title="PostgreSQL WAL Retention Boundary Rehearsal API",
    version="1.0.0",
    description=(
        "Compute and durably commit the highest complete WAL segment that can "
        "be safely recycled given standbys, backup pins and replication slots."
    ),
    lifespan=lifespan,
)


def _plan_response(row) -> PlanResponse:
    boundary = get_storage().get_boundary()
    highest = row["highest_segment_lsn"]
    return PlanResponse(
        plan_id=row["plan_id"],
        snapshot_hash=row["snapshot_hash"],
        reclaimable=bool(row["reclaimable"]),
        highest_reclaimable_segment=describe_boundary(highest, row["segment_size"]),
        highest_reclaimable_segment_start_lsn=highest,
        cutoff_lsn=row["cutoff_lsn"],
        cutoff_lsn_text=format_lsn(row["cutoff_lsn"]),
        protection_points=json.loads(row["protection_points"]),
        blockers=json.loads(row["blockers"]),
        segment_size=row["segment_size"],
        committed_version=boundary["version"],
        committed_boundary_lsn=boundary["boundary_lsn"],
        committed_boundary_lsn_text=format_lsn(boundary["boundary_lsn"]),
        created_at=row["created_at"],
    )


@app.post("/reclaim", response_model=PlanResponse, status_code=201)
def reclaim(req: ReclaimRequest) -> PlanResponse:
    """Compute (or return the cached) reclaim plan for a protection-point snapshot.

    The same snapshot always returns the same plan: retries are idempotent and
    keyed by a canonical hash of every protection point in the request.
    """
    try:
        plan = compute_plan(req)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    snap_hash = snapshot_hash(req)
    storage = get_storage()
    row = storage.get_plan_by_hash(snap_hash)
    if row is None:
        row = storage.insert_plan(
            plan_id=uuid.uuid4().hex,
            snapshot_hash=snap_hash,
            snapshot=canonical_snapshot(req),
            reclaimable=plan.reclaimable,
            highest_segment_lsn=plan.highest_reclaimable_segment_start_lsn,
            cutoff_lsn=plan.cutoff_lsn,
            protection_points=plan.protection_points,
            blockers=plan.blockers,
            segment_size=req.segment_size,
        )
    return _plan_response(row)


@app.post("/plans/{plan_id}/commit", response_model=CommitResponse)
def commit_plan(plan_id: str, req: CommitRequest) -> CommitResponse:
    """Commit a plan with optimistic concurrency control.

    The boundary may only move forward. A stale ``expected_version`` is
    rejected with 409 and never changes any state.
    """
    storage = get_storage()
    try:
        plan_row = storage.get_plan(plan_id)
    except PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail="plan not found") from exc

    try:
        result = storage.commit_plan(
            plan_id=plan_id,
            proposed_lsn=plan_row["highest_segment_lsn"],
            expected_version=req.expected_version,
        )
    except StaleVersionError as exc:
        state = storage.get_boundary()
        raise HTTPException(
            status_code=409,
            detail={
                "error": "stale_version",
                "message": str(exc),
                "current_version": state["version"],
                "current_boundary_lsn": state["boundary_lsn"],
            },
        ) from exc
    except BoundaryRegressionError as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return CommitResponse(
        plan_id=plan_id,
        committed=result["committed"],
        no_op=result["no_op"],
        version=result["version"],
        boundary_lsn=result["boundary_lsn"],
        boundary_lsn_text=format_lsn(result["boundary_lsn"]),
        reason=result["reason"],
    )


@app.get("/plans", response_model=list[PlanSummary])
def list_plans(limit: int = 100) -> list[PlanSummary]:
    rows = get_storage().list_plans(limit=min(limit, 500))
    return [
        PlanSummary(
            plan_id=r["plan_id"],
            snapshot_hash=r["snapshot_hash"],
            reclaimable=bool(r["reclaimable"]),
            highest_reclaimable_segment_start_lsn=r["highest_segment_lsn"],
            created_at=r["created_at"],
            committed=bool(r["committed"]),
            commit_version=r["commit_version"],
        )
        for r in rows
    ]


@app.get("/plans/{plan_id}", response_model=PlanResponse)
def get_plan(plan_id: str) -> PlanResponse:
    try:
        row = get_storage().get_plan(plan_id)
    except PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail="plan not found") from exc
    return _plan_response(row)


@app.get("/boundary", response_model=BoundaryResponse)
def get_boundary() -> BoundaryResponse:
    state = get_storage().get_boundary()
    return BoundaryResponse(
        version=state["version"],
        boundary_lsn=state["boundary_lsn"],
        boundary_lsn_text=format_lsn(state["boundary_lsn"]),
        updated_at=state["updated_at"],
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
