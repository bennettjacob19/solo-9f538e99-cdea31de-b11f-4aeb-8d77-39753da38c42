"""Pydantic request/response models for the WAL retention API."""

from __future__ import annotations

from typing import Annotated, Optional

from pydantic import BaseModel, Field, field_validator

from .lsn import DEFAULT_SEGMENT_SIZE, parse_lsn, validate_segment_size

# An LSN may arrive as a uint64 integer or as PostgreSQL's 'X/Y' hex string.
LsnInput = Annotated[int | str, Field(description="LSN as uint64 integer or 'X/Y' hex string")]


class Standby(BaseModel):
    """A physical standby and the LSN it has replayed."""

    name: str = Field(min_length=1)
    replayed_lsn: LsnInput

    @field_validator("replayed_lsn")
    @classmethod
    def _parse(cls, v: int | str) -> int:
        return parse_lsn(v)


class ReplicationSlot(BaseModel):
    """A replication slot.

    ``restart_lsn`` is the oldest LSN the slot still needs.  Slots whose state
    is not ``active`` are treated as *invalidated blockers*: a disabled slot
    cannot be trusted to advance, so reclaim is blocked outright rather than
    ignoring it.
    """

    name: str = Field(min_length=1)
    active: bool
    restart_lsn: Optional[LsnInput] = None

    @field_validator("restart_lsn")
    @classmethod
    def _parse(cls, v: int | str | None) -> int | None:
        if v is None:
            return None
        return parse_lsn(v)


class ReclaimRequest(BaseModel):
    """Snapshot of all WAL retention protection points at one point in time."""

    current_segment_start_lsn: LsnInput = Field(
        description="Start LSN of the WAL segment currently being written"
    )
    current_segment_end_lsn: LsnInput = Field(
        description="End LSN (exclusive) of the WAL segment currently being written"
    )
    standbys: list[Standby] = Field(default_factory=list)
    backup_start_lsn: LsnInput = Field(
        description="Fixed start LSN pinned by the in-flight backup task"
    )
    slots: list[ReplicationSlot] = Field(default_factory=list)
    segment_size: int = Field(
        default=DEFAULT_SEGMENT_SIZE,
        description="WAL segment size in bytes (power of two, 1 MiB..1 GiB)",
    )

    @field_validator("current_segment_start_lsn", "current_segment_end_lsn", "backup_start_lsn")
    @classmethod
    def _parse_lsn(cls, v: int | str) -> int:
        return parse_lsn(v)

    @field_validator("segment_size")
    @classmethod
    def _check_size(cls, v: int) -> int:
        validate_segment_size(v)
        return v


class CommitRequest(BaseModel):
    """Optimistic-concurrency commit of a previously computed plan."""

    expected_version: int = Field(ge=0, description="Version the caller saw before committing")


class PlanResponse(BaseModel):
    plan_id: str
    snapshot_hash: str
    reclaimable: bool
    highest_reclaimable_segment: Optional[str] = None
    highest_reclaimable_segment_start_lsn: Optional[int] = None
    cutoff_lsn: int
    cutoff_lsn_text: str
    protection_points: dict[str, str]
    blockers: list[dict[str, str]]
    segment_size: int
    committed_version: int
    committed_boundary_lsn: int
    committed_boundary_lsn_text: str
    created_at: str


class CommitResponse(BaseModel):
    plan_id: str
    committed: bool
    no_op: bool
    version: int
    boundary_lsn: int
    boundary_lsn_text: str
    reason: Optional[str] = None


class BoundaryResponse(BaseModel):
    version: int
    boundary_lsn: int
    boundary_lsn_text: str
    updated_at: Optional[str] = None


class PlanSummary(BaseModel):
    plan_id: str
    snapshot_hash: str
    reclaimable: bool
    highest_reclaimable_segment_start_lsn: Optional[int]
    created_at: str
    committed: bool
    commit_version: Optional[int] = None
