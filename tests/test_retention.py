"""Unit tests for LSN arithmetic and the pure retention engine."""

import pytest

from app.lsn import (
    DEFAULT_SEGMENT_SIZE,
    format_lsn,
    highest_reclaimable_segment_start,
    next_segment_start,
    parse_lsn,
    segment_start,
    validate_segment_size,
)
from app.retention import compute_plan, snapshot_hash
from app.schemas import ReclaimRequest, ReplicationSlot, Standby

SIZE = DEFAULT_SEGMENT_SIZE  # 16 MiB
SEG0 = 0
SEG1 = SIZE
SEG2 = 2 * SIZE
SEG3 = 3 * SIZE
SEG4 = 4 * SIZE


# --------------------------------------------------------------------- LSN I/O

def test_parse_lsn_hex_and_int_equivalent():
    assert parse_lsn("0/1000000") == SEG1
    assert parse_lsn("1/00000000") == 1 << 32
    assert parse_lsn(SEG2) == SEG2


def test_parse_lsn_rejects_garbage():
    for bad in ["abc", "12/3/4", "", "-1/2"]:
        with pytest.raises(ValueError):
            parse_lsn(bad)
    with pytest.raises(ValueError):
        parse_lsn(-1)


def test_format_roundtrip():
    for lsn in (0, SEG1, SEG1 + 4096, (1 << 64) - 1):
        assert parse_lsn(format_lsn(lsn)) == lsn
    assert format_lsn(SEG1) == "0/01000000"


def test_segment_helpers():
    assert segment_start(SEG2 + 1234, SIZE) == SEG2
    assert next_segment_start(SEG2 + 1234, SIZE) == SEG3
    assert segment_start(SEG3, SIZE) == SEG3


def test_highest_reclaimable_complete_segment_only():
    # Cutoff inside segment 2 -> segments 0..1 fully below it -> highest is SEG1
    assert highest_reclaimable_segment_start(SEG2 + 5, SIZE) == SEG1
    # Cutoff exactly on the SEG2 boundary -> SEG2 not fully below -> highest SEG1
    assert highest_reclaimable_segment_start(SEG2, SIZE) == SEG1
    # Cutoff below the first segment end -> nothing fully reclaimable
    assert highest_reclaimable_segment_start(SIZE - 1, SIZE) is None
    assert highest_reclaimable_segment_start(0, SIZE) is None


def test_validate_segment_size():
    validate_segment_size(SIZE)
    validate_segment_size(1024 * 1024)
    for bad in (3, SIZE + 1, 0, 2 * 1024 * 1024 * 1024):
        with pytest.raises(ValueError):
            validate_segment_size(bad)


# ------------------------------------------------------------- plan computation

def _req(
    standbys=None,
    backup_start_lsn=SEG4,
    slots=None,
    current_start=SEG3,
    segment_size=SIZE,
):
    return ReclaimRequest(
        current_segment_start_lsn=current_start,
        current_segment_end_lsn=current_start + segment_size,
        standbys=standbys or [],
        backup_start_lsn=backup_start_lsn,
        slots=slots or [],
        segment_size=segment_size,
    )


def test_no_protection_below_current_segment_boundary():
    # Only the current segment protects; cutoff = SEG3, so segments 0..2 go.
    plan = compute_plan(_req())
    assert plan.reclaimable is True
    assert plan.highest_reclaimable_segment_start_lsn == SEG2
    assert plan.cutoff_lsn == SEG3


def test_lagging_standby_is_binding_protection_point():
    # Fast standby is at SEG4; the lagging one only replayed into SEG1.
    req = _req(
        standbys=[
            Standby(name="fast", replayed_lsn=SEG4),
            Standby(name="laggard", replayed_lsn=SEG1 + 10),
        ]
    )
    plan = compute_plan(req)
    # Cutoff is the laggard's LSN, inside segment 1 -> only segment 0 is whole.
    assert plan.cutoff_lsn == SEG1 + 10
    assert plan.highest_reclaimable_segment_start_lsn == SEG0
    assert plan.protection_points["standby:laggard"] == format_lsn(SEG1 + 10)


def test_standby_exactly_on_boundary_does_not_free_that_segment():
    req = _req(standbys=[Standby(name="s", replayed_lsn=SEG2)])
    plan = compute_plan(req)
    assert plan.highest_reclaimable_segment_start_lsn == SEG1


def test_backup_pin_drives_the_cutoff():
    req = _req(
        standbys=[Standby(name="s", replayed_lsn=SEG4)],
        backup_start_lsn=SEG1 + 99,
    )
    plan = compute_plan(req)
    assert plan.cutoff_lsn == SEG1 + 99
    assert plan.highest_reclaimable_segment_start_lsn == SEG0


def test_active_slot_restart_lsn_is_a_protection_point():
    req = _req(
        slots=[
            ReplicationSlot(name="slot_a", active=True, restart_lsn=SEG2 + 7),
        ],
        backup_start_lsn=SEG4,
    )
    plan = compute_plan(req)
    assert plan.cutoff_lsn == SEG2 + 7
    assert plan.highest_reclaimable_segment_start_lsn == SEG1


def test_inactive_slot_is_blocker_and_cannot_be_ignored():
    req = _req(
        slots=[
            ReplicationSlot(name="dead_slot", active=False, restart_lsn=SEG1),
            ReplicationSlot(name="ok_slot", active=True, restart_lsn=SEG3),
        ],
        backup_start_lsn=SEG4,
    )
    plan = compute_plan(req)
    assert plan.reclaimable is False
    assert plan.highest_reclaimable_segment_start_lsn is None
    assert len(plan.blockers) == 1
    assert plan.blockers[0]["type"] == "invalid_slot"
    assert plan.blockers[0]["name"] == "dead_slot"


def test_active_slot_without_restart_lsn_does_not_pin():
    # A brand-new active slot may have NULL restart_lsn: it pins nothing.
    req = _req(
        slots=[ReplicationSlot(name="fresh", active=True, restart_lsn=None)],
        backup_start_lsn=SEG4,
    )
    plan = compute_plan(req)
    assert plan.reclaimable is True
    assert plan.highest_reclaimable_segment_start_lsn == SEG2


def test_current_segment_validation():
    bad = ReclaimRequest(
        current_segment_start_lsn=SEG1 + 1,  # misaligned
        current_segment_end_lsn=SEG2 + 1,
        backup_start_lsn=SEG4,
    )
    with pytest.raises(ValueError):
        compute_plan(bad)

    bad2 = ReclaimRequest(
        current_segment_start_lsn=SEG1,
        current_segment_end_lsn=SEG3,  # not exactly one segment
        backup_start_lsn=SEG4,
    )
    with pytest.raises(ValueError):
        compute_plan(bad2)


def test_snapshot_hash_is_order_independent_and_stable():
    a = _req(
        standbys=[
            Standby(name="a", replayed_lsn=SEG1),
            Standby(name="b", replayed_lsn=SEG2),
        ]
    )
    b = _req(
        standbys=[
            Standby(name="b", replayed_lsn=SEG2),
            Standby(name="a", replayed_lsn=SEG1),
        ]
    )
    assert snapshot_hash(a) == snapshot_hash(b)
    c = _req(standbys=[Standby(name="a", replayed_lsn=SEG1 + 1)])
    assert snapshot_hash(a) != snapshot_hash(c)


def test_text_and_integer_lsn_produce_same_snapshot():
    req_int = _req(backup_start_lsn=SEG2)
    req_text = _req(backup_start_lsn=format_lsn(SEG2))
    assert snapshot_hash(req_int) == snapshot_hash(req_text)
