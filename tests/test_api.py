"""End-to-end API tests via FastAPI's TestClient.

Covers the required scenarios: lagging standby, backup pin, invalid-slot
blocker, idempotent same-snapshot retries, stale optimistic commits, monotonic
boundary and persistence across a simulated restart.
"""

from __future__ import annotations

import importlib
import os
import tempfile

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(monkeypatch):
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    monkeypatch.setenv("WAL_REHEARSAL_DB", tmp.name)
    import app.main as main
    importlib.reload(main)  # pick up the env-driven DB path
    storage = main.Storage(tmp.name)
    main._storage = storage
    # TestClient without a context manager does not run lifespan events, so the
    # storage we installed above stays in control for the whole test.
    c = TestClient(main.app)
    c.db_path = tmp.name
    c.main_module = main
    c.storage = storage
    yield c
    storage.close()
    main._storage = None
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(tmp.name + suffix)
        except FileNotFoundError:
            pass


def _snapshot(
    current_start,
    backup_lsn,
    standbys=None,
    slots=None,
    segment_size=16 * 1024 * 1024,
):
    return {
        "current_segment_start_lsn": current_start,
        "current_segment_end_lsn": current_start + segment_size,
        "backup_start_lsn": backup_lsn,
        "standbys": standbys or [],
        "slots": slots or [],
        "segment_size": segment_size,
    }


SIZE = 16 * 1024 * 1024


# --------------------------------------------------------------------- basics

def test_health_and_initial_boundary(client):
    assert client.get("/health").json() == {"status": "ok"}
    boundary = client.get("/boundary").json()
    assert boundary == {
        "version": 0,
        "boundary_lsn": 0,
        "boundary_lsn_text": "0/00000000",
        "updated_at": boundary["updated_at"],
    }


def test_reclaim_then_commit_advances_boundary(client):
    snap = _snapshot(current_start=3 * SIZE, backup_lsn=4 * SIZE)
    r = client.post("/reclaim", json=snap)
    assert r.status_code == 201
    plan = r.json()
    assert plan["reclaimable"] is True
    assert plan["highest_reclaimable_segment_start_lsn"] == 2 * SIZE
    assert plan["highest_reclaimable_segment"].startswith("00000001")

    commit = client.post(
        f"/plans/{plan['plan_id']}/commit",
        json={"expected_version": 0},
    )
    assert commit.status_code == 200
    body = commit.json()
    assert body["committed"] is True
    assert body["no_op"] is False
    assert body["version"] == 1
    assert body["boundary_lsn"] == 2 * SIZE

    assert client.get("/boundary").json()["version"] == 1


def test_same_snapshot_retry_returns_same_plan(client):
    snap = _snapshot(
        current_start=3 * SIZE,
        backup_lsn=4 * SIZE,
        standbys=[{"name": "s1", "replayed_lsn": 2 * SIZE},
                  {"name": "s2", "replayed_lsn": "0/3000000"}],
    )
    first = client.post("/reclaim", json=snap).json()

    # Identical semantics, different JSON layout: reordered standbys and the
    # same LSNs expressed as integers instead of hex text.
    snap2 = {
        "current_segment_start_lsn": 3 * SIZE,
        "current_segment_end_lsn": 4 * SIZE,
        "backup_start_lsn": 4 * SIZE,
        "segment_size": SIZE,
        "slots": [],
        "standbys": [
            {"name": "s2", "replayed_lsn": 3 * SIZE},
            {"name": "s1", "replayed_lsn": 2 * SIZE},
        ],
    }
    second = client.post("/reclaim", json=snap2).json()
    assert second["plan_id"] == first["plan_id"]
    assert second["snapshot_hash"] == first["snapshot_hash"]

    # And a plain byte-for-byte retry stays stable too.
    retry = client.post("/reclaim", json=snap).json()
    assert retry["plan_id"] == first["plan_id"]
    plans = client.get("/plans").json()
    matching = [p for p in plans if p["plan_id"] == first["plan_id"]]
    assert len(matching) == 1


# ---------------------------------------------------------- required scenarios

def test_lagging_standby_limits_reclaim(client):
    snap = _snapshot(
        current_start=5 * SIZE,
        backup_lsn=6 * SIZE,
        standbys=[
            {"name": "fast", "replayed_lsn": 5 * SIZE},
            {"name": "laggard", "replayed_lsn": SIZE + 4096},
        ],
    )
    plan = client.post("/reclaim", json=snap).json()
    # Laggard is inside segment 1: only segment 0 is fully below its LSN.
    assert plan["highest_reclaimable_segment_start_lsn"] == 0
    assert plan["protection_points"]["standby:laggard"] == (
        f"0/{SIZE + 4096:08X}"
    )


def test_backup_pin_blocks_reclaim_beyond_pin(client):
    snap = _snapshot(
        current_start=5 * SIZE,
        backup_lsn=2 * SIZE + 100,
        standbys=[{"name": "s", "replayed_lsn": 5 * SIZE}],
    )
    plan = client.post("/reclaim", json=snap).json()
    assert plan["cutoff_lsn"] == 2 * SIZE + 100
    assert plan["highest_reclaimable_segment_start_lsn"] == SIZE


def test_invalid_slot_is_blocker(client):
    snap = _snapshot(
        current_start=3 * SIZE,
        backup_lsn=4 * SIZE,
        slots=[{"name": "lost_slot", "active": False, "restart_lsn": SIZE}],
    )
    plan = client.post("/reclaim", json=snap).json()
    assert plan["reclaimable"] is False
    assert plan["blockers"][0]["name"] == "lost_slot"
    assert plan["highest_reclaimable_segment_start_lsn"] is None

    # Committing a blocked plan must not move the boundary.
    commit = client.post(
        f"/plans/{plan['plan_id']}/commit", json={"expected_version": 0}
    ).json()
    assert commit["committed"] is False
    assert commit["no_op"] is True
    assert client.get("/boundary").json()["version"] == 0


def test_stale_version_commit_is_rejected(client):
    snap = _snapshot(current_start=3 * SIZE, backup_lsn=4 * SIZE)
    plan1 = client.post("/reclaim", json=snap).json()
    assert client.post(
        f"/plans/{plan1['plan_id']}/commit", json={"expected_version": 0}
    ).json()["version"] == 1

    # Re-commit with the old expected version.
    stale = client.post(
        f"/plans/{plan1['plan_id']}/commit", json={"expected_version": 0}
    )
    assert stale.status_code == 409
    assert stale.json()["detail"]["error"] == "stale_version"
    assert stale.json()["detail"]["current_version"] == 1


def test_boundary_can_only_move_forward(client):
    # First commit advances to segment 2.
    snap_far = _snapshot(current_start=4 * SIZE, backup_lsn=5 * SIZE)
    plan_far = client.post("/reclaim", json=snap_far).json()
    client.post(
        f"/plans/{plan_far['plan_id']}/commit", json={"expected_version": 0}
    )
    assert client.get("/boundary").json()["boundary_lsn"] == 3 * SIZE

    # Protection points regress (new snapshot whose safe cut is segment 0):
    # recomputation succeeds, but committing must not move the boundary back.
    snap_near = _snapshot(
        current_start=2 * SIZE,
        backup_lsn=SIZE + 10,
        standbys=[{"name": "s", "replayed_lsn": SIZE + 10}],
    )
    plan_near = client.post("/reclaim", json=snap_near).json()
    assert plan_near["highest_reclaimable_segment_start_lsn"] == 0
    commit = client.post(
        f"/plans/{plan_near['plan_id']}/commit", json={"expected_version": 1}
    ).json()
    assert commit["committed"] is False
    assert commit["no_op"] is True
    boundary = client.get("/boundary").json()
    assert boundary["boundary_lsn"] == 3 * SIZE
    assert boundary["version"] == 1  # version unchanged on no-op


def test_forward_commit_after_seeing_new_version(client):
    snap_a = _snapshot(current_start=3 * SIZE, backup_lsn=4 * SIZE)
    plan_a = client.post("/reclaim", json=snap_a).json()
    client.post(f"/plans/{plan_a['plan_id']}/commit", json={"expected_version": 0})

    snap_b = _snapshot(current_start=5 * SIZE, backup_lsn=6 * SIZE)
    plan_b = client.post("/reclaim", json=snap_b).json()
    ok = client.post(
        f"/plans/{plan_b['plan_id']}/commit", json={"expected_version": 1}
    )
    assert ok.status_code == 200
    assert ok.json()["version"] == 2
    assert ok.json()["boundary_lsn"] == 4 * SIZE


def test_plan_lookup_and_404(client):
    assert client.get("/plans/does-not-exist").status_code == 404
    plan = client.post(
        "/reclaim", json=_snapshot(current_start=2 * SIZE, backup_lsn=3 * SIZE)
    ).json()
    fetched = client.get(f"/plans/{plan['plan_id']}")
    assert fetched.status_code == 200
    assert fetched.json()["plan_id"] == plan["plan_id"]


def test_invalid_current_segment_returns_422(client):
    bad = _snapshot(current_start=SIZE + 1, backup_lsn=4 * SIZE)
    r = client.post("/reclaim", json=bad)
    assert r.status_code == 422


def test_persistence_across_simulated_restart(client):
    db_path = client.db_path
    main = client.main_module
    snap = _snapshot(current_start=3 * SIZE, backup_lsn=4 * SIZE)
    plan = client.post("/reclaim", json=snap).json()
    client.post(f"/plans/{plan['plan_id']}/commit", json={"expected_version": 0})

    # Simulate process restart: drop all in-memory state and reopen the DB.
    client.storage.close()
    main._storage = None
    reloaded = main.Storage(db_path)
    main._storage = reloaded
    client.storage = reloaded
    try:
        boundary = client.get("/boundary").json()
        assert boundary["version"] == 1
        assert boundary["boundary_lsn"] == 2 * SIZE
        # The plan survived too and is still retrievable.
        fetched = client.get(f"/plans/{plan['plan_id']}").json()
        assert fetched["snapshot_hash"] == plan["snapshot_hash"]
        # Same snapshot after restart still maps to the same stored plan.
        again = client.post("/reclaim", json=snap).json()
        assert again["plan_id"] == plan["plan_id"]
    finally:
        pass


def test_failed_write_keeps_old_boundary(client, monkeypatch):
    # First establish a committed boundary.
    snap = _snapshot(current_start=3 * SIZE, backup_lsn=4 * SIZE)
    plan = client.post("/reclaim", json=snap).json()
    client.post(f"/plans/{plan['plan_id']}/commit", json={"expected_version": 0})

    storage = client.main_module.get_storage()
    real_execute = storage._conn.execute

    class FailingConnection:
        """Proxy that fails one UPDATE inside the commit transaction."""

        def __getattr__(self, name):
            return getattr(storage._conn, name)

        def execute(self, sql, *args, **kwargs):
            if isinstance(sql, str) and "UPDATE meta" in sql and "boundary_lsn" in sql:
                raise RuntimeError("simulated disk failure")
            return real_execute(sql, *args, **kwargs)

    storage._conn = FailingConnection()
    try:
        snap2 = _snapshot(current_start=5 * SIZE, backup_lsn=6 * SIZE)
        plan2 = client.post("/reclaim", json=snap2).json()
        with pytest.raises(RuntimeError, match="simulated disk failure"):
            storage.commit_plan(plan2["plan_id"], 4 * SIZE, expected_version=1)
    finally:
        storage._conn = real_execute.__self__

    boundary = client.get("/boundary").json()
    assert boundary["version"] == 1
    assert boundary["boundary_lsn"] == 2 * SIZE  # old boundary retained
    # Storage is still usable after the rolled-back transaction.
    snap3 = _snapshot(current_start=6 * SIZE, backup_lsn=7 * SIZE)
    plan3 = client.post("/reclaim", json=snap3).json()
    ok = client.post(
        f"/plans/{plan3['plan_id']}/commit", json={"expected_version": 1}
    ).json()
    assert ok["committed"] is True
    assert ok["version"] == 2
