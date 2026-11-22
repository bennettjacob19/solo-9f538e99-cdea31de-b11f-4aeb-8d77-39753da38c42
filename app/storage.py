"""SQLite-backed storage for plans and the durable committed boundary.

Concurrency model
-----------------
SQLite access is wrapped in a process-wide re-entrant lock so the FastAPI
threadpool cannot interleave transactions.  Each commit runs inside a single
``BEGIN IMMEDIATE`` transaction: the expected-version check, the monotonic
boundary check and the update commit together, so a stale commit can never
advance the boundary and a failed write leaves the old boundary untouched.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plans (
    plan_id              TEXT PRIMARY KEY,
    snapshot_hash        TEXT NOT NULL UNIQUE,
    snapshot_json        TEXT NOT NULL,
    reclaimable          INTEGER NOT NULL,
    highest_segment_lsn  INTEGER,
    cutoff_lsn           INTEGER NOT NULL,
    protection_points    TEXT NOT NULL,
    blockers             TEXT NOT NULL,
    segment_size         INTEGER NOT NULL,
    created_at           TEXT NOT NULL,
    committed            INTEGER NOT NULL DEFAULT 0,
    commit_version       INTEGER
);

CREATE TABLE IF NOT EXISTS boundary_log (
    version      INTEGER PRIMARY KEY AUTOINCREMENT,
    old_lsn      INTEGER NOT NULL,
    new_lsn      INTEGER NOT NULL,
    plan_id      TEXT NOT NULL,
    committed_at TEXT NOT NULL
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class StaleVersionError(Exception):
    """The expected_version no longer matches the current boundary version."""


class BoundaryRegressionError(Exception):
    """A commit tried to move the boundary below its current value."""


class PlanNotFoundError(Exception):
    pass


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        if db_path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            isolation_level=None,  # explicit transaction control
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._bootstrap_boundary()

    def _bootstrap_boundary(self) -> None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'boundary_lsn'"
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO meta(key, value) VALUES ('boundary_lsn', '0')"
                )
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'boundary_version'"
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO meta(key, value) VALUES ('boundary_version', '0')"
                )
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'boundary_updated_at'"
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO meta(key, value) VALUES (?, ?)",
                    ("boundary_updated_at", _utcnow()),
                )

    # ------------------------------------------------------------------ plans

    def get_plan_by_hash(self, snapshot_hash: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM plans WHERE snapshot_hash = ?", (snapshot_hash,)
            ).fetchone()

    def get_plan(self, plan_id: str) -> sqlite3.Row:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM plans WHERE plan_id = ?", (plan_id,)
            ).fetchone()
            if row is None:
                raise PlanNotFoundError(plan_id)
            return row

    def insert_plan(
        self,
        plan_id: str,
        snapshot_hash: str,
        snapshot: dict[str, Any],
        reclaimable: bool,
        highest_segment_lsn: int | None,
        cutoff_lsn: int,
        protection_points: dict[str, str],
        blockers: list[dict[str, str]],
        segment_size: int,
    ) -> sqlite3.Row:
        """Persist a new plan; if the snapshot was seen before, return the old one.

        The UNIQUE constraint on snapshot_hash makes same-snapshot retries
        idempotent even under concurrent requests.
        """
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO plans(plan_id, snapshot_hash, snapshot_json,
                                      reclaimable, highest_segment_lsn, cutoff_lsn,
                                      protection_points, blockers, segment_size,
                                      created_at, committed, commit_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
                    """,
                    (
                        plan_id,
                        snapshot_hash,
                        json.dumps(snapshot, sort_keys=True),
                        int(reclaimable),
                        highest_segment_lsn,
                        cutoff_lsn,
                        json.dumps(protection_points, sort_keys=True),
                        json.dumps(blockers),
                        segment_size,
                        _utcnow(),
                    ),
                )
            except sqlite3.IntegrityError:
                # Identical snapshot already has a plan – retries return it.
                pass
            return self.get_plan_by_hash(snapshot_hash)  # type: ignore[return-value]

    def list_plans(self, limit: int = 100) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM plans ORDER BY rowid DESC LIMIT ?", (limit,)
                ).fetchall()
            )

    # --------------------------------------------------------------- boundary

    def get_boundary(self) -> dict[str, Any]:
        with self._lock:
            lsn = int(
                self._conn.execute(
                    "SELECT value FROM meta WHERE key = 'boundary_lsn'"
                ).fetchone()["value"]
            )
            version = int(
                self._conn.execute(
                    "SELECT value FROM meta WHERE key = 'boundary_version'"
                ).fetchone()["value"]
            )
            updated_at = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'boundary_updated_at'"
            ).fetchone()["value"]
            return {
                "boundary_lsn": lsn,
                "version": version,
                "updated_at": updated_at,
            }

    def commit_plan(
        self,
        plan_id: str,
        proposed_lsn: int | None,
        expected_version: int,
    ) -> dict[str, Any]:
        """Atomically advance the boundary according to a computed plan.

        Returns the resulting boundary state.  Raises StaleVersionError when
        the caller's expected_version is out of date, BoundaryRegressionError
        when the plan would move the boundary backwards.  Any SQLite failure
        rolls the transaction back and the previous boundary survives.
        """
        with self._lock:
            plan = self.get_plan(plan_id)
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                state = self.get_boundary()
                current_lsn = state["boundary_lsn"]
                current_version = state["version"]

                if expected_version != current_version:
                    raise StaleVersionError(
                        f"expected_version={expected_version} != "
                        f"current_version={current_version}"
                    )

                # Nothing reclaimable in this snapshot, or the proposal does
                # not pass the current boundary: keep the old boundary.
                if proposed_lsn is None or proposed_lsn <= current_lsn:
                    self._conn.execute("COMMIT")
                    return {
                        "committed": False,
                        "no_op": True,
                        "version": current_version,
                        "boundary_lsn": current_lsn,
                        "reason": (
                            "plan proposes no boundary advance "
                            "(no reclaimable segment or boundary cannot regress)"
                        ),
                    }

                new_version = current_version + 1
                now = _utcnow()
                self._conn.execute(
                    "UPDATE meta SET value = ? WHERE key = 'boundary_lsn'",
                    (str(proposed_lsn),),
                )
                self._conn.execute(
                    "UPDATE meta SET value = ? WHERE key = 'boundary_version'",
                    (str(new_version),),
                )
                self._conn.execute(
                    "UPDATE meta SET value = ? WHERE key = 'boundary_updated_at'",
                    (now,),
                )
                self._conn.execute(
                    """
                    INSERT INTO boundary_log(version, old_lsn, new_lsn,
                                             plan_id, committed_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (new_version, current_lsn, proposed_lsn, plan_id, now),
                )
                self._conn.execute(
                    "UPDATE plans SET committed = 1, commit_version = ? "
                    "WHERE plan_id = ?",
                    (new_version, plan_id),
                )
                self._conn.execute("COMMIT")
                return {
                    "committed": True,
                    "no_op": False,
                    "version": new_version,
                    "boundary_lsn": proposed_lsn,
                    "reason": None,
                }
            except StaleVersionError:
                self._conn.execute("ROLLBACK")
                raise
            except BoundaryRegressionError:
                self._conn.execute("ROLLBACK")
                raise
            except Exception:
                # Any write failure: roll back so the old boundary is retained.
                self._conn.execute("ROLLBACK")
                raise

    def boundary_log(self, limit: int = 50) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM boundary_log ORDER BY version DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()
