#!/usr/bin/env python3
"""End-to-end smoke test against a running server.

Usage:
    python scripts/smoke_test.py [base_url]

Default base URL: http://127.0.0.1:8000
The script uses only the Python standard library and exits non-zero on failure.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

SIZE = 16 * 1024 * 1024  # PostgreSQL default WAL segment size
BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"

_failures: list[str] = []


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def check(name: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        _failures.append(name)


def snapshot(current_seg: int, backup: int, standbys=None, slots=None) -> dict:
    return {
        "current_segment_start_lsn": current_seg * SIZE,
        "current_segment_end_lsn": (current_seg + 1) * SIZE,
        "backup_start_lsn": backup * SIZE,
        "segment_size": SIZE,
        "standbys": standbys or [],
        "slots": slots or [],
    }


def main() -> int:
    print(f"Smoke-testing {BASE}\n")

    code, body = call("GET", "/health")
    check("health endpoint", code == 200 and body["status"] == "ok")

    code, boundary = call("GET", "/boundary")
    check("initial boundary version is 0", code == 200 and boundary["version"] == 0)
    expected_version = boundary["version"]

    # 1) Basic plan: everything healthy, current segment 3 -> reclaim through seg 2.
    code, plan = call("POST", "/reclaim", snapshot(3, 4))
    check("reclaim returns 201", code == 201)
    check(
        "highest reclaimable segment is segment 2",
        plan["highest_reclaimable_segment_start_lsn"] == 2 * SIZE,
        str(plan),
    )
    plan_id = plan["plan_id"]

    # 2) Same snapshot retry -> identical plan id (idempotent).
    code, retry = call("POST", "/reclaim", snapshot(3, 4))
    check("same-snapshot retry returns same plan", retry["plan_id"] == plan_id)

    # 3) Commit with expected version.
    code, commit = call(
        "POST", f"/plans/{plan_id}/commit", {"expected_version": expected_version}
    )
    check(
        "commit advances boundary and version",
        code == 200 and commit["committed"] and commit["version"] == expected_version + 1,
        str(commit),
    )
    expected_version = commit["version"]

    # 4) Stale version must be rejected with 409.
    code, stale = call(
        "POST", f"/plans/{plan_id}/commit", {"expected_version": expected_version - 1}
    )
    check("stale commit rejected with 409", code == 409 and stale["detail"]["error"] == "stale_version")

    # 5) Lagging standby: laggard only inside segment 1 -> only segment 0 reclaimable.
    lag = snapshot(
        5, 6,
        standbys=[
            {"name": "fast", "replayed_lsn": 5 * SIZE},
            {"name": "laggard", "replayed_lsn": SIZE + 4096},
        ],
    )
    code, lag_plan = call("POST", "/reclaim", lag)
    check(
        "lagging standby caps reclaim at segment 0",
        lag_plan["highest_reclaimable_segment_start_lsn"] == 0,
        str(lag_plan),
    )
    # Committing it must not regress the boundary already at segment 2.
    code, lag_commit = call(
        "POST", f"/plans/{lag_plan['plan_id']}/commit",
        {"expected_version": expected_version},
    )
    check(
        "regressive plan is a no-op, boundary unchanged",
        lag_commit["no_op"] and not lag_commit["committed"]
        and lag_commit["version"] == expected_version,
        str(lag_commit),
    )

    # 6) Backup pin inside segment 2 -> highest complete segment is 1.
    pin = snapshot(
        5, 2,
        standbys=[{"name": "s", "replayed_lsn": 5 * SIZE}],
    )
    pin["backup_start_lsn"] = 2 * SIZE + 100
    code, pin_plan = call("POST", "/reclaim", pin)
    check(
        "backup pin drives cutoff",
        pin_plan["cutoff_lsn"] == 2 * SIZE + 100
        and pin_plan["highest_reclaimable_segment_start_lsn"] == SIZE,
        str(pin_plan),
    )

    # 7) Invalid slot blocks everything and cannot be ignored.
    blocked = snapshot(
        3, 4,
        slots=[{"name": "lost_slot", "active": False, "restart_lsn": SIZE}],
    )
    code, blocked_plan = call("POST", "/reclaim", blocked)
    check(
        "invalid slot is reported as blocker",
        not blocked_plan["reclaimable"]
        and blocked_plan["blockers"][0]["type"] == "invalid_slot"
        and blocked_plan["blockers"][0]["name"] == "lost_slot",
        str(blocked_plan),
    )
    code, blocked_commit = call(
        "POST", f"/plans/{blocked_plan['plan_id']}/commit",
        {"expected_version": expected_version},
    )
    check("blocked plan cannot commit an advance", blocked_commit["no_op"])

    # 8) Forward progress with the current version.
    code, ahead = call("POST", "/reclaim", snapshot(6, 7))
    code, ahead_commit = call(
        "POST", f"/plans/{ahead['plan_id']}/commit",
        {"expected_version": expected_version},
    )
    check(
        "forward commit succeeds at segment 5",
        ahead_commit["committed"] and ahead_commit["boundary_lsn"] == 5 * SIZE,
        str(ahead_commit),
    )

    # 9) Plan listing and boundary query.
    code, plans = call("GET", "/plans")
    check("/plans lists stored plans", code == 200 and len(plans) >= 4)
    code, final_boundary = call("GET", "/boundary")
    check(
        "final boundary persisted at segment 5",
        final_boundary["boundary_lsn"] == 5 * SIZE
        and final_boundary["version"] == expected_version + 1,
        str(final_boundary),
    )

    print()
    if _failures:
        print(f"{len(_failures)} check(s) FAILED: {', '.join(_failures)}")
        return 1
    print("All smoke checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
