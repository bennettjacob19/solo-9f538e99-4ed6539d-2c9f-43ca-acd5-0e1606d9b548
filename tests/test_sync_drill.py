"""End-to-end tests for the synchronous-commit (remote_apply) drill.

Requires a real primary/standby pair:

    TEST_DATABASE_URL=postgresql://drill@127.0.0.1:55432/drill
    TEST_STANDBY_URL=postgresql://drill@127.0.0.1:55433/drill

The standby must be a physical streaming replica named ``standby1`` in
``synchronous_standby_names`` (see docker/postgres and the local
setup_cluster.sh helper). Tests pause and resume WAL replay, so the instances
must be throwaway.
"""

from __future__ import annotations

import importlib
import json
import os
import re
import sys
import time

import psycopg
import pytest
from fastapi.testclient import TestClient

PRIMARY_URL = os.environ.get("TEST_DATABASE_URL")
STANDBY_URL = os.environ.get("TEST_STANDBY_URL")

pytestmark = pytest.mark.skipif(
    not (PRIMARY_URL and STANDBY_URL),
    reason="TEST_DATABASE_URL/TEST_STANDBY_URL not set; replication tests skipped",
)


@pytest.fixture()
def web(tmp_path, monkeypatch):
    """Fresh app + stores per test, mimicking a web restart."""
    monkeypatch.setenv("DATABASE_URL", PRIMARY_URL)
    monkeypatch.setenv("STANDBY_URL", STANDBY_URL)
    monkeypatch.setenv("SYNC_STATE_FILE", str(tmp_path / "sync_drills.json"))
    monkeypatch.setenv("SYNC_WAIT_CONFIRM_SECONDS", "10")
    monkeypatch.setenv("SYNC_COMMIT_SECONDS", "20")
    monkeypatch.setenv("SYNC_CATCHUP_SECONDS", "20")
    for name in list(sys.modules):
        if name.startswith("app."):
            del sys.modules[name]
    import app.config
    import app.db
    import app.drill
    import app.sync_drill
    import app.main

    importlib.reload(app.config)
    importlib.reload(app.db)
    app.drill = importlib.reload(app.drill)
    app.sync_drill = importlib.reload(app.sync_drill)
    app.main = importlib.reload(app.main)
    client = TestClient(app.main.app)
    with client:
        yield client, app.sync_drill.sync_store, app.sync_drill
    app.main.sync_store.shutdown()


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _start(client) -> str:
    res = client.post("/sync/start", follow_redirects=False)
    assert res.status_code == 303, res.text
    return res.headers["location"].rsplit("/", 1)[-1]


def _token(client, sync_id: str) -> str:
    html = client.get(f"/sync/{sync_id}").text
    return re.search(r'name="token" value="([^"]+)"', html).group(1)


def _state(client, sync_id: str) -> dict:
    return client.get(f"/api/sync/state?sync_id={sync_id}").json()["sync"]


def _post(client, sync_id: str, action: str, token: str, **extra) -> dict:
    data = {"token": token, **extra}
    res = client.post(f"/api/sync/{sync_id}/{action}", data=data)
    assert res.status_code == 200, res.text
    return res.json()


def _wait_for(client, sync_id: str, predicate, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = _state(client, sync_id)
        if predicate(last):
            return last
        time.sleep(0.2)
    raise AssertionError(f"condition not met; last state={json.dumps(last, ensure_ascii=False)[:800]}")


def _standby(sql: str):
    with psycopg.connect(STANDBY_URL, autocommit=True) as conn:
        return conn.execute(sql).fetchone()


def _primary(sql: str, params=None):
    with psycopg.connect(PRIMARY_URL, autocommit=True) as conn:
        return conn.execute(sql, params).fetchone()


def _ensure_replay_running():
    with psycopg.connect(STANDBY_URL, autocommit=True) as conn:
        if conn.execute("SELECT pg_is_wal_replay_paused()").fetchone()[0]:
            conn.execute("SELECT pg_wal_replay_resume()")
            deadline = time.time() + 10
            while time.time() < deadline:
                if not conn.execute("SELECT pg_is_wal_replay_paused()").fetchone()[0]:
                    break
                time.sleep(0.1)


@pytest.fixture(autouse=True)
def _clean_replay():
    _ensure_replay_running()
    # Tables are replicated; truncating on the primary cleans both sides.
    with psycopg.connect(PRIMARY_URL, autocommit=True) as conn:
        conn.execute("TRUNCATE TABLE deploy_order")
        conn.execute("UPDATE lock_drill_row SET value = 0 WHERE id = 1")
    deadline = time.time() + 10
    while time.time() < deadline:
        with psycopg.connect(STANDBY_URL, autocommit=True) as conn:
            left = conn.execute("SELECT count(*) FROM deploy_order").fetchone()[0]
        if left == 0:
            break
        time.sleep(0.2)
    yield
    _ensure_replay_running()


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------


def test_cancel_wait_flow_primary_yes_standby_no_then_catchup(web):
    client, store, mod = web
    sync_id = _start(client)
    token = _token(client, sync_id)

    # Pause replay and wait for the pause to be confirmed.
    _post(client, sync_id, "pause", token)
    snap = _state(client, sync_id)
    assert snap["status"] == "paused"
    assert snap["standby_paused"] is True
    assert _standby("SELECT pg_is_wal_replay_paused()")[0] is True

    # Submit an order with remote_apply.
    order = "DEPLOY-CANCEL-001"
    _post(client, sync_id, "submit", token, order_no=order, note="cancel case")

    # The page must observe the real SyncRep wait before cancelling is allowed.
    snap = _wait_for(
        client, sync_id, lambda s: s["status"] == "waiting" and s["waiting_confirmed"]
    )
    assert snap["can_cancel"] is True
    assert snap["commit_session"]["wait_event"] in mod.SYNC_WAIT_EVENTS
    assert snap["commit_session"]["wait_event_type"] == "IPC"

    # Cancelling without the proper credential is refused.
    bad = client.post(f"/api/sync/{sync_id}/cancel", data={"token": "wrong"})
    assert bad.status_code == 403
    assert bad.json()["kind"] == "SyncDrillTokenMismatch"

    body = _post(client, sync_id, "cancel", token)
    result = body["result"]
    assert result["outcome"] == "cancelled"
    assert result["label"] == "取消等待"
    assert result["terminated"] is True

    # While replay stays paused: primary has the row, standby does not.
    snap = _state(client, sync_id)
    assert snap["verification"]["primary"]["found"] is True
    assert snap["verification"]["primary"]["confirmed"] is True
    assert snap["verification"]["standby"]["found"] is False
    assert snap["verification"]["standby"]["confirmed"] is True
    assert snap["verification"]["replay_paused"] is True
    assert snap["caught_up"] is False
    assert _primary("SELECT count(*) FROM deploy_order WHERE order_no = %s", (order,))[0] == 1
    assert _standby(f"SELECT count(*) FROM deploy_order WHERE order_no = '{order}'")[0] == 0

    # Idempotent: cancelling again returns the same verdict.
    again = _post(client, sync_id, "cancel", token)
    assert again["result"] == result

    # Resume replay; the standby catches up and both sides show the order.
    _post(client, sync_id, "resume", token)
    snap = _wait_for(client, sync_id, lambda s: s["caught_up"] is True, timeout=20)
    assert snap["verification"]["primary"]["found"] is True
    assert snap["verification"]["standby"]["found"] is True
    assert snap["verification"]["replay_paused"] is False
    assert _standby(f"SELECT count(*) FROM deploy_order WHERE order_no = '{order}'")[0] == 1
    # The original verdict is preserved after catch-up.
    assert snap["result"]["label"] == "取消等待"
    assert snap["result"]["outcome"] == "cancelled"


def test_normal_remote_apply_commit_is_synced(web):
    client, store, mod = web
    sync_id = _start(client)
    token = _token(client, sync_id)
    _post(client, sync_id, "pause", token)

    order = "DEPLOY-SYNC-001"
    _post(client, sync_id, "submit", token, order_no=order)
    _wait_for(client, sync_id, lambda s: s["status"] == "waiting")

    # Replay resumes: the blocked remote_apply COMMIT then completes normally.
    _post(client, sync_id, "resume", token)
    snap = _wait_for(
        client, sync_id, lambda s: s["is_finished"], timeout=20
    )
    assert snap["status"] == "synced"
    assert snap["result"]["label"] == "已同步确认"
    assert snap["result"]["outcome"] == "synced"
    assert snap["result"]["terminated"] is False
    assert snap["verification"]["primary"]["found"] is True
    assert snap["verification"]["standby"]["found"] is True
    assert snap["caught_up"] is True


def test_cancel_guard_requires_confirmed_wait(web):
    """The cancel endpoint must refuse anything that is not a confirmed
    synchronous-replication wait: a fresh paused round and a finished round."""
    client, store, mod = web
    sync_id = _start(client)
    token = _token(client, sync_id)

    # No commit session exists yet: cancelling is refused.
    before = client.post(f"/api/sync/{sync_id}/cancel", data={"token": token})
    assert before.status_code == 409
    assert before.json()["kind"] == "SyncDrillConflict"

    # Pause and run a normal commit through to completion; a finished round
    # can never be cancelled again.
    _post(client, sync_id, "pause", token)
    _post(client, sync_id, "submit", token, order_no="DEPLOY-EARLY-001")
    _wait_for(client, sync_id, lambda s: s["status"] == "waiting")
    _post(client, sync_id, "resume", token)
    _wait_for(client, sync_id, lambda s: s["is_finished"], timeout=20)

    after = client.post(f"/api/sync/{sync_id}/cancel", data={"token": token})
    assert after.status_code == 409
    assert after.json()["kind"] == "SyncDrillConflict"

    # And while the wait is real, the server itself double-checks the wait
    # event inside the terminating SQL (identity + IPC/SyncRep required).
    assert "wait_event_type = 'IPC'" in mod._TERMINATE_WAITING_COMMIT_SQL


def test_duplicate_and_bad_order_numbers_are_rejected(web):
    client, store, mod = web
    sync_id = _start(client)
    token = _token(client, sync_id)
    _post(client, sync_id, "pause", token)
    _post(client, sync_id, "submit", token, order_no="DEPLOY-DUP-001")
    _wait_for(client, sync_id, lambda s: s["status"] == "waiting")
    _post(client, sync_id, "resume", token)
    _wait_for(client, sync_id, lambda s: s["is_finished"], timeout=20)

    second = _start(client)
    token2 = _token(client, second)
    _post(client, second, "pause", token2)
    dup = client.post(
        f"/api/sync/{second}/submit",
        data={"token": token2, "order_no": "DEPLOY-DUP-001"},
    )
    assert dup.status_code == 409
    bad = client.post(
        f"/api/sync/{second}/submit",
        data={"token": token2, "order_no": "bad order!"},
    )
    assert bad.status_code == 400
    client.post(f"/api/sync/{second}/resume", data={"token": token2})


def test_verdict_survives_web_restart_and_reconciles_live_rows(web, tmp_path, monkeypatch):
    client, store, mod = web
    state_file = os.environ["SYNC_STATE_FILE"]
    sync_id = _start(client)
    token = _token(client, sync_id)
    _post(client, sync_id, "pause", token)
    order = "DEPLOY-RESTART-001"
    _post(client, sync_id, "submit", token, order_no=order)
    _wait_for(client, sync_id, lambda s: s["status"] == "waiting")
    _post(client, sync_id, "cancel", token)
    snap = _state(client, sync_id)
    assert snap["result"]["label"] == "取消等待"

    # Simulate a full web restart: shut the stores down (replay is resumed),
    # drop every module and reload from the persisted state file.
    store.shutdown()
    for name in list(sys.modules):
        if name.startswith("app."):
            del sys.modules[name]
    import app.config
    import app.db
    import app.drill
    import app.sync_drill
    import app.main

    importlib.reload(app.config)
    importlib.reload(app.db)
    importlib.reload(app.drill)
    app.sync_drill = importlib.reload(app.sync_drill)
    app.main = importlib.reload(app.main)
    client2 = TestClient(app.main.app)
    with client2:
        # The persisted round is still listed and keeps its original verdict.
        listing = client2.get("/api/sync/state").json()
        ids = [r["id"] for r in listing["recent"]]
        assert sync_id in ids
        page = client2.get(f"/sync/{sync_id}")
        assert page.status_code == 200
        assert "取消等待" in page.text

        snap2 = client2.get(f"/api/sync/state?sync_id={sync_id}").json()["sync"]
        assert snap2["result"]["outcome"] == "cancelled"
        assert snap2["order_no"] == order
        # Live facts, re-queried against both real databases.
        assert snap2["verification"]["primary"]["found"] is True
        # Shutdown resumed replay; give the standby a moment to catch up.
        deadline = time.time() + 20
        while time.time() < deadline:
            snap2 = client2.get(
                f"/api/sync/state?sync_id={sync_id}"
            ).json()["sync"]
            if snap2["verification"]["standby"].get("found"):
                break
            time.sleep(0.3)
        assert snap2["verification"]["standby"]["found"] is True
        assert snap2["verification"]["standby"]["error"] is None

        # The old page token still drives verification (credential present).
        ok = client2.post(f"/api/sync/{sync_id}/verify", data={"token": token})
        assert ok.status_code == 200
        # A missing/forged credential cannot trigger any action.
        forbidden = client2.post(
            f"/api/sync/{sync_id}/verify", data={"token": "nope"}
        )
        assert forbidden.status_code == 403


def test_query_failures_are_marked_unconfirmable(web, monkeypatch):
    client, store, mod = web
    sync_id = _start(client)
    token = _token(client, sync_id)
    _post(client, sync_id, "pause", token)
    _post(client, sync_id, "submit", token, order_no="DEPLOY-BROKEN-001")
    _wait_for(client, sync_id, lambda s: s["status"] == "waiting")

    # Break the standby connection: the standby item must flag "无法确认".
    def boom(*args, **kwargs):
        raise psycopg.OperationalError("simulated standby outage")

    monkeypatch.setattr(mod, "connect_standby", boom)
    snap = _state(client, sync_id)
    assert snap["verification"]["standby"]["confirmed"] is False
    assert snap["verification"]["standby"]["error"]
    assert "simulated standby outage" in snap["verification"]["standby"]["error"]
    # Primary is still confirmable.
    assert snap["verification"]["primary"]["confirmed"] is True

    monkeypatch.undo()
    _post(client, sync_id, "resume", token)
    _wait_for(client, sync_id, lambda s: s["is_finished"], timeout=20)


def test_row_lock_drill_keeps_working_alongside_sync_wait(web):
    """The original row-lock drill must remain operable while a remote_apply
    commit is stuck waiting."""
    client, store, mod = web
    sync_id = _start(client)
    token = _token(client, sync_id)
    _post(client, sync_id, "pause", token)
    _post(client, sync_id, "submit", token, order_no="DEPLOY-CONCURRENT-001")
    _wait_for(client, sync_id, lambda s: s["status"] == "waiting")

    # Run a full row-lock drill round in parallel.
    res = client.post("/drill/start", follow_redirects=False)
    assert res.status_code == 303
    lock_id = res.headers["location"].rsplit("/", 1)[-1]
    lock_snap = client.get(
        f"/api/state?drill_id={lock_id}"
    ).json()["drill"]
    assert lock_snap["status"] == "blocked"
    lock_token = re.search(
        r'name="token" value="([^"]+)"', client.get(f"/drill/{lock_id}").text
    ).group(1)
    released = client.post(
        f"/api/drills/{lock_id}/release", data={"token": lock_token}
    ).json()
    assert released["release"]["recovered"] is True

    # The sync wait is still there and can still be cancelled.
    snap = _state(client, sync_id)
    assert snap["status"] == "waiting"
    _post(client, sync_id, "cancel", token)
    _post(client, sync_id, "resume", token)
    _wait_for(client, sync_id, lambda s: s["caught_up"] is True, timeout=20)


def test_new_round_resumes_replay_left_paused(web):
    client, store, mod = web
    first = _start(client)
    token = _token(client, first)
    _post(client, first, "pause", token)
    assert _standby("SELECT pg_is_wal_replay_paused()")[0] is True

    second = _start(client)
    assert second != first
    # Starting a new round must not leave replay paused.
    deadline = time.time() + 10
    while time.time() < deadline:
        if not _standby("SELECT pg_is_wal_replay_paused()")[0]:
            break
        time.sleep(0.2)
    assert _standby("SELECT pg_is_wal_replay_paused()")[0] is False
    snap = _state(client, second)
    assert snap["standby_paused"] is False


def test_replication_state_is_reported(web):
    client, store, mod = web
    sync_id = _start(client)
    token = _token(client, sync_id)
    _post(client, sync_id, "pause", token)
    _post(client, sync_id, "submit", token, order_no="DEPLOY-REP-001")
    _wait_for(client, sync_id, lambda s: s["status"] == "waiting")
    snap = _state(client, sync_id)
    rep = snap["verification"]["replication"]
    assert rep is not None
    assert rep["application_name"] == "standby1"
    assert rep["sync_state"] in {"quorum", "sync", "potential"}
    _post(client, sync_id, "resume", token)
    _wait_for(client, sync_id, lambda s: s["is_finished"], timeout=20)


def test_restart_while_waiting_marks_round_unconfirmed(tmp_path, monkeypatch):
    """If the web process dies while a remote_apply commit is waiting, the
    restart must not leave a permanently stuck round: a round persisted in a
    live-commit state is restored as unconfirmed, reconciled by row facts."""
    state_file = tmp_path / "crash_drills.json"
    monkeypatch.setenv("DATABASE_URL", PRIMARY_URL)
    monkeypatch.setenv("STANDBY_URL", STANDBY_URL)
    monkeypatch.setenv("SYNC_STATE_FILE", str(state_file))
    for name in list(sys.modules):
        if name.startswith("app."):
            del sys.modules[name]
    import app.config
    import app.db
    import app.drill
    import app.sync_drill
    import app.main

    importlib.reload(app.config)
    importlib.reload(app.db)
    importlib.reload(app.drill)
    app.sync_drill = importlib.reload(app.sync_drill)
    app.main = importlib.reload(app.main)

    # Persist a round that was mid-wait when the previous process died.
    import app.config
    import app.db
    import app.drill
    import app.sync_drill
    import app.main

    drill = app.sync_drill.SyncDrill("deadbeef" * 4, 1, "old-token")
    drill.order_no = "DEPLOY-CRASH-001"
    drill.status = "waiting"
    drill.waiting_confirmed = True
    drill.commit_pid = 999999
    drill.commit_backend_start = None
    app.sync_drill.sync_store._drills[drill.id] = drill
    app.sync_drill.sync_store._active_id = drill.id
    app.sync_drill.sync_store.persist()
    app.sync_drill.sync_store.shutdown()

    # A fresh process loads the file: live-commit state cannot own a thread.
    for name in list(sys.modules):
        if name.startswith("app."):
            del sys.modules[name]
    import app.config
    import app.db
    import app.drill
    import app.sync_drill
    import app.main
    client = TestClient(app.main.app)
    with client:
        snap = client.get(
            f"/api/sync/state?sync_id={'deadbeef' * 4}"
        ).json()["sync"]
        assert snap["status"] == "error"
        assert snap["result"]["label"] == "重启未确认"
        assert snap["is_finished"] is True
        assert snap["waiting_confirmed"] is True  # observation facts retained
    app.main.sync_store.shutdown()
