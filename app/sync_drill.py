"""Synchronous-commit (remote_apply) drill orchestration.

Scenario
--------
The drill drives one real PostgreSQL primary and one real physical standby.

1. The operator pauses WAL replay on the standby
   (``pg_wal_replay_pause()``) and waits until the pause is confirmed.
2. A deployment order number is submitted; a dedicated session sets
   ``synchronous_commit = remote_apply`` and runs an INSERT + COMMIT.
   Because the standby cannot replay while paused, the COMMIT blocks in
   synchronous-replication wait (``wait_event = SyncRep``).
3. The cancel button is only enabled once PostgreSQL's own
   ``pg_stat_activity`` confirms that exact session is waiting for
   synchronous replication. Cancelling terminates that single session
   (identity triple checked in one SQL statement); the commit record was
   already durably written on the primary, so the row stays there, while the
   paused standby still cannot see it.
4. Replay is resumed; once the standby catches up, both sides show the same
   order number, while the original verdict ("取消等待") is preserved.
5. A second order submitted with replay running completes normally and is
   labelled "已同步确认".

Every drill round is persisted to a JSON state file so that after a web
restart the original verdict and both databases' live facts can still be
reconciled by order number.
"""

from __future__ import annotations

import datetime as dt
import decimal
import hmac
import json
import logging
import os
import re
import secrets
import threading
import time
import uuid
from collections import OrderedDict
from typing import Any

import psycopg

from .config import settings
from .db import connect, connect_standby

logger = logging.getLogger("lockdrill.sync")

HISTORY_LIMIT = 50
PERSIST_LIMIT = 200

# PostgreSQL reports a remote_apply commit wait as IPC/SyncRep; some newer
# code paths use RepOpSync. Accept both, plus nothing else.
SYNC_WAIT_EVENTS = {"SyncRep", "RepOpSync"}

ORDER_NO_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")

# States in which the standby replay is (or should be) held paused.
PAUSED_STATES = {"pausing", "paused", "submitting", "waiting", "cancelling"}
# States in which a commit thread may still be running.
LIVE_COMMIT_STATES = {"submitting", "waiting", "cancelling"}


class SyncDrillError(Exception):
    status_code = 400


class SyncDrillNotFound(SyncDrillError):
    status_code = 410


class SyncDrillTokenMismatch(SyncDrillError):
    status_code = 403


class SyncDrillConflict(SyncDrillError):
    status_code = 409


class StandbyUnavailable(SyncDrillError):
    status_code = 503


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ", timespec="microseconds")
    return str(value)


def _app_name(drill_id: str) -> str:
    # PostgreSQL truncates application_name to 63 bytes (NAMEDATALEN - 1).
    return f"{settings.session_prefix}:sync:commit:{drill_id}"[:63]


# Terminate the committing session only if that exact session
# (pid + backend_start + application_name) is still alive and still waiting
# for synchronous replication. A reused pid can never match.
_TERMINATE_WAITING_COMMIT_SQL = """
WITH target AS (
    SELECT pid
    FROM pg_stat_activity
    WHERE pid = %(pid)s
      AND backend_start = %(backend_start)s
      AND application_name = %(app_name)s
      AND state = 'active'
      AND wait_event_type = 'IPC'
      AND wait_event = ANY (%(wait_events)s)
)
SELECT pid, pg_terminate_backend(pid) AS terminated FROM target
"""


class SyncDrill:
    """One synchronous-commit drill round."""

    def __init__(self, drill_id: str, round_no: int, token: str) -> None:
        self.id = drill_id
        self.round = round_no
        self.token = token
        self.app_name = _app_name(drill_id)
        self.created_at = utcnow()

        self.order_no: str | None = None
        self.note = ""
        self.status = "ready"
        self.error: str | None = None
        self.ended_reason: str | None = None
        self.ended_at: dt.datetime | None = None

        # Replay / replication facts.
        self.standby_paused = False
        self.replication: dict[str, Any] | None = None

        # Committing session identity.
        self.commit_pid: int | None = None
        self.commit_backend_start: dt.datetime | None = None
        self.commit_thread: threading.Thread | None = None
        self.commit_ready = threading.Event()
        self.commit_done = threading.Event()
        self.commit_outcome: dict[str, Any] | None = None
        self.commit_setup_error: str | None = None
        self.waiting_confirmed = False
        self.waiting_first_seen: dt.datetime | None = None

        # Final verdict: {"outcome": "cancelled|synced|error", "label": ...}
        self.result: dict[str, Any] | None = None
        self.caught_up = False
        self.caught_up_at: dt.datetime | None = None
        self.last_verification: dict[str, Any] | None = None

        self.lock = threading.Lock()

    def age_seconds(self) -> float:
        return (utcnow() - self.created_at).total_seconds()

    def is_finished(self) -> bool:
        return self.status in {"cancelled", "synced", "error", "expired", "ended"}

    # -- (de)serialisation -----------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        with self.lock:
            return {
                "id": self.id,
                "round": self.round,
                "token": self.token,
                "app_name": self.app_name,
                "created_at": iso(self.created_at),
                "order_no": self.order_no,
                "note": self.note,
                "status": self.status,
                "error": self.error,
                "ended_reason": self.ended_reason,
                "ended_at": iso(self.ended_at),
                "standby_paused": self.standby_paused,
                "replication": self.replication,
                "commit_pid": self.commit_pid,
                "commit_backend_start": iso(self.commit_backend_start),
                "waiting_confirmed": self.waiting_confirmed,
                "waiting_first_seen": iso(self.waiting_first_seen),
                "commit_outcome": self.commit_outcome,
                "result": self.result,
                "caught_up": self.caught_up,
                "caught_up_at": iso(self.caught_up_at),
                "last_verification": self.last_verification,
            }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SyncDrill":
        drill = cls(data["id"], int(data["round"]), data["token"])
        drill.app_name = data.get("app_name") or _app_name(drill.id)
        drill.created_at = _parse_dt(data["created_at"]) or drill.created_at
        drill.order_no = data.get("order_no")
        drill.note = data.get("note", "") or ""
        drill.status = data.get("status", "ready")
        drill.error = data.get("error")
        drill.ended_reason = data.get("ended_reason")
        drill.ended_at = _parse_dt(data.get("ended_at"))
        drill.standby_paused = bool(data.get("standby_paused"))
        drill.replication = data.get("replication")
        drill.commit_pid = data.get("commit_pid")
        drill.commit_backend_start = _parse_dt(data.get("commit_backend_start"))
        drill.waiting_confirmed = bool(data.get("waiting_confirmed"))
        drill.waiting_first_seen = _parse_dt(data.get("waiting_first_seen"))
        drill.commit_outcome = data.get("commit_outcome")
        drill.result = data.get("result")
        drill.caught_up = bool(data.get("caught_up"))
        drill.caught_up_at = _parse_dt(data.get("caught_up_at"))
        drill.last_verification = data.get("last_verification")
        # A restored drill owns no live thread in this process. If it was in a
        # live commit state when the previous process went away, its orphaned
        # session was terminated at startup; its facts can only be reconciled
        # via the verification table, so mark it unconfirmed rather than stuck.
        if drill.status in {"submitting", "waiting", "cancelling"}:
            drill.status = "error"
            drill.error = (
                "服务重启时该提交会话仍在等待，已按启动清理终止；"
                "请按下方两库核对结果确认单号实况。"
            )
            drill.ended_reason = "服务重启，等待中的会话已终止"
            drill.ended_at = utcnow()
            if drill.result is None:
                drill.result = {
                    "outcome": "error",
                    "label": "重启未确认",
                    "terminated": False,
                    "terminate_error": None,
                    "detail": drill.error,
                    "commit_outcome": drill.commit_outcome,
                    "finished_at": iso(utcnow()),
                }
        # A restored drill owns no live thread in this process.
        drill.commit_done.set()
        return drill


def _parse_dt(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


def _json_default(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return int(value)
    if isinstance(value, dt.datetime):
        return iso(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


class SyncStore:
    """Process-local registry of sync-commit drills, persisted to JSON."""

    def __init__(self, state_file: str | None = None) -> None:
        self._lock = threading.RLock()
        self._drills: "OrderedDict[str, SyncDrill]" = OrderedDict()
        self._active_id: str | None = None
        self._round = 0
        self._stopping = False
        self._state_file = state_file or os.environ.get(
            "SYNC_STATE_FILE", os.path.join("var", "sync_drills.json")
        )
        self.persistence_error: str | None = None
        self._load()

    # -- persistence -----------------------------------------------------
    def _load(self) -> None:
        try:
            with open(self._state_file, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            return
        except Exception as exc:  # corrupt file must not break startup
            self.persistence_error = f"历史状态读取失败：{type(exc).__name__}: {exc}"
            logger.exception("could not load sync drill state")
            return
        try:
            for item in payload.get("drills", [])[-PERSIST_LIMIT:]:
                drill = SyncDrill.from_dict(item)
                self._drills[drill.id] = drill
                if not drill.is_finished():
                    self._active_id = drill.id
            self._round = int(payload.get("round", len(self._drills)))
        except Exception as exc:
            self.persistence_error = f"历史状态解析失败：{type(exc).__name__}: {exc}"
            logger.exception("could not parse sync drill state")

    def _persist_locked(self) -> None:
        items = list(self._drills.values())[-PERSIST_LIMIT:]
        payload = {
            "round": self._round,
            "saved_at": iso(utcnow()),
            "drills": [d.to_dict() for d in items],
        }
        tmp = f"{self._state_file}.tmp"
        os.makedirs(os.path.dirname(os.path.abspath(self._state_file)), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1, default=_json_default)
        os.replace(tmp, self._state_file)

    def persist(self) -> None:
        with self._lock:
            try:
                self._persist_locked()
            except Exception as exc:
                self.persistence_error = (
                    f"状态落盘失败：{type(exc).__name__}: {exc}"
                )
                logger.exception("could not persist sync drill state")

    # -- registry --------------------------------------------------------
    def get(self, drill_id: str) -> SyncDrill | None:
        with self._lock:
            return self._drills.get(drill_id)

    def require(self, drill_id: str) -> SyncDrill:
        drill = self.get(drill_id)
        if drill is None:
            raise SyncDrillNotFound(
                "该同步提交演练记录已不存在（服务可能已重启或记录已清理），未执行任何操作。"
            )
        return drill

    def active(self) -> SyncDrill | None:
        with self._lock:
            if self._active_id is None:
                return None
            return self._drills.get(self._active_id)

    def recent(self, limit: int = 10) -> list[SyncDrill]:
        with self._lock:
            items = list(self._drills.values())
        return items[-limit:][::-1]

    def _check_token(self, drill: SyncDrill, token: str) -> None:
        if not token or not hmac.compare_digest(token, drill.token):
            raise SyncDrillTokenMismatch(
                "缺少有效的确认凭据（页面令牌缺失或不匹配，可能来自已失效的旧页面），"
                "未执行任何操作。"
            )

    # -- round lifecycle -------------------------------------------------
    def start_round(self) -> SyncDrill:
        with self._lock:
            if self._stopping:
                raise SyncDrillConflict("服务正在关闭，无法开始新的演练。")
            previous = self.active()
        # Network I/O must not happen while holding the registry lock.
        if previous is not None and not previous.is_finished():
            # A new round must not leave the standby replay paused.
            if previous.status in PAUSED_STATES:
                self._resume_replay(previous)
            with previous.lock:
                previous.status = "ended"
                previous.ended_reason = "开始新一轮演练，上一轮已结束"
                previous.ended_at = utcnow()
        with self._lock:
            if self._stopping:
                raise SyncDrillConflict("服务正在关闭，无法开始新的演练。")
            self._round += 1
            round_no = self._round
            drill = SyncDrill(uuid.uuid4().hex, round_no, secrets.token_urlsafe(24))
            while len(self._drills) >= HISTORY_LIMIT:
                self._drills.popitem(last=False)
            self._drills[drill.id] = drill
            self._active_id = drill.id
        self.persist()
        return drill

    # -- standby replay control ------------------------------------------
    def _standby_admin(self):
        return connect_standby(
            application_name=f"{settings.session_prefix}:sync:admin",
            autocommit=True,
        )

    def pause_replay(self, drill_id: str, token: str) -> None:
        drill = self.require(drill_id)
        self._check_token(drill, token)
        with drill.lock:
            if drill.status in PAUSED_STATES:
                raise SyncDrillConflict("备库 WAL 回放已处于暂停（或暂停流程进行中）。")
            if not settings.standby_url:
                raise StandbyUnavailable("未配置备库连接（STANDBY_URL），无法暂停回放。")
            drill.status = "pausing"
            drill.error = None
        try:
            with self._standby_admin() as conn:
                conn.execute("SELECT pg_wal_replay_pause()")
                deadline = time.monotonic() + settings.sync_wait_confirm_seconds
                paused = False
                while time.monotonic() < deadline:
                    row = conn.execute(
                        "SELECT pg_is_wal_replay_paused() AS paused"
                    ).fetchone()
                    paused = bool(row and row["paused"])
                    if paused:
                        break
                    time.sleep(0.1)
        except psycopg.Error as exc:
            with drill.lock:
                drill.status = "ready"
                drill.error = f"连接或操作备库失败：{type(exc).__name__}: {exc}"
            self.persist()
            raise StandbyUnavailable(drill.error) from exc
        if not paused:
            with drill.lock:
                drill.status = "ready"
                drill.error = "已调用暂停，但在限定时间内未观察到回放暂停生效。"
            self.persist()
            raise SyncDrillError(drill.error)
        with drill.lock:
            drill.standby_paused = True
            drill.status = "paused"
        self.persist()

    def resume_replay(self, drill_id: str, token: str) -> None:
        drill = self.require(drill_id)
        self._check_token(drill, token)
        self._resume_replay(drill)
        # Best-effort bounded wait for this round's order to reach the standby,
        # so a resumed page can immediately show the reconciled facts.
        if drill.order_no:
            deadline = time.monotonic() + settings.sync_catchup_seconds
            while time.monotonic() < deadline and not drill.caught_up:
                verification = self._verify_unlocked(drill)
                if (
                    verification["primary"].get("found")
                    and verification["standby"].get("found")
                    and not verification["standby"].get("error")
                ):
                    with drill.lock:
                        drill.caught_up = True
                        drill.caught_up_at = utcnow()
                    break
                time.sleep(0.2)
        self.persist()

    def _resume_replay(self, drill: SyncDrill) -> None:
        if not settings.standby_url:
            with drill.lock:
                drill.standby_paused = False
            return
        try:
            with self._standby_admin() as conn:
                row = conn.execute(
                    "SELECT pg_is_wal_replay_paused() AS paused"
                ).fetchone()
                if row and row["paused"]:
                    conn.execute("SELECT pg_wal_replay_resume()")
                    deadline = time.monotonic() + settings.sync_wait_confirm_seconds
                    while time.monotonic() < deadline:
                        now = conn.execute(
                            "SELECT pg_is_wal_replay_paused() AS paused"
                        ).fetchone()
                        if not (now and now["paused"]):
                            break
                        time.sleep(0.1)
        except Exception as exc:
            logger.exception("could not resume standby replay")
            with drill.lock:
                drill.error = (
                    (drill.error + " " if drill.error else "")
                    + f"恢复备库回放失败：{type(exc).__name__}: {exc}"
                )
        with drill.lock:
            try:
                with self._standby_admin() as conn:
                    row = conn.execute(
                        "SELECT pg_is_wal_replay_paused() AS paused"
                    ).fetchone()
                    drill.standby_paused = bool(row and row["paused"])
            except Exception:
                # Keep the last known value; snapshot observation will flag it.
                pass

    # -- remote_apply commit ---------------------------------------------
    def submit_order(self, drill_id: str, token: str, order_no: str, note: str) -> None:
        drill = self.require(drill_id)
        self._check_token(drill, token)
        if not settings.standby_url:
            raise StandbyUnavailable(
                "未配置备库连接（STANDBY_URL），无法执行 remote_apply 同步提交演练。"
            )
        order_no = (order_no or "").strip()
        if not ORDER_NO_RE.match(order_no):
            raise SyncDrillError("部署单号需为 1-64 位字母、数字、下划线或短横线。")
        if self._order_exists(drill_id, order_no):
            raise SyncDrillConflict(f"部署单号 {order_no} 已存在，请换一个单号。")
        with drill.lock:
            if drill.status != "paused":
                raise SyncDrillConflict(
                    "请先暂停备库 WAL 回放并确认生效，再提交部署单"
                    f"（当前状态：{drill.status}）。"
                )
            drill.order_no = order_no
            drill.note = (note or "").strip()[:200]
            drill.status = "submitting"
            drill.error = None
            drill.waiting_confirmed = False
            drill.waiting_first_seen = None
            drill.commit_outcome = None
            drill.result = None
            drill.caught_up = False
            drill.caught_up_at = None
            drill.commit_ready = threading.Event()
            drill.commit_done = threading.Event()

        thread = threading.Thread(
            target=_commit_main,
            args=(self, drill),
            name=f"sync-commit-{drill.id[:8]}",
            daemon=True,
        )
        with drill.lock:
            drill.commit_thread = thread
        thread.start()
        if not drill.commit_ready.wait(timeout=settings.sync_wait_confirm_seconds):
            with drill.lock:
                drill.status = "error"
                drill.error = "提交会话未能在限定时间内建立。"
                drill.result = {
                    "outcome": "error",
                    "label": "提交未确认",
                    "terminated": False,
                    "terminate_error": None,
                    "detail": drill.error,
                    "commit_outcome": None,
                    "finished_at": iso(utcnow()),
                }
            self.persist()
            raise SyncDrillError(drill.error)
        if drill.commit_setup_error:
            with drill.lock:
                drill.status = "error"
                drill.error = f"提交会话建立失败：{drill.commit_setup_error}"
                drill.result = {
                    "outcome": "error",
                    "label": "提交失败",
                    "terminated": False,
                    "terminate_error": None,
                    "detail": drill.error,
                    "commit_outcome": drill.commit_outcome,
                    "finished_at": iso(utcnow()),
                }
            self.persist()
            raise SyncDrillError(drill.error)
        self.persist()

    def _order_exists(self, drill_id: str, order_no: str) -> bool:
        with self._lock:
            known = [
                other.order_no
                for other in self._drills.values()
                if other.id != drill_id and other.result
            ]
        if order_no in known:
            return True
        try:
            with connect(
                application_name=f"{settings.session_prefix}:sync:check",
                autocommit=True,
            ) as conn:
                row = conn.execute(
                    "SELECT 1 FROM deploy_order WHERE order_no = %s", (order_no,)
                ).fetchone()
                return row is not None
        except Exception:
            return False  # let the INSERT surface the real database error

    def cancel_wait(self, drill_id: str, token: str) -> dict[str, Any]:
        drill = self.require(drill_id)
        self._check_token(drill, token)
        with drill.lock:
            # Idempotent: repeat submissions on an already-cancelled round
            # return the original verdict without touching any session.
            if drill.result is not None and drill.result.get("outcome") == "cancelled":
                return drill.result
            if drill.status != "waiting" or not drill.waiting_confirmed:
                raise SyncDrillConflict(
                    "只有在数据库确认该事务正在等待同步复制时才能取消等待。"
                )
            if drill.commit_pid is None or drill.commit_backend_start is None:
                raise SyncDrillConflict("提交会话身份缺失，无法安全取消。")
            drill.status = "cancelling"
            params = {
                "pid": drill.commit_pid,
                "backend_start": drill.commit_backend_start,
                "app_name": drill.app_name,
                "wait_events": list(SYNC_WAIT_EVENTS),
            }

        terminated = False
        terminate_error: str | None = None
        try:
            with connect(
                application_name=f"{settings.session_prefix}:sync:cancel",
                autocommit=True,
            ) as admin:
                # Re-observe immediately before terminating.
                live = self._read_commit_session(admin, drill)
                if not self._is_waiting(live):
                    raise SyncDrillConflict(
                        "提交会话已不再处于同步复制等待状态，取消操作已拒绝。"
                    )
                row = admin.execute(_TERMINATE_WAITING_COMMIT_SQL, params).fetchone()
                terminated = bool(row and row["terminated"])
        except SyncDrillError:
            with drill.lock:
                drill.status = "waiting"
            self.persist()
            raise
        except Exception as exc:
            terminate_error = f"{type(exc).__name__}: {exc}"

        thread = drill.commit_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=settings.release_wait_seconds)
        drill.commit_done.wait(timeout=settings.release_wait_seconds)

        # The commit record is already durable on the primary: verify the row
        # really landed there, and that the paused standby still lacks it.
        verification = self.verify_order(drill_id, token, persist=False)
        primary_found = bool(verification["primary"].get("found"))
        standby_found = bool(verification["standby"].get("found"))

        outcome = drill.commit_outcome or {}
        if terminated and primary_found and not standby_found:
            result = {
                "outcome": "cancelled",
                "label": "取消等待",
                "terminated": True,
                "terminate_error": None,
                "detail": (
                    "已终止等待同步复制的提交会话（PID/会话启动时间/应用名三重校验通过）。"
                    "主库可查到该单号，备库因 WAL 回放暂停仍查不到。"
                ),
                "commit_outcome": outcome,
                "finished_at": iso(utcnow()),
            }
            new_status = "cancelled"
        else:
            reasons = []
            if not terminated:
                reasons.append(
                    f"原子校验未通过或终止失败{('：' + terminate_error) if terminate_error else '，会话未被终止'}"
                )
            if not primary_found:
                reasons.append("主库未查到该单号，无法确认提交已在主库落盘")
            if standby_found:
                reasons.append("备库已可查到该单号，暂停状态可能已失效")
            result = {
                "outcome": "error",
                "label": "取消未确认",
                "terminated": terminated,
                "terminate_error": terminate_error,
                "detail": "取消等待的预期结果未能全部确认：" + "；".join(reasons) + "。",
                "commit_outcome": outcome,
                "finished_at": iso(utcnow()),
            }
            new_status = "error"
        with drill.lock:
            drill.result = result
            drill.status = new_status
        self.persist()
        return result

    # -- observation -----------------------------------------------------
    @staticmethod
    def _is_waiting(row: dict[str, Any] | None) -> bool:
        return bool(
            row
            and row.get("state") == "active"
            and row.get("wait_event_type") == "IPC"
            and row.get("wait_event") in SYNC_WAIT_EVENTS
        )

    def _read_commit_session(self, conn, drill: SyncDrill) -> dict[str, Any] | None:
        if drill.commit_pid is None:
            return None
        row = conn.execute(
            """
            SELECT pid, backend_start, state, wait_event_type, wait_event,
                   query, application_name, xact_start, query_start
            FROM pg_stat_activity
            WHERE pid = %s
            """,
            (drill.commit_pid,),
        ).fetchone()
        if row is None:
            return None
        if row["application_name"] != drill.app_name:
            return None  # pid reused by an unrelated session
        if (
            drill.commit_backend_start is not None
            and row["backend_start"] != drill.commit_backend_start
        ):
            return None
        return dict(row)

    def _refresh_commit_state(self, drill: SyncDrill) -> dict[str, Any] | None:
        """Observe the committing session and advance waiting/synced state."""
        if drill.commit_pid is None:
            return None
        try:
            with connect(
                application_name=f"{settings.session_prefix}:sync:observe",
                autocommit=True,
            ) as conn:
                row = self._read_commit_session(conn, drill)
        except Exception as exc:
            with drill.lock:
                drill.error = f"读取主库会话状态失败：{type(exc).__name__}: {exc}"
            return None

        with drill.lock:
            if self._is_waiting(row):
                if not drill.waiting_confirmed:
                    drill.waiting_confirmed = True
                    drill.waiting_first_seen = utcnow()
                if drill.status == "submitting":
                    drill.status = "waiting"
            elif row is None and drill.status in {"submitting", "waiting"}:
                # Session ended on its own (normal commit or an outside
                # failure). The "cancelling" state is owned by cancel_wait().
                if drill.commit_done.is_set() and drill.commit_outcome is not None:
                    self._finalise_from_outcome(drill)
        return row

    def _finalise_from_outcome(self, drill: SyncDrill) -> None:
        if drill.result is not None:
            return
        outcome = drill.commit_outcome or {}
        status = outcome.get("status")
        if status == "committed":
            # A normal remote_apply commit means the standby replayed it.
            verification = self._verify_unlocked(drill)
            synced = bool(
                verification["primary"].get("found")
                and verification["standby"].get("found")
                and not verification["standby"].get("error")
            )
            drill.result = {
                "outcome": "synced" if synced else "error",
                "label": "已同步确认" if synced else "同步状态未确认",
                "terminated": False,
                "terminate_error": None,
                "detail": (
                    "remote_apply 提交正常返回，主库与备库均可查到该单号。"
                    if synced
                    else "提交已返回，但两库实况未能全部确认，请按下方核对结果检查。"
                ),
                "commit_outcome": outcome,
                "finished_at": iso(utcnow()),
            }
            drill.status = "synced" if synced else "error"
            drill.caught_up = synced
            if synced:
                drill.caught_up_at = utcnow()
        elif status and drill.result is None:
            drill.result = {
                "outcome": "error",
                "label": "提交失败",
                "terminated": False,
                "terminate_error": None,
                "detail": f"提交会话异常结束：{outcome.get('error') or status}",
                "commit_outcome": outcome,
                "finished_at": iso(utcnow()),
            }
            drill.status = "error"

    def poll(self, drill: SyncDrill) -> None:
        """Advance state from live database facts (called by snapshots)."""
        with drill.lock:
            status = drill.status
            pid = drill.commit_pid
            finished = drill.result is not None
        if pid is not None and not finished and status in LIVE_COMMIT_STATES:
            self._refresh_commit_state(drill)
        # A thread that finished while nobody was looking.
        with drill.lock:
            done = drill.commit_done.is_set()
            outcome = drill.commit_outcome
            current = drill.status
        if done and outcome is not None and current in {"submitting", "waiting"}:
            with drill.lock:
                self._finalise_from_outcome(drill)
        # Catch-up observation after a cancelled round is resumed. A cancelled
        # round is a finished round, so this check runs independently of the
        # live-commit branch above.
        with drill.lock:
            cancelled = drill.status == "cancelled"
            caught_up = drill.caught_up
        if cancelled and not caught_up:
            verification = self._verify_unlocked(drill)
            if (
                verification["primary"].get("found")
                and verification["standby"].get("found")
                and not verification["standby"].get("error")
            ):
                with drill.lock:
                    drill.caught_up = True
                    drill.caught_up_at = utcnow()
        self.persist()

    def verify_order(self, drill_id: str, token: str | None, persist: bool = True) -> dict[str, Any]:
        drill = self.require(drill_id)
        if token is not None:
            self._check_token(drill, token)
        verification = self._verify_unlocked(drill)
        with drill.lock:
            drill.last_verification = verification
        if persist:
            self.persist()
        return verification

    def _verify_unlocked(self, drill: SyncDrill) -> dict[str, Any]:
        order_no = drill.order_no
        verification: dict[str, Any] = {
            "order_no": order_no,
            "checked_at": iso(utcnow()),
            "primary": {"confirmed": False, "found": False, "row": None, "error": None},
            "standby": {"confirmed": False, "found": False, "row": None, "error": None},
            "replication": None,
            "replay_paused": None,
        }
        if not order_no:
            verification["primary"]["error"] = "本轮尚未提交部署单号"
            verification["standby"]["error"] = "本轮尚未提交部署单号"
            return verification

        # Primary.
        try:
            with connect(
                application_name=f"{settings.session_prefix}:sync:verify-p",
                autocommit=True,
            ) as conn:
                row = conn.execute(
                    "SELECT order_no, note, created_at, committed_at "
                    "FROM deploy_order WHERE order_no = %s",
                    (order_no,),
                ).fetchone()
                rep = conn.execute(
                    """
                    SELECT application_name, state, sync_state,
                           write_lsn::text AS write_lsn, flush_lsn::text AS flush_lsn,
                           replay_lsn::text AS replay_lsn,
                           pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint
                               AS replay_lag_bytes
                    FROM pg_stat_replication
                    WHERE application_name = %s
                    """,
                    (settings.replication_app_name,),
                ).fetchone()
            verification["primary"]["confirmed"] = True
            verification["primary"]["found"] = row is not None
            if row is not None:
                verification["primary"]["row"] = {
                    "order_no": row["order_no"],
                    "note": row["note"],
                    "created_at": iso(row["created_at"]),
                    "committed_at": iso(row["committed_at"]),
                }
            verification["replication"] = (
                {k: (int(v) if isinstance(v, decimal.Decimal) else v)
                 for k, v in dict(rep).items()}
                if rep
                else None
            )
        except Exception as exc:
            verification["primary"]["error"] = f"{type(exc).__name__}: {exc}"

        # Standby.
        if not settings.standby_url:
            verification["standby"]["error"] = "未配置备库连接（STANDBY_URL），无法确认"
        else:
            try:
                with connect_standby(
                    application_name=f"{settings.session_prefix}:sync:verify-s",
                    autocommit=True,
                ) as conn:
                    paused = conn.execute(
                        "SELECT pg_is_wal_replay_paused() AS paused"
                    ).fetchone()
                    verification["replay_paused"] = (
                        bool(paused and paused["paused"]) if paused else None
                    )
                    row = conn.execute(
                        "SELECT order_no, note, created_at, committed_at "
                        "FROM deploy_order WHERE order_no = %s",
                        (order_no,),
                    ).fetchone()
                verification["standby"]["confirmed"] = True
                verification["standby"]["found"] = row is not None
                if row is not None:
                    verification["standby"]["row"] = {
                        "order_no": row["order_no"],
                        "note": row["note"],
                        "created_at": iso(row["created_at"]),
                        "committed_at": iso(row["committed_at"]),
                    }
            except Exception as exc:
                verification["standby"]["error"] = f"{type(exc).__name__}: {exc}"
        return verification

    def snapshot(self, drill: SyncDrill) -> dict[str, Any]:
        self.poll(drill)
        with drill.lock:
            status = drill.status
            pid = drill.commit_pid
        session_live = False
        commit_session = None
        if pid is not None and status in LIVE_COMMIT_STATES:
            try:
                with connect(
                    application_name=f"{settings.session_prefix}:sync:snap",
                    autocommit=True,
                ) as conn:
                    commit_session = self._read_commit_session(conn, drill)
                    session_live = commit_session is not None
            except Exception:
                commit_session = None
        now_waiting = self._is_waiting(commit_session)
        if now_waiting:
            with drill.lock:
                if not drill.waiting_confirmed:
                    drill.waiting_confirmed = True
                    drill.waiting_first_seen = utcnow()
                if drill.status == "submitting":
                    drill.status = "waiting"
                    status = "waiting"
        with drill.lock:
            snap = {
                "id": drill.id,
                "round": drill.round,
                "status": drill.status,
                "created_at": iso(drill.created_at),
                "order_no": drill.order_no,
                "note": drill.note,
                "error": drill.error,
                "ended_reason": drill.ended_reason,
                "ended_at": iso(drill.ended_at),
                "standby_configured": bool(settings.standby_url),
                "standby_paused": drill.standby_paused,
                "waiting_confirmed": drill.waiting_confirmed,
                "waiting_first_seen": iso(drill.waiting_first_seen),
                "commit_pid": drill.commit_pid,
                "commit_backend_start": iso(drill.commit_backend_start),
                "commit_session": _session_payload(commit_session),
                "session_live": session_live,
                "result": drill.result,
                "caught_up": drill.caught_up,
                "caught_up_at": iso(drill.caught_up_at),
                "last_verification": drill.last_verification,
                "persistence_error": self.persistence_error,
                "can_pause": drill.status not in PAUSED_STATES
                and not drill.is_finished()
                and bool(settings.standby_url),
                "can_resume": drill.standby_paused,
                "can_submit": drill.status == "paused"
                and bool(settings.standby_url),
                "can_cancel": drill.status == "waiting"
                and drill.waiting_confirmed
                and now_waiting,
                "is_finished": drill.is_finished(),
            }
        # Always include a fresh verification when an order exists.
        if drill.order_no:
            snap["verification"] = self._verify_unlocked(drill)
            with drill.lock:
                drill.last_verification = snap["verification"]
            self.persist()
        else:
            snap["verification"] = None
        return snap

    # -- housekeeping ----------------------------------------------------
    def expire_stale(self) -> None:
        with self._lock:
            drills = list(self._drills.values())
        for drill in drills:
            if not drill.is_finished() and drill.age_seconds() > settings.sync_drill_ttl_seconds:
                if drill.status in PAUSED_STATES:
                    self._resume_replay(drill)
                with drill.lock:
                    drill.status = "expired"
                    drill.ended_reason = "演练超过保留时长，已自动过期并恢复回放"
                    drill.ended_at = utcnow()
        self.persist()

    def shutdown(self) -> None:
        with self._lock:
            self._stopping = True
            drills = list(self._drills.values())
        # Never leave the physical standby with replay paused after we exit.
        for drill in drills:
            if drill.standby_paused or drill.status in PAUSED_STATES:
                try:
                    self._resume_replay(drill)
                except Exception:
                    logger.exception("error resuming replay during shutdown")
        self.persist()


def _session_payload(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    data = dict(row)
    for key in ("backend_start", "xact_start", "query_start"):
        data[key] = iso(data.get(key))
    return data


# ---------------------------------------------------------------------------
# Committing session (thread)
# ---------------------------------------------------------------------------


def _commit_main(store: SyncStore, drill: SyncDrill) -> None:
    started = time.monotonic()
    conn: psycopg.Connection[Any] | None = None
    outcome: dict[str, Any]
    try:
        conn = connect(application_name=drill.app_name, autocommit=False)
        identity = conn.execute(
            "SELECT pg_backend_pid() AS pid, backend_start "
            "FROM pg_stat_activity WHERE pid = pg_backend_pid()"
        ).fetchone()
        with drill.lock:
            drill.commit_pid = identity["pid"]
            drill.commit_backend_start = identity["backend_start"]
        drill.commit_ready.set()

        # remote_apply: COMMIT blocks until the standby has replayed the
        # transaction. With replay paused on the standby, this is the wait the
        # operator is allowed to cancel.
        conn.execute("SET synchronous_commit TO remote_apply")
        conn.execute("BEGIN")
        try:
            conn.execute(
                "INSERT INTO deploy_order (order_no, note, committed_at) "
                "VALUES (%s, %s, now())",
                (drill.order_no, drill.note),
            )
        except psycopg.Error:
            conn.rollback()
            raise
        conn.execute("COMMIT")
        outcome = {
            "status": "committed",
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "finished_at": iso(utcnow()),
        }
    except BaseException as exc:  # includes administrator command termination
        outcome = {
            "status": "terminated" if _is_termination(exc) else "error",
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "finished_at": iso(utcnow()),
        }
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        if conn is None:
            drill.commit_setup_error = outcome["error"]
    finally:
        with drill.lock:
            drill.commit_outcome = outcome
        drill.commit_ready.set()
        drill.commit_done.set()
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        store.persist()


def _is_termination(exc: BaseException) -> bool:
    text = str(exc)
    return (
        isinstance(exc, psycopg.OperationalError)
        and ("terminating connection" in text or "server closed the connection" in text)
    )


# Module-level singleton.
sync_store = SyncStore()
