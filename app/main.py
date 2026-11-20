"""FastAPI application for the PostgreSQL drills."""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import settings
from .db import connect, ensure_schema
from .drill import DrillError, store
from .sync_drill import (
    StandbyUnavailable,
    SyncDrillConflict,
    SyncDrillError,
    SyncDrillNotFound,
    SyncDrillTokenMismatch,
    sync_store,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("lockdrill.web")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # A re-entered lifespan (e.g. nested TestClient contexts in the same
    # process) must not kill sessions owned by stores still alive here.
    fresh_start = not getattr(lifespan, "_entered", False)
    lifespan._entered = True
    for attempt in range(1, 31):
        try:
            ensure_schema()
            logger.info("drill schema ready")
            break
        except Exception as exc:  # database may still be starting
            logger.warning("database not ready (attempt %s): %s", attempt, exc)
            if attempt == 30:
                raise
            time.sleep(1)
    if fresh_start:
        # Any row-lock drill session naming this app instance is a leftover
        # from a previous process. The pattern is anchored at ":drill:" so
        # synchronous commit sessions (":sync:commit:") are never touched.
        try:
            with connect(application_name="lockdrill:startup", autocommit=True) as conn:
                rows = conn.execute(
                    """
                    SELECT pid FROM pg_stat_activity
                    WHERE application_name LIKE %s
                      AND pid <> pg_backend_pid()
                    """,
                    (f"{settings.session_prefix}:drill:%",),
                ).fetchall()
                for row in rows:
                    conn.execute("SELECT pg_terminate_backend(%s)", (row["pid"],))
                if rows:
                    logger.info("terminated %s orphaned drill session(s)", len(rows))
        except Exception:
            logger.exception("could not clean up orphaned drill sessions")
        # Sync-commit sessions from a previous process must be terminated too,
        # so a remote_apply commit never keeps waiting with nobody watching it.
        try:
            with connect(application_name="lockdrill:startup", autocommit=True) as conn:
                rows = conn.execute(
                    """
                    SELECT pid FROM pg_stat_activity
                    WHERE application_name LIKE %s
                      AND pid <> pg_backend_pid()
                    """,
                    (f"{settings.session_prefix}:sync:commit:%",),
                ).fetchall()
                for row in rows:
                    conn.execute("SELECT pg_terminate_backend(%s)", (row["pid"],))
                if rows:
                    logger.info(
                        "terminated %s orphaned sync-commit session(s)", len(rows)
                    )
        except Exception:
            logger.exception("could not clean up orphaned sync-commit sessions")
        # Make sure a crash never leaves the standby replay paused.
        try:
            for drill in list(sync_store._drills.values()):  # noqa: SLF001
                if drill.standby_paused:
                    sync_store._resume_replay(drill)  # noqa: SLF001
                    with drill.lock:
                        drill.ended_reason = (
                            (drill.ended_reason + "；" if drill.ended_reason else "")
                            + "服务重启时自动恢复备库回放"
                        )
            sync_store.persist()
        except Exception:
            logger.exception("could not resume standby replay on startup")
    try:
        yield
    finally:
        logger.info("shutting down: releasing drill sessions")
        store.shutdown()
        sync_store.shutdown()
        lifespan._entered = False


app = FastAPI(title="PostgreSQL 演练台", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


def _json_error(exc: DrillError | SyncDrillError) -> JSONResponse:
    return JSONResponse(
        {"ok": False, "error": str(exc), "kind": type(exc).__name__},
        status_code=getattr(exc, "status_code", 400),
    )


def _resolve(drill_id: str | None):
    if drill_id is None:
        return store.active()
    return store.get(drill_id)


def _render(
    request: Request,
    *,
    drill_id: str | None = None,
    message: str | None = None,
    error: str | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    drill = _resolve(drill_id)
    snapshot = store.snapshot(drill) if drill is not None else None
    context: dict[str, Any] = {
        "request": request,
        "snapshot": snapshot,
        # The page token is bound to this drill round; old pages carry old
        # tokens and are rejected by the server.
        "token": drill.token if drill is not None else None,
        "recent": [store.snapshot(item) for item in store.recent()],
        "message": message,
        "error": error,
        "standby_configured": bool(settings.standby_url),
    }
    return templates.TemplateResponse(request, "index.html", context, status_code=status_code)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return _render(request)


@app.get("/drill/{drill_id}", response_class=HTMLResponse)
async def drill_page(request: Request, drill_id: str) -> HTMLResponse:
    if store.get(drill_id) is None:
        return _render(
            request,
            error="该演练记录已不存在（服务可能已重启或记录已清理），页面不再可操作。",
            status_code=410,
        )
    return _render(request, drill_id=drill_id)


@app.post("/drill/start")
async def start_drill(request: Request) -> HTMLResponse:
    try:
        drill = store.start()
    except DrillError as exc:
        return _render(request, error=f"未能开始演练：{exc}", status_code=409)
    return RedirectResponse(url=f"/drill/{drill.id}", status_code=303)


@app.post("/drill/{drill_id}/release", response_class=HTMLResponse)
async def release_form(request: Request, drill_id: str, token: str = Form(...)) -> HTMLResponse:
    try:
        store.release(drill_id, token)
    except DrillError as exc:
        return _render(request, drill_id=drill_id, error=str(exc), status_code=exc.status_code)
    return _render(
        request,
        drill_id=drill_id,
        message="解锁操作已完成，下方为以受阻更新真实提交结果作出的判定。",
    )


@app.post("/drill/{drill_id}/end", response_class=HTMLResponse)
async def end_form(request: Request, drill_id: str, token: str = Form(...)) -> HTMLResponse:
    try:
        store.end(drill_id, token)
    except DrillError as exc:
        return _render(request, drill_id=drill_id, error=str(exc), status_code=exc.status_code)
    return RedirectResponse(url="/", status_code=303)


# ---------------------------------------------------------------------------
# Synchronous-commit drill pages
# ---------------------------------------------------------------------------


def _sync_render(
    request: Request,
    *,
    sync_id: str | None = None,
    message: str | None = None,
    error: str | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    drill = sync_store.get(sync_id) if sync_id is not None else sync_store.active()
    if drill is None and sync_id is not None:
        return templates.TemplateResponse(
            request,
            "sync.html",
            {
                "request": request,
                "snap": None,
                "token": None,
                "recent": [sync_store.snapshot(item) for item in sync_store.recent()],
                "standby_configured": bool(settings.standby_url),
                "persistence_error": sync_store.persistence_error,
                "message": None,
                "error": "该同步提交演练记录已不存在（服务可能已重启或记录已清理）。",
            },
            status_code=410,
        )
    snap = sync_store.snapshot(drill) if drill is not None else None
    context = {
        "request": request,
        "snap": snap,
        "token": drill.token if drill is not None else None,
        "recent": [sync_store.snapshot(item) for item in sync_store.recent()],
        "standby_configured": bool(settings.standby_url),
        "persistence_error": sync_store.persistence_error,
        "message": message,
        "error": error,
    }
    return templates.TemplateResponse(
        request, "sync.html", context, status_code=status_code
    )


@app.get("/sync", response_class=HTMLResponse)
async def sync_page(request: Request) -> HTMLResponse:
    return _sync_render(request)


@app.get("/sync/{sync_id}", response_class=HTMLResponse)
async def sync_round_page(request: Request, sync_id: str) -> HTMLResponse:
    return _sync_render(request, sync_id=sync_id)


@app.post("/sync/start")
async def sync_start(request: Request) -> HTMLResponse:
    drill = sync_store.start_round()
    return RedirectResponse(url=f"/sync/{drill.id}", status_code=303)


@app.post("/sync/{sync_id}/pause")
async def sync_pause(request: Request, sync_id: str, token: str = Form("")) -> HTMLResponse:
    try:
        sync_store.pause_replay(sync_id, token)
        return _sync_render(request, sync_id=sync_id, message="备库 WAL 回放已暂停并确认生效。")
    except SyncDrillError as exc:
        return _sync_render(
            request, sync_id=sync_id, error=str(exc), status_code=exc.status_code
        )


@app.post("/sync/{sync_id}/resume")
async def sync_resume(request: Request, sync_id: str, token: str = Form("")) -> HTMLResponse:
    try:
        sync_store.resume_replay(sync_id, token)
        return _sync_render(request, sync_id=sync_id, message="备库 WAL 回放已恢复。")
    except SyncDrillError as exc:
        return _sync_render(
            request, sync_id=sync_id, error=str(exc), status_code=exc.status_code
        )


@app.post("/sync/{sync_id}/submit")
async def sync_submit(
    request: Request,
    sync_id: str,
    token: str = Form(""),
    order_no: str = Form(...),
    note: str = Form(""),
) -> HTMLResponse:
    try:
        sync_store.submit_order(sync_id, token, order_no, note)
        return _sync_render(
            request, sync_id=sync_id,
            message=f"部署单 {order_no} 已用 remote_apply 提交，等待数据库确认同步复制等待。",
        )
    except SyncDrillError as exc:
        return _sync_render(
            request, sync_id=sync_id, error=str(exc), status_code=exc.status_code
        )


@app.post("/sync/{sync_id}/cancel")
async def sync_cancel(request: Request, sync_id: str, token: str = Form("")) -> HTMLResponse:
    try:
        result = sync_store.cancel_wait(sync_id, token)
        return _sync_render(
            request, sync_id=sync_id,
            message=f"已执行取消等待，本次提交结果：{result['label']}。",
        )
    except SyncDrillError as exc:
        return _sync_render(
            request, sync_id=sync_id, error=str(exc), status_code=exc.status_code
        )


@app.post("/sync/{sync_id}/verify")
async def sync_verify(request: Request, sync_id: str, token: str = Form("")) -> HTMLResponse:
    try:
        sync_store.verify_order(sync_id, token)
        return _sync_render(request, sync_id=sync_id, message="已按单号重新核对两库实况。")
    except SyncDrillError as exc:
        return _sync_render(
            request, sync_id=sync_id, error=str(exc), status_code=exc.status_code
        )


# ---------------------------------------------------------------------------
# JSON API (used by the live poller and by automated checks)
# ---------------------------------------------------------------------------


@app.get("/api/state", response_class=JSONResponse)
async def api_state(drill_id: str | None = None) -> JSONResponse:
    active = store.active()
    drill = store.get(drill_id) if drill_id is not None else active
    return JSONResponse(
        {
            "ok": True,
            "active_drill_id": active.id if active else None,
            "drill": store.snapshot(drill) if drill is not None else None,
            "recent": [store.snapshot(item) for item in store.recent()],
        }
    )


@app.get("/api/drills/{drill_id}", response_class=JSONResponse)
async def api_drill(drill_id: str) -> JSONResponse:
    drill = store.get(drill_id)
    if drill is None:
        return _json_error(
            DrillError("该演练记录已不存在，未执行任何操作。")
        )
    return JSONResponse({"ok": True, "drill": store.snapshot(drill)})


@app.post("/api/drills/{drill_id}/release", response_class=JSONResponse)
async def api_release(drill_id: str, token: str = Form(...)) -> JSONResponse:
    try:
        result = store.release(drill_id, token)
    except DrillError as exc:
        return _json_error(exc)
    return JSONResponse(
        {
            "ok": True,
            "release": result,
            "drill": store.snapshot(store.require(drill_id)),
        }
    )


@app.post("/api/drills/{drill_id}/end", response_class=JSONResponse)
async def api_end(drill_id: str, token: str = Form(...)) -> JSONResponse:
    try:
        store.end(drill_id, token)
    except DrillError as exc:
        return _json_error(exc)
    return JSONResponse({"ok": True, "drill": store.snapshot(store.require(drill_id))})


@app.get("/api/sync/state", response_class=JSONResponse)
async def api_sync_state(sync_id: str | None = None) -> JSONResponse:
    active = sync_store.active()
    drill = sync_store.get(sync_id) if sync_id is not None else active
    return JSONResponse(
        {
            "ok": True,
            "standby_configured": bool(settings.standby_url),
            "active_sync_id": active.id if active else None,
            "sync": sync_store.snapshot(drill) if drill is not None else None,
            "recent": [sync_store.snapshot(item) for item in sync_store.recent()],
            "persistence_error": sync_store.persistence_error,
        }
    )


@app.get("/api/sync/{sync_id}", response_class=JSONResponse)
async def api_sync_round(sync_id: str) -> JSONResponse:
    drill = sync_store.get(sync_id)
    if drill is None:
        return _json_error(SyncDrillNotFound("该同步提交演练记录已不存在。"))
    return JSONResponse({"ok": True, "sync": sync_store.snapshot(drill)})


@app.post("/api/sync/{sync_id}/pause", response_class=JSONResponse)
async def api_sync_pause(sync_id: str, token: str = Form("")) -> JSONResponse:
    try:
        sync_store.pause_replay(sync_id, token)
    except SyncDrillError as exc:
        return _json_error(exc)
    return JSONResponse({"ok": True, "sync": sync_store.snapshot(sync_store.require(sync_id))})


@app.post("/api/sync/{sync_id}/resume", response_class=JSONResponse)
async def api_sync_resume(sync_id: str, token: str = Form("")) -> JSONResponse:
    try:
        sync_store.resume_replay(sync_id, token)
    except SyncDrillError as exc:
        return _json_error(exc)
    return JSONResponse({"ok": True, "sync": sync_store.snapshot(sync_store.require(sync_id))})


@app.post("/api/sync/{sync_id}/submit", response_class=JSONResponse)
async def api_sync_submit(
    sync_id: str,
    token: str = Form(""),
    order_no: str = Form(...),
    note: str = Form(""),
) -> JSONResponse:
    try:
        sync_store.submit_order(sync_id, token, order_no, note)
    except SyncDrillError as exc:
        return _json_error(exc)
    return JSONResponse({"ok": True, "sync": sync_store.snapshot(sync_store.require(sync_id))})


@app.post("/api/sync/{sync_id}/cancel", response_class=JSONResponse)
async def api_sync_cancel(sync_id: str, token: str = Form("")) -> JSONResponse:
    try:
        result = sync_store.cancel_wait(sync_id, token)
    except SyncDrillError as exc:
        return _json_error(exc)
    return JSONResponse(
        {
            "ok": True,
            "result": result,
            "sync": sync_store.snapshot(sync_store.require(sync_id)),
        }
    )


@app.post("/api/sync/{sync_id}/verify", response_class=JSONResponse)
async def api_sync_verify(sync_id: str, token: str = Form("")) -> JSONResponse:
    try:
        verification = sync_store.verify_order(sync_id, token)
    except SyncDrillError as exc:
        return _json_error(exc)
    return JSONResponse(
        {
            "ok": True,
            "verification": verification,
            "sync": sync_store.snapshot(sync_store.require(sync_id)),
        }
    )


@app.get("/healthz", response_class=JSONResponse)
async def healthz() -> JSONResponse:
    payload: dict[str, Any] = {"ok": True}
    try:
        with connect(application_name="lockdrill:health", autocommit=True) as conn:
            conn.execute("SELECT 1").fetchone()
    except Exception as exc:
        payload.update(ok=False, error=str(exc))
    if settings.standby_url:
        try:
            from .db import connect_standby

            with connect_standby(
                application_name="lockdrill:health-standby", autocommit=True
            ) as conn:
                row = conn.execute(
                    "SELECT pg_is_in_recovery() AS in_recovery"
                ).fetchone()
                payload["standby_in_recovery"] = bool(row and row["in_recovery"])
        except Exception as exc:
            payload["standby_error"] = str(exc)
    active = store.active()
    payload["active_drill_id"] = active.id if active else None
    status = 200 if payload["ok"] else 503
    return JSONResponse(payload, status_code=status)
