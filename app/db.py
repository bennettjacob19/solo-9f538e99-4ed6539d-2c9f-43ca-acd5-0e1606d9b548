"""Database helpers."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from .config import settings

DRILL_ROW_ID = 1


def connect(
    *,
    application_name: str | None = None,
    autocommit: bool = False,
    url: str | None = None,
) -> psycopg.Connection[Any]:
    """Open a connection with the drill's standard connection settings.

    Rows are returned as dicts so callers can address columns by name.
    ``url`` selects the primary (default) or the physical standby.
    """
    kwargs: dict[str, Any] = {
        "connect_timeout": settings.connect_timeout,
        "autocommit": autocommit,
        "row_factory": dict_row,
    }
    if application_name is not None:
        kwargs["application_name"] = application_name
    return psycopg.connect(url or settings.database_url, **kwargs)


def connect_standby(
    *,
    application_name: str | None = None,
    autocommit: bool = False,
) -> psycopg.Connection[Any]:
    if not settings.standby_url:
        raise RuntimeError("STANDBY_URL 未配置，同步提交演练不可用。")
    return connect(application_name=application_name, autocommit=autocommit,
                   url=settings.standby_url)


def ensure_schema() -> None:
    """Create and seed the tables used by both drills."""
    with connect(application_name=f"{settings.session_prefix}:setup") as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lock_drill_row (
                id integer PRIMARY KEY,
                value integer NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            INSERT INTO lock_drill_row (id, value)
            VALUES (%s, 0)
            ON CONFLICT (id) DO NOTHING
            """,
            (DRILL_ROW_ID,),
        )
        # Deployment records for the synchronous-commit drill. Each row is one
        # transaction committed on the primary with synchronous_commit =
        # remote_apply; the standby can only see it after WAL replay reaches it.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS deploy_order (
                order_no text PRIMARY KEY,
                note text NOT NULL DEFAULT '',
                created_at timestamptz NOT NULL DEFAULT now(),
                committed_at timestamptz
            )
            """
        )
        conn.commit()
