import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import jwt
import psycopg
from fastapi import APIRouter, HTTPException, Request
from psycopg.rows import dict_row

router = APIRouter()

JWT_COOKIE_NAME = "rh_session"


def _get_dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not dsn:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")
    return dsn


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_dsn(), row_factory=dict_row)


def _jwt_secret() -> str:
    secret = os.getenv("JWT_SECRET", "").strip()
    if not secret:
        raise HTTPException(status_code=500, detail="JWT_SECRET is not set")
    return secret


def _decode_session_jwt(token: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, _jwt_secret(), algorithms=["HS256"])
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid session")


def _admin_user_ids() -> set[str]:
    raw = os.getenv("DEV_USER_IDS", "").strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _require_admin(request: Request) -> Dict[str, Any]:
    token = request.cookies.get(JWT_COOKIE_NAME, "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    claims = _decode_session_jwt(token)
    uid = str(claims.get("sub") or "")
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid session")
    is_admin = uid in _admin_user_ids()
    if not is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return {
        "discord_user_id": uid,
        "discord_username": str(claims.get("name") or ""),
        "is_admin": is_admin,
    }


def _table_exists(cur, fq_table: str) -> bool:
    cur.execute("SELECT to_regclass(%s) AS t", (fq_table,))
    row = cur.fetchone() or {}
    return bool(row.get("t"))


def _count_table(cur, fq_table: str) -> int:
    if not _table_exists(cur, fq_table):
        return 0
    cur.execute(f"SELECT COUNT(*)::int AS c FROM {fq_table}")
    return int((cur.fetchone() or {}).get("c") or 0)


def _latest_ts(cur, fq_table: str, col: str) -> Optional[datetime]:
    if not _table_exists(cur, fq_table):
        return None
    cur.execute(f"SELECT MAX({col}) AS ts FROM {fq_table}")
    row = cur.fetchone() or {}
    ts = row.get("ts")
    return ts if isinstance(ts, datetime) else None


@router.get("/api/admin/overview")
def admin_overview(request: Request):
    user = _require_admin(request)
    now = datetime.now(timezone.utc)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            counts = {
                "spy_reports": _count_table(cur, "public.spy_reports"),
                "kg_top_kingdoms": _count_table(cur, "public.kg_top_kingdoms"),
                "nw_history": _count_table(cur, "public.nw_history"),
                "nw_latest": _count_table(cur, "public.nw_latest"),
                "kg_connections": _count_table(cur, "public.user_kg_connections"),
            }

            latest = {
                "spy_report_at": _latest_ts(cur, "public.spy_reports", "created_at"),
                "rankings_fetch_at": _latest_ts(cur, "public.kg_top_kingdoms", "fetched_at"),
                "nw_tick_at": _latest_ts(cur, "public.nw_history", "tick_time"),
            }

            cur.execute("SELECT current_database() AS db_name")
            db_name = (cur.fetchone() or {}).get("db_name")

            cur.execute(
                """
                SELECT kingdom, rank, networth, updated_at
                FROM public.nw_latest
                ORDER BY rank ASC
                LIMIT 10
                """
            ) if _table_exists(cur, "public.nw_latest") else None
            top = cur.fetchall() if _table_exists(cur, "public.nw_latest") else []

        def age_s(ts: Optional[datetime]) -> Optional[int]:
            if ts is None:
                return None
            return int((now - ts).total_seconds())

        health = {
            "rankings_age_seconds": age_s(latest["rankings_fetch_at"]),
            "nw_tick_age_seconds": age_s(latest["nw_tick_at"]),
            "spy_report_age_seconds": age_s(latest["spy_report_at"]),
        }

        return {
            "ok": True,
            "admin": user,
            "now": now.isoformat(),
            "database": {"name": db_name},
            "counts": counts,
            "latest": {k: (v.isoformat() if v else None) for k, v in latest.items()},
            "health": health,
            "top_nw_latest": top,
            "notes": [
                "Admin access is controlled by DEV_USER_IDS.",
                "Use rankings_age_seconds/nw_tick_age_seconds to monitor poll freshness.",
            ],
        }
    finally:
        conn.close()
