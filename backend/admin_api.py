import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import jwt
import psycopg
from fastapi import APIRouter, HTTPException, Request
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

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


def _as_utc_aware(ts: Optional[datetime]) -> Optional[datetime]:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class AdminNoteBody(BaseModel):
    note: str = Field(..., min_length=1, max_length=4000)


class AllianceCreateBody(BaseModel):
    name: str = Field(..., min_length=2, max_length=80)
    slug: str = Field(..., min_length=2, max_length=80)


class AllianceMembershipAssignBody(BaseModel):
    alliance_id: int = Field(..., gt=0)
    discord_user_id: str = Field(..., min_length=3, max_length=64)
    discord_username: str = Field(default="", max_length=128)
    role: str = Field(default="member", min_length=3, max_length=24)


def ensure_admin_tables():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.admin_feedback_notes (
                    id BIGSERIAL PRIMARY KEY,
                    note_text TEXT NOT NULL,
                    created_by_discord_user_id TEXT NOT NULL,
                    created_by_discord_username TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
        conn.commit()
    finally:
        conn.close()


@router.get("/api/admin/overview")
def admin_overview(request: Request):
    user = _require_admin(request)
    now = datetime.now(timezone.utc)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            has_nw_latest = _table_exists(cur, "public.nw_latest")
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

            if has_nw_latest:
                cur.execute(
                    """
                    SELECT kingdom, rank, networth, updated_at
                    FROM public.nw_latest
                    ORDER BY rank ASC
                    LIMIT 10
                    """
                )
                top = cur.fetchall()
            else:
                top = []

        def age_s(ts: Optional[datetime]) -> Optional[int]:
            ts_aware = _as_utc_aware(ts)
            if ts_aware is None:
                return None
            return int((now - ts_aware).total_seconds())

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


@router.get("/api/admin/notes")
def list_admin_notes(request: Request, limit: int = 200):
    _require_admin(request)
    safe_limit = max(1, min(int(limit), 500))

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, note_text, created_by_discord_user_id, created_by_discord_username, created_at
                FROM public.admin_feedback_notes
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (safe_limit,),
            )
            rows = cur.fetchall()

        return {"ok": True, "notes": rows}
    finally:
        conn.close()


@router.post("/api/admin/notes")
def create_admin_note(body: AdminNoteBody, request: Request):
    user = _require_admin(request)
    note_text = body.note.strip()
    if not note_text:
        raise HTTPException(status_code=400, detail="Note cannot be empty")

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.admin_feedback_notes
                  (note_text, created_by_discord_user_id, created_by_discord_username, created_at)
                VALUES
                  (%s, %s, %s, now())
                RETURNING id, note_text, created_by_discord_user_id, created_by_discord_username, created_at
                """,
                (
                    note_text,
                    user["discord_user_id"],
                    user["discord_username"],
                ),
            )
            created = cur.fetchone()
        conn.commit()
        return {"ok": True, "note": created}
    finally:
        conn.close()


@router.post("/api/admin/alliances")
def create_alliance(body: AllianceCreateBody, request: Request):
    _require_admin(request)
    name = body.name.strip()
    slug = body.slug.strip().lower()
    if not name or not slug:
        raise HTTPException(status_code=400, detail="name and slug are required")

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.alliances (name, slug, created_at)
                VALUES (%s, %s, now())
                ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
                RETURNING id, name, slug, created_at
                """,
                (name, slug),
            )
            row = cur.fetchone()
        conn.commit()
        return {"ok": True, "alliance": row}
    finally:
        conn.close()


@router.get("/api/admin/alliances")
def list_alliances(request: Request):
    _require_admin(request)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id, a.name, a.slug, a.created_at,
                       COUNT(m.id)::int AS members
                FROM public.alliances a
                LEFT JOIN public.alliance_memberships m
                  ON m.alliance_id = a.id AND m.status = 'active'
                GROUP BY a.id, a.name, a.slug, a.created_at
                ORDER BY a.name
                """
            )
            rows = cur.fetchall()
        return {"ok": True, "alliances": rows}
    finally:
        conn.close()


@router.post("/api/admin/alliances/memberships")
def assign_alliance_membership(body: AllianceMembershipAssignBody, request: Request):
    _require_admin(request)
    uid = body.discord_user_id.strip()
    uname = body.discord_username.strip()
    role = body.role.strip().lower() or "member"

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.app_users (discord_user_id, discord_username, created_at, updated_at)
                VALUES (%s, %s, now(), now())
                ON CONFLICT (discord_user_id) DO UPDATE SET
                  discord_username = CASE
                    WHEN EXCLUDED.discord_username = '' THEN public.app_users.discord_username
                    ELSE EXCLUDED.discord_username
                  END,
                  updated_at = now()
                """,
                (uid, uname),
            )

            cur.execute(
                """
                INSERT INTO public.alliance_memberships
                  (alliance_id, discord_user_id, role, status, created_at)
                VALUES
                  (%s, %s, %s, 'active', now())
                ON CONFLICT (alliance_id, discord_user_id) DO UPDATE SET
                  role = EXCLUDED.role,
                  status = 'active'
                RETURNING id, alliance_id, discord_user_id, role, status, created_at
                """,
                (body.alliance_id, uid, role),
            )
            row = cur.fetchone()
        conn.commit()
        return {"ok": True, "membership": row}
    finally:
        conn.close()


@router.get("/api/admin/users")
def list_app_users(request: Request, limit: int = 500, search: str = ""):
    _require_admin(request)
    lim = max(1, min(int(limit), 2000))
    s = search.strip()

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if s:
                like = f"%{s}%"
                cur.execute(
                    """
                    SELECT discord_user_id, discord_username, created_at, updated_at
                    FROM public.app_users
                    WHERE discord_user_id ILIKE %s OR COALESCE(discord_username,'') ILIKE %s
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """,
                    (like, like, lim),
                )
            else:
                cur.execute(
                    """
                    SELECT discord_user_id, discord_username, created_at, updated_at
                    FROM public.app_users
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """,
                    (lim,),
                )
            users = cur.fetchall()

            cur.execute(
                """
                SELECT
                  m.discord_user_id,
                  m.alliance_id,
                  m.role,
                  m.status,
                  m.created_at,
                  a.name AS alliance_name,
                  a.slug AS alliance_slug
                FROM public.alliance_memberships m
                JOIN public.alliances a ON a.id = m.alliance_id
                ORDER BY m.created_at DESC
                """
            )
            memberships = cur.fetchall()

            cur.execute(
                """
                SELECT discord_user_id, alliance_id
                FROM public.user_active_alliance
                """
            )
            active_rows = cur.fetchall()

        active_map = {
            str(r.get("discord_user_id") or ""): int(r.get("alliance_id") or 0)
            for r in (active_rows or [])
        }

        m_map: Dict[str, List[Dict[str, Any]]] = {}
        for m in memberships or []:
            uid = str(m.get("discord_user_id") or "")
            if not uid:
                continue
            if uid not in m_map:
                m_map[uid] = []
            m_map[uid].append(
                {
                    "alliance_id": int(m["alliance_id"]),
                    "alliance_name": str(m["alliance_name"]),
                    "alliance_slug": str(m["alliance_slug"]),
                    "role": str(m.get("role") or "member"),
                    "status": str(m.get("status") or "active"),
                    "created_at": m.get("created_at"),
                }
            )

        out = []
        for u in users or []:
            uid = str(u.get("discord_user_id") or "")
            out.append(
                {
                    "discord_user_id": uid,
                    "discord_username": str(u.get("discord_username") or ""),
                    "created_at": u.get("created_at"),
                    "updated_at": u.get("updated_at"),
                    "active_alliance_id": active_map.get(uid),
                    "memberships": m_map.get(uid, []),
                }
            )

        return {"ok": True, "users": out}
    finally:
        conn.close()
