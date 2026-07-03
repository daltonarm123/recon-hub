import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import psycopg
from fastapi import APIRouter, HTTPException
from psycopg.rows import dict_row

from db_dsn import resolve_database_dsn
from rankings_poll import (
    _ensure_tables as ensure_rankings_tables,
    _poll_rankings_once,
    _resolve_rankings_creds,
    get_rankings_health,
)

router = APIRouter()

_SEED_LOCK = threading.Lock()
_SEED_THREAD: Optional[threading.Thread] = None
_LAST_SEED_NOTE = ""
_LAST_SEED_FINISHED_AT = 0.0
_SEED_RETRY_COOLDOWN_SECONDS = 45.0


def _get_dsn() -> str:
    try:
        return resolve_database_dsn()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_dsn(), row_factory=dict_row)


def _table_exists(cur: psycopg.Cursor, regclass_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) AS t", (regclass_name,))
    row = cur.fetchone() or {}
    return bool(row.get("t"))


def _parse_window(text: str, default_minutes: int) -> timedelta:
    raw = str(text or "").strip().lower()
    if not raw:
        return timedelta(minutes=default_minutes)
    unit = raw[-1]
    num = raw[:-1]
    if unit not in {"m", "h", "d"}:
        raise HTTPException(status_code=400, detail="Window/range must end with m, h, or d (example: 15m, 24h).")
    try:
        value = int(num)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Window/range value must be an integer.") from exc
    if value <= 0:
        raise HTTPException(status_code=400, detail="Window/range must be greater than zero.")
    if unit == "m":
        return timedelta(minutes=value)
    if unit == "h":
        return timedelta(hours=value)
    return timedelta(days=value)


def _seed_rankings_if_possible() -> str:
    try:
        ensure_rankings_tables()
        creds = _resolve_rankings_creds()
    except Exception as exc:
        return (
            "No rankings data yet. Configure KG poller credentials or login auth and retry. "
            f"Detail: {exc}"
        )

    world_id = "1"
    try:
        import os

        world_id = str(os.getenv("KG_WORLD_ID", "1") or "1")
    except Exception:
        world_id = "1"

    if not creds:
        return "No static creds to seed with. Poller will rely on login-auth in background."

    last_err: Exception | None = None
    for cred in creds:
        try:
            count, _acct = _poll_rankings_once(world_id=world_id, creds=cred)
            if int(count or 0) > 0:
                return "Rankings synced on demand."
        except Exception as exc:
            last_err = exc
            continue

    if last_err is not None:
        return f"Could not sync rankings now. Detail: {last_err}"
    return "No rankings rows returned from KG yet."


def _kickoff_seed_rankings_if_needed() -> str:
    global _SEED_THREAD, _LAST_SEED_NOTE, _LAST_SEED_FINISHED_AT

    with _SEED_LOCK:
        if _SEED_THREAD and _SEED_THREAD.is_alive():
            return _LAST_SEED_NOTE or "Rankings sync in progress..."

        now = time.monotonic()
        if _LAST_SEED_NOTE and (now - _LAST_SEED_FINISHED_AT) < _SEED_RETRY_COOLDOWN_SECONDS:
            return _LAST_SEED_NOTE

        _LAST_SEED_NOTE = "Rankings sync started in background..."

        def _runner():
            global _LAST_SEED_NOTE, _LAST_SEED_FINISHED_AT
            try:
                _LAST_SEED_NOTE = _seed_rankings_if_possible()
            except Exception as exc:
                _LAST_SEED_NOTE = f"Could not sync rankings now. Detail: {exc}"
            finally:
                _LAST_SEED_FINISHED_AT = time.monotonic()

        _SEED_THREAD = threading.Thread(target=_runner, daemon=True, name="nw-seed")
        _SEED_THREAD.start()
        return _LAST_SEED_NOTE


@router.get("/live")
def nw_live(limit: int = 100):
    safe_limit = max(1, min(int(limit), 500))
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, "public.nw_latest"):
                return {"ok": True, "rows": [], "updatedAt": None, "note": "nw_latest is not ready yet."}

            cur.execute(
                """
                SELECT kingdom_id, kingdom, rank, networth, delta, updated_at
                FROM public.nw_latest
                ORDER BY rank ASC NULLS LAST, kingdom ASC
                LIMIT %s
                """,
                (safe_limit,),
            )
            rows = cur.fetchall()

            if not rows and _table_exists(cur, "public.kg_top_kingdoms"):
                cur.execute(
                    """
                    SELECT kingdom_id, kingdom, COALESCE(ranking, 999999) AS rank, networth, fetched_at AS updated_at
                    FROM public.kg_top_kingdoms
                    ORDER BY ranking ASC NULLS LAST, kingdom ASC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                fallback_rows = cur.fetchall()
                rows = [
                    {
                        "kingdom_id": r.get("kingdom_id"),
                        "kingdom": r.get("kingdom"),
                        "rank": r.get("rank"),
                        "networth": r.get("networth"),
                        "delta": 0,
                        "updated_at": r.get("updated_at"),
                    }
                    for r in fallback_rows
                ]

        out_rows: List[Dict[str, Any]] = []
        latest_ts: Optional[datetime] = None
        for row in rows:
            updated_at = row.get("updated_at")
            if isinstance(updated_at, datetime) and (latest_ts is None or updated_at > latest_ts):
                latest_ts = updated_at
            out_rows.append(
                {
                    "kingdomId": int(row.get("kingdom_id") or 0),
                    "kingdom": str(row.get("kingdom") or ""),
                    "rank": int(row.get("rank") or 999999),
                    "networth": int(row.get("networth") or 0),
                    "delta": int(row.get("delta") or 0),
                    "updatedAt": updated_at.isoformat() if isinstance(updated_at, datetime) else None,
                }
            )

        return {
            "ok": True,
            "rows": out_rows,
            "updatedAt": latest_ts.isoformat() if latest_ts else None,
            "count": len(out_rows),
        }
    finally:
        conn.close()


@router.get("/history")
def nw_history_by_kingdom_id(kingdomId: int, range: str = "24h"):
    safe_kingdom_id = int(kingdomId)
    span = _parse_window(range, default_minutes=24 * 60)
    since = datetime.now(timezone.utc) - span

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, "public.nw_history"):
                return {"ok": True, "kingdomId": safe_kingdom_id, "range": range, "points": []}

            cur.execute(
                """
                SELECT tick_time, networth
                FROM public.nw_history
                WHERE kingdom_id = %s
                  AND tick_time >= %s
                ORDER BY tick_time ASC
                """,
                (safe_kingdom_id, since),
            )
            rows = cur.fetchall()

        points = []
        for row in rows:
            tt = row.get("tick_time")
            nw = row.get("networth")
            if not isinstance(tt, datetime) or nw is None:
                continue
            points.append({"t": tt.isoformat(), "v": int(nw)})

        return {"ok": True, "kingdomId": safe_kingdom_id, "range": range, "points": points}
    finally:
        conn.close()


@router.get("/movers")
def nw_movers(window: str = "15m", minDelta: int = 1000, limit: int = 10):
    span = _parse_window(window, default_minutes=15)
    since = datetime.now(timezone.utc) - span
    safe_limit = max(1, min(int(limit), 50))
    safe_min_delta = max(0, int(minDelta))

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, "public.nw_history"):
                return {"ok": True, "window": window, "minDelta": safe_min_delta, "gainers": [], "losers": []}

            cur.execute(
                """
                WITH h AS (
                    SELECT kingdom_id, kingdom, tick_time, networth
                    FROM public.nw_history
                    WHERE tick_time >= %s
                ),
                first_points AS (
                    SELECT DISTINCT ON (kingdom_id) kingdom_id, kingdom, networth AS first_nw
                    FROM h
                    ORDER BY kingdom_id, tick_time ASC
                ),
                last_points AS (
                    SELECT DISTINCT ON (kingdom_id) kingdom_id, kingdom, networth AS last_nw
                    FROM h
                    ORDER BY kingdom_id, tick_time DESC
                ),
                deltas AS (
                    SELECT
                        l.kingdom_id,
                        COALESCE(l.kingdom, f.kingdom) AS kingdom,
                        (l.last_nw - f.first_nw)::bigint AS delta
                    FROM first_points f
                    JOIN last_points l ON l.kingdom_id = f.kingdom_id
                )
                SELECT
                    d.kingdom_id,
                    d.kingdom,
                    d.delta,
                    COALESCE(n.rank, 999999) AS rank,
                    n.networth,
                    n.updated_at
                FROM deltas d
                LEFT JOIN public.nw_latest n ON n.kingdom_id = d.kingdom_id
                WHERE ABS(d.delta) >= %s
                ORDER BY d.delta DESC, d.kingdom_id ASC
                """,
                (since, safe_min_delta),
            )
            rows = cur.fetchall()

        gainers: List[Dict[str, Any]] = []
        losers: List[Dict[str, Any]] = []
        for row in rows:
            rec = {
                "kingdomId": int(row.get("kingdom_id") or 0),
                "kingdom": str(row.get("kingdom") or ""),
                "rank": int(row.get("rank") or 999999),
                "networth": int(row.get("networth") or 0),
                "delta": int(row.get("delta") or 0),
                "updatedAt": row.get("updated_at").isoformat() if isinstance(row.get("updated_at"), datetime) else None,
            }
            if rec["delta"] >= 0:
                gainers.append(rec)
            else:
                losers.append(rec)

        gainers = sorted(gainers, key=lambda r: r["delta"], reverse=True)[:safe_limit]
        losers = sorted(losers, key=lambda r: r["delta"])[:safe_limit]

        return {
            "ok": True,
            "window": window,
            "minDelta": safe_min_delta,
            "gainers": gainers,
            "losers": losers,
        }
    finally:
        conn.close()


@router.get("/health")
def nw_health():
    health = get_rankings_health()
    now = datetime.now(timezone.utc)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            has_latest = _table_exists(cur, "public.nw_latest")
            has_hist = _table_exists(cur, "public.nw_history")

            last_poll = None
            row_count = 0
            if has_latest:
                cur.execute("SELECT COUNT(*)::int AS c, MAX(updated_at) AS last_poll FROM public.nw_latest")
                row = cur.fetchone() or {}
                row_count = int(row.get("c") or 0)
                last_poll = row.get("last_poll")

            latest_age_seconds = None
            if isinstance(last_poll, datetime):
                latest_age_seconds = int((now - last_poll).total_seconds())

        return {
            "ok": True,
            "login_status": health.get("login_status"),
            "rankings_status": health.get("rankings_status"),
            "auth_mode": health.get("auth_mode"),
            "rows_pulled": int(health.get("rows_pulled") or row_count),
            "last_poll": health.get("last_poll") or (last_poll.isoformat() if isinstance(last_poll, datetime) else None),
            "last_error": health.get("last_error"),
            "rankings_attempts": health.get("attempts") or [],
            "nw_latest_rows": row_count,
            "nw_tables_ready": bool(has_latest and has_hist),
            "nw_age_seconds": latest_age_seconds,
        }
    finally:
        conn.close()


@router.get("/kingdoms")
def nw_kingdoms(limit: int = 300, search: str = ""):
    safe_limit = max(1, min(int(limit), 1000))
    s = (search or "").strip().lower()

    note = ""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, "public.nw_latest"):
                note = _kickoff_seed_rankings_if_needed()
                return {"ok": True, "kingdoms": [], "note": note or "nw_latest is not ready yet."}

            cur.execute(
                """
                SELECT kingdom_id, kingdom, rank, networth, delta, updated_at
                FROM public.nw_latest
                ORDER BY rank ASC NULLS LAST, kingdom ASC
                LIMIT %s
                """,
                (safe_limit,),
            )
            rows = cur.fetchall()

            if not rows and _table_exists(cur, "public.kg_top_kingdoms"):
                cur.execute(
                    """
                    SELECT kingdom_id, kingdom, COALESCE(ranking, 999999) AS rank, networth, fetched_at AS updated_at
                    FROM public.kg_top_kingdoms
                    ORDER BY ranking ASC NULLS LAST, kingdom ASC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                rows = [
                    {
                        "kingdom_id": r.get("kingdom_id"),
                        "kingdom": r.get("kingdom"),
                        "rank": r.get("rank"),
                        "networth": r.get("networth"),
                        "delta": 0,
                        "updated_at": r.get("updated_at"),
                    }
                    for r in cur.fetchall()
                ]

        out = []
        for row in rows:
            kingdom = str(row.get("kingdom") or "")
            if s and s not in kingdom.lower():
                continue
            out.append(
                {
                    "kingdom_id": int(row.get("kingdom_id") or 0),
                    "kingdom": kingdom,
                    "rank": int(row.get("rank") or 999999),
                    "networth": int(row.get("networth") or 0),
                    "delta": int(row.get("delta") or 0),
                    "last_tick": row.get("updated_at").isoformat() if isinstance(row.get("updated_at"), datetime) else None,
                    "points": None,
                }
            )

        return {"ok": True, "kingdoms": out, "note": note or None}
    finally:
        conn.close()


@router.get("/history/{kingdom}")
def nw_history_legacy(kingdom: str, hours: int = 24):
    safe_hours = max(1, min(int(hours), 168))
    since = datetime.now(timezone.utc) - timedelta(hours=safe_hours)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, "public.nw_history"):
                return []

            cur.execute(
                """
                SELECT tick_time, networth
                FROM public.nw_history
                WHERE kingdom = %s
                  AND tick_time >= %s
                ORDER BY tick_time ASC
                """,
                (kingdom, since),
            )
            rows = cur.fetchall()

        points = []
        for row in rows:
            tt = row.get("tick_time")
            nw = row.get("networth")
            if tt is None or nw is None:
                continue
            points.append({"t": tt.isoformat(), "v": int(nw)})
        return points
    finally:
        conn.close()


@router.get("/status")
def nw_status():
    health = nw_health()
    return {
        "ok": True,
        "last_rankings_fetch": health.get("last_poll"),
        "last_nw_tick": health.get("last_poll"),
        "rankings_age_seconds": health.get("nw_age_seconds"),
        "nw_tick_age_seconds": health.get("nw_age_seconds"),
        "rankings_status": health.get("rankings_status"),
    }
