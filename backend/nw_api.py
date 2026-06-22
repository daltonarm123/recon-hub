from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, HTTPException

from db_dsn import resolve_database_dsn
from rankings_poll import _ensure_tables as ensure_rankings_tables
from rankings_poll import _poll_rankings_once, _resolve_rankings_creds

router = APIRouter()


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


def _seed_rankings_if_possible() -> str:
    """
    Attempt a one-shot rankings pull using configured poller credentials.
    Returns a short diagnostic note for UI consumption.
    """
    try:
        ensure_rankings_tables()
        creds = _resolve_rankings_creds()
    except Exception as exc:
        return (
            "No rankings data yet. Configure KG poller credentials "
            f"(KG_POLLER_*) and retry. Detail: {exc}"
        )

    world_id = "1"
    try:
        import os

        world_id = str(os.getenv("KG_WORLD_ID", "1") or "1")
    except Exception:
        world_id = "1"

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


@router.get("/kingdoms")
def nw_kingdoms(limit: int = 300, search: str = ""):
    """
    Source of truth for NWOT list = public.kg_top_kingdoms (filled by rankings_poller).
    We LEFT JOIN nw_history so the UI can show last_tick + points.
    """
    s = (search or "").strip()

    note = ""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            has_top = _table_exists(cur, "public.kg_top_kingdoms")
            has_hist = _table_exists(cur, "public.nw_history")

            if not has_top:
                note = _seed_rankings_if_possible()
                has_top = _table_exists(cur, "public.kg_top_kingdoms")
                if not has_top:
                    return {
                        "ok": True,
                        "kingdoms": [],
                        "note": note or "Rankings source table is not ready yet (kg_top_kingdoms).",
                    }

            if s:
                like = f"%{s}%"
                if has_hist:
                    cur.execute(
                        """
                        WITH hist AS (
                            SELECT kingdom,
                                   MAX(tick_time) AS last_tick,
                                   COUNT(*)::int  AS points
                            FROM public.nw_history
                            GROUP BY kingdom
                        )
                        SELECT
                            k.ranking AS rank,
                            k.kingdom_id,
                            k.kingdom,
                            k.networth,
                            COALESCE(k.alliance, '') AS alliance,
                            k.fetched_at,
                            h.last_tick,
                            COALESCE(h.points, 0)::int AS points
                        FROM public.kg_top_kingdoms k
                        LEFT JOIN hist h
                            ON h.kingdom = k.kingdom
                        WHERE k.kingdom ILIKE %s
                           OR COALESCE(k.alliance,'') ILIKE %s
                        ORDER BY k.ranking ASC NULLS LAST
                        LIMIT %s
                        """,
                        (like, like, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            k.ranking AS rank,
                            k.kingdom_id,
                            k.kingdom,
                            k.networth,
                            COALESCE(k.alliance, '') AS alliance,
                            k.fetched_at,
                            NULL::timestamptz AS last_tick,
                            0::int AS points
                        FROM public.kg_top_kingdoms k
                        WHERE k.kingdom ILIKE %s
                           OR COALESCE(k.alliance,'') ILIKE %s
                        ORDER BY k.ranking ASC NULLS LAST
                        LIMIT %s
                        """,
                        (like, like, limit),
                    )
            else:
                if has_hist:
                    cur.execute(
                        """
                        WITH hist AS (
                            SELECT kingdom,
                                   MAX(tick_time) AS last_tick,
                                   COUNT(*)::int  AS points
                            FROM public.nw_history
                            GROUP BY kingdom
                        )
                        SELECT
                            k.ranking AS rank,
                            k.kingdom_id,
                            k.kingdom,
                            k.networth,
                            COALESCE(k.alliance, '') AS alliance,
                            k.fetched_at,
                            h.last_tick,
                            COALESCE(h.points, 0)::int AS points
                        FROM public.kg_top_kingdoms k
                        LEFT JOIN hist h
                            ON h.kingdom = k.kingdom
                        ORDER BY k.ranking ASC NULLS LAST
                        LIMIT %s
                        """,
                        (limit,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            k.ranking AS rank,
                            k.kingdom_id,
                            k.kingdom,
                            k.networth,
                            COALESCE(k.alliance, '') AS alliance,
                            k.fetched_at,
                            NULL::timestamptz AS last_tick,
                            0::int AS points
                        FROM public.kg_top_kingdoms k
                        ORDER BY k.ranking ASC NULLS LAST
                        LIMIT %s
                        """,
                        (limit,),
                    )
            rows = cur.fetchall()

            # Common first-run case: table exists but has no rows yet.
            if not rows and not s:
                note = _seed_rankings_if_possible()
                has_hist = _table_exists(cur, "public.nw_history")

                if has_hist:
                    cur.execute(
                        """
                        WITH hist AS (
                            SELECT kingdom,
                                   MAX(tick_time) AS last_tick,
                                   COUNT(*)::int  AS points
                            FROM public.nw_history
                            GROUP BY kingdom
                        )
                        SELECT
                            k.ranking AS rank,
                            k.kingdom_id,
                            k.kingdom,
                            k.networth,
                            COALESCE(k.alliance, '') AS alliance,
                            k.fetched_at,
                            h.last_tick,
                            COALESCE(h.points, 0)::int AS points
                        FROM public.kg_top_kingdoms k
                        LEFT JOIN hist h
                            ON h.kingdom = k.kingdom
                        ORDER BY k.ranking ASC NULLS LAST
                        LIMIT %s
                        """,
                        (limit,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            k.ranking AS rank,
                            k.kingdom_id,
                            k.kingdom,
                            k.networth,
                            COALESCE(k.alliance, '') AS alliance,
                            k.fetched_at,
                            NULL::timestamptz AS last_tick,
                            0::int AS points
                        FROM public.kg_top_kingdoms k
                        ORDER BY k.ranking ASC NULLS LAST
                        LIMIT %s
                        """,
                        (limit,),
                    )
                rows = cur.fetchall()

        out: Dict[str, Any] = {"ok": True, "kingdoms": rows}
        if note:
            out["note"] = note
        return out
    finally:
        conn.close()


@router.get("/history/{kingdom}")
def nw_history(kingdom: str, hours: int = 24):
    """
    Returns chart points: [{t: ISO8601, v: networth}, ...]
    """
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
        for r in rows:
            tt = r.get("tick_time")
            nw = r.get("networth")
            if tt is None or nw is None:
                continue
            points.append({"t": tt.isoformat(), "v": int(nw)})

        return points
    finally:
        conn.close()


@router.get("/status")
def nw_status():
    """
    Returns source freshness for rankings->nw pipeline.
    """
    now = datetime.now(timezone.utc)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            has_top = _table_exists(cur, "public.kg_top_kingdoms")
            has_hist = _table_exists(cur, "public.nw_history")

            r1: Dict[str, Any] = {}
            r2: Dict[str, Any] = {}
            if has_top:
                cur.execute(
                    """
                    SELECT MAX(fetched_at) AS last_rankings_fetch
                    FROM public.kg_top_kingdoms
                    """
                )
                r1 = cur.fetchone() or {}

            if has_hist:
                cur.execute(
                    """
                    SELECT MAX(tick_time) AS last_nw_tick
                    FROM public.nw_history
                    """
                )
                r2 = cur.fetchone() or {}

        last_fetch = r1.get("last_rankings_fetch")
        last_tick = r2.get("last_nw_tick")

        fetch_age_s = None
        tick_age_s = None
        if last_fetch is not None:
            fetch_age_s = int((now - last_fetch).total_seconds())
        if last_tick is not None:
            tick_age_s = int((now - last_tick).total_seconds())

        return {
            "ok": True,
            "now": now.isoformat(),
            "has_kg_top_kingdoms": has_top,
            "has_nw_history": has_hist,
            "last_rankings_fetch": last_fetch.isoformat() if last_fetch else None,
            "last_nw_tick": last_tick.isoformat() if last_tick else None,
            "rankings_age_seconds": fetch_age_s,
            "nw_tick_age_seconds": tick_age_s,
        }
    finally:
        conn.close()
