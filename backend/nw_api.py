import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, HTTPException

router = APIRouter()


def _get_dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not dsn:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")
    return dsn


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_dsn(), row_factory=dict_row)


@router.get("/kingdoms")
def nw_kingdoms(limit: int = 300, search: str = ""):
    """
    Source of truth for NWOT list = public.kg_top_kingdoms (filled by rankings_poller).
    We LEFT JOIN nw_history so the UI can show last_tick + points.
    """
    s = (search or "").strip()

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if s:
                like = f"%{s}%"
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
            rows = cur.fetchall()

        return {"ok": True, "kingdoms": rows}
    finally:
        conn.close()


@router.get("/history/{kingdom}")
def nw_history(kingdom: str, hours: int = 24):
    """
    Returns chart points: [{t: ISO8601, v: networth}, ...]
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    conn = _connect()
    try:
        with conn.cursor() as cur:
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