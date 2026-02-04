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
def nw_kingdoms(limit: int = 300, world_id: str = "1"):
    """
    Source of truth for the NWOT list = rankings_top300.
    We LEFT JOIN nw_history so the UI can show last_tick + points if we have them.
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
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
                    r.rank,
                    r.kingdom_id,
                    r.kingdom,
                    r.networth,
                    COALESCE(r.alliance, '') AS alliance,
                    r.updated_at,
                    h.last_tick,
                    COALESCE(h.points, 0)::int AS points
                FROM public.rankings_top300 r
                LEFT JOIN hist h
                    ON h.kingdom = r.kingdom
                WHERE r.world_id = %s
                ORDER BY r.rank ASC
                LIMIT %s
                """,
                (str(world_id), limit),
            )
            rows = cur.fetchall()

        return {"ok": True, "world_id": str(world_id), "kingdoms": rows}
    finally:
        conn.close()

@router.get("/history/{kingdom}")
def nw_history(kingdom: str, hours: int = 24):
    # Use aware UTC time to match timestamptz comparisons cleanly.
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

        # Chart-ready: [{t: "...ISO...", v: 123}, ...]
        points = []
        for r in rows:
            tt = r["tick_time"]
            nw = r["networth"]
            if tt is None or nw is None:
                continue
            points.append({"t": tt.isoformat(), "v": int(nw)})

        return points
    finally:
        conn.close()