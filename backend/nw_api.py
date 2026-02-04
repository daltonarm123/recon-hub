import os
from datetime import datetime, timedelta, timezone

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
def nw_kingdoms(limit: int = 300):
    """
    ✅ Option A:
    Return the accurate Top 300 list from public.kg_top_kingdoms
    (filled by rankings_poll.py).
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    kingdom,
                    kingdom_id,
                    alliance,
                    ranking,
                    networth,
                    fetched_at
                FROM public.kg_top_kingdoms
                ORDER BY ranking ASC NULLS LAST
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
    Chart-ready points: [{t: ISO, v: networth}, ...]
    Reads from nw_history which is updated by nw_poll.py.
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
            tt = r["tick_time"]
            nw = r["networth"]
            if tt is None or nw is None:
                continue
            points.append({"t": tt.isoformat(), "v": int(nw)})

        return points
    finally:
        conn.close()