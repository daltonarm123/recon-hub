import os
from datetime import datetime, timedelta
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
def nw_kingdoms(limit: int = 300):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kingdom, MAX(tick_time) AS last_tick, COUNT(*)::int AS points
                FROM public.nw_history
                GROUP BY kingdom
                ORDER BY last_tick DESC NULLS LAST
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
    since = datetime.utcnow() - timedelta(hours=hours)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kingdom, networth, tick_time
                FROM public.nw_history
                WHERE kingdom = %s AND tick_time >= %s
                ORDER BY tick_time ASC
                """,
                (kingdom, since),
            )
            rows = cur.fetchall()

        return {"ok": True, "kingdom": kingdom, "hours": hours, "points": rows}
    finally:
        conn.close()
