from fastapi import APIRouter
import psycopg
import os

router = APIRouter()

DB = os.environ.get("DATABASE_URL")

@router.get("/nw/{kingdom}")
def get_nw(kingdom: str):
    with psycopg.connect(DB) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tick_time, networth
                FROM nw_history
                WHERE kingdom = %s
                ORDER BY tick_time ASC
                """,
                (kingdom,)
            )
            rows = cur.fetchall()
    return {
        "kingdom": kingdom,
        "points": [
            {"datetime": r[0].isoformat(), "networth": r[1]} for r in rows
        ]
    }
