import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row


# -------------------------
# DB helpers
# -------------------------
def _get_dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    return dsn


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_dsn(), row_factory=dict_row)


# -------------------------
# Ensure tables
# -------------------------
def _ensure_tables():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.nw_history (
                    kingdom   TEXT NOT NULL,
                    tick_time TIMESTAMPTZ NOT NULL,
                    networth  BIGINT NOT NULL,
                    PRIMARY KEY (kingdom, tick_time)
                );
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS ix_nw_history_kingdom_time
                ON public.nw_history (kingdom, tick_time DESC);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.nw_latest (
                    kingdom    TEXT PRIMARY KEY,
                    rank       INT NOT NULL DEFAULT 999999,
                    networth   BIGINT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
            """)
        conn.commit()
    finally:
        conn.close()


# -------------------------
# Snapshot Sources
# -------------------------
def _fetch_top300_from_kg_cache() -> List[Tuple[str, int]]:
    """
    Primary source: rankings poller snapshot table
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.kg_top_kingdoms') AS t;")
            reg = cur.fetchone()
            if not reg or not reg.get("t"):
                return []

            cur.execute("""
                SELECT kingdom, networth
                FROM public.kg_top_kingdoms
                ORDER BY ranking ASC NULLS LAST
                LIMIT 300;
            """)
            rows = cur.fetchall()

        return [
            (r["kingdom"].strip(), int(r["networth"]))
            for r in rows
            if r.get("kingdom") and r.get("networth") is not None
        ]
    finally:
        conn.close()


def _fetch_top300_from_latest() -> List[Tuple[str, int]]:
    """
    Fallback: nw_latest snapshot table
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.nw_latest') AS t;")
            reg = cur.fetchone()
            if not reg or not reg.get("t"):
                return []

            cur.execute("""
                SELECT kingdom, networth
                FROM public.nw_latest
                ORDER BY networth DESC
                LIMIT 300;
            """)
            rows = cur.fetchall()

        return [
            (r["kingdom"].strip(), int(r["networth"]))
            for r in rows
            if r.get("kingdom") and r.get("networth") is not None
        ]
    finally:
        conn.close()


def _fetch_top300_from_history() -> List[Tuple[str, int]]:
    """
    Last fallback: derive latest per kingdom from nw_history
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH latest AS (
                    SELECT DISTINCT ON (kingdom)
                        kingdom,
                        networth,
                        tick_time
                    FROM public.nw_history
                    ORDER BY kingdom, tick_time DESC
                )
                SELECT kingdom, networth
                FROM latest
                ORDER BY networth DESC
                LIMIT 300;
            """)
            rows = cur.fetchall()

        return [
            (r["kingdom"].strip(), int(r["networth"]))
            for r in rows
            if r.get("kingdom") and r.get("networth") is not None
        ]
    finally:
        conn.close()


def _fetch_top300_resilient() -> List[Tuple[str, int]]:
    rows = _fetch_top300_from_kg_cache()
    if rows:
        return rows

    rows = _fetch_top300_from_latest()
    if rows:
        return rows

    return _fetch_top300_from_history()


# -------------------------
# Writers
# -------------------------
def _upsert_history(points: List[Dict[str, Any]]):
    if not points:
        return

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO public.nw_history (kingdom, tick_time, networth)
                VALUES (%s, %s, %s)
                ON CONFLICT (kingdom, tick_time)
                DO UPDATE SET networth = EXCLUDED.networth;
            """, [
                (p["kingdom"], p["tick_time"], p["networth"])
                for p in points
            ])
        conn.commit()
    finally:
        conn.close()


def _upsert_latest(snapshot: List[Tuple[str, int]]):
    now = datetime.now(timezone.utc)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO public.nw_latest (kingdom, rank, networth, updated_at)
                VALUES (%s, 999999, %s, %s)
                ON CONFLICT (kingdom)
                DO UPDATE SET
                    networth = EXCLUDED.networth,
                    updated_at = EXCLUDED.updated_at;
            """, [
                (k, nw, now)
                for (k, nw) in snapshot
            ])
        conn.commit()
    finally:
        conn.close()


# -------------------------
# Poll Loop
# -------------------------
_POLL_THREAD: Optional[threading.Thread] = None
_STOP = False


def start_nw_poller(poll_seconds: int, world_id: str, kg_token: str):
    global _POLL_THREAD, _STOP

    if _POLL_THREAD and _POLL_THREAD.is_alive():
        return

    _ensure_tables()
    _STOP = False

    def loop():
        while not _STOP:
            try:
                snapshot = _fetch_top300_resilient()

                if not snapshot:
                    print("[nw_poll] no snapshot available")
                else:
                    now = datetime.now(timezone.utc)

                    _upsert_latest(snapshot)

                    points = [
                        {
                            "kingdom": k,
                            "tick_time": now,
                            "networth": nw
                        }
                        for (k, nw) in snapshot
                    ]

                    _upsert_history(points)

                    print(f"[nw_poll] ok: wrote {len(points)} points @ {now.isoformat()}")

            except Exception as e:
                print(f"[nw_poll] error: {repr(e)}")

            time.sleep(max(30, int(poll_seconds)))

    _POLL_THREAD = threading.Thread(target=loop, daemon=True)
    _POLL_THREAD.start()


def stop_nw_poller():
    global _STOP
    _STOP = True