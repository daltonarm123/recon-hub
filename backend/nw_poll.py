import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row


def _get_dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    return dsn


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_dsn(), row_factory=dict_row)


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


# Snapshot is (kingdom, rank, networth)
Snapshot = List[Tuple[str, int, int]]


def _fetch_from_kg_top() -> Snapshot:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.kg_top_kingdoms') AS t;")
            reg = cur.fetchone()
            if not reg or not reg.get("t"):
                return []

            cur.execute("""
                SELECT kingdom, COALESCE(ranking, 999999) AS rank, networth
                FROM public.kg_top_kingdoms
                ORDER BY ranking ASC NULLS LAST
                LIMIT 300;
            """)
            rows = cur.fetchall()

        out: Snapshot = []
        for r in rows:
            k = (r.get("kingdom") or "").strip()
            if not k or r.get("networth") is None:
                continue
            out.append((k, int(r.get("rank") or 999999), int(r["networth"])))
        return out
    finally:
        conn.close()


def _fetch_from_latest() -> Snapshot:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.nw_latest') AS t;")
            reg = cur.fetchone()
            if not reg or not reg.get("t"):
                return []

            cur.execute("""
                SELECT kingdom, COALESCE(rank, 999999) AS rank, networth
                FROM public.nw_latest
                ORDER BY networth DESC
                LIMIT 300;
            """)
            rows = cur.fetchall()

        out: Snapshot = []
        for r in rows:
            k = (r.get("kingdom") or "").strip()
            if not k or r.get("networth") is None:
                continue
            out.append((k, int(r.get("rank") or 999999), int(r["networth"])))
        return out
    finally:
        conn.close()


def _fetch_from_history() -> Snapshot:
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
                SELECT kingdom, 999999 AS rank, networth
                FROM latest
                ORDER BY networth DESC
                LIMIT 300;
            """)
            rows = cur.fetchall()

        out: Snapshot = []
        for r in rows:
            k = (r.get("kingdom") or "").strip()
            if not k or r.get("networth") is None:
                continue
            out.append((k, 999999, int(r["networth"])))
        return out
    finally:
        conn.close()


def _fetch_top300_resilient() -> Tuple[str, Snapshot]:
    rows = _fetch_from_kg_top()
    if rows:
        return ("kg_top_kingdoms", rows)

    rows = _fetch_from_latest()
    if rows:
        return ("nw_latest", rows)

    rows = _fetch_from_history()
    if rows:
        return ("nw_history", rows)

    return ("none", [])


def _upsert_history(points: List[Tuple[str, datetime, int]]):
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
            """, points)
        conn.commit()
    finally:
        conn.close()


def _upsert_latest(snapshot: Snapshot, now: datetime):
    if not snapshot:
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO public.nw_latest (kingdom, rank, networth, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (kingdom)
                DO UPDATE SET
                    rank = EXCLUDED.rank,
                    networth = EXCLUDED.networth,
                    updated_at = EXCLUDED.updated_at;
            """, [(k, rank, nw, now) for (k, rank, nw) in snapshot])
        conn.commit()
    finally:
        conn.close()


_POLL_THREAD: Optional[threading.Thread] = None
_STOP = False


def start_nw_poller(poll_seconds: int = 240):
    global _POLL_THREAD, _STOP

    if _POLL_THREAD and _POLL_THREAD.is_alive():
        return

    _ensure_tables()
    _STOP = False

    def loop():
        while not _STOP:
            try:
                source, snapshot = _fetch_top300_resilient()

                if not snapshot:
                    print("[nw_poll] source=none no snapshot available")
                else:
                    now = datetime.now(timezone.utc)

                    _upsert_latest(snapshot, now)

                    points = [(k, now, nw) for (k, _rank, nw) in snapshot]
                    _upsert_history(points)

                    print(f"[nw_poll] source={source} ok: wrote {len(points)} points @ {now.isoformat()}")

            except Exception as e:
                print(f"[nw_poll] error: {repr(e)}")

            time.sleep(max(30, int(poll_seconds)))

    _POLL_THREAD = threading.Thread(target=loop, daemon=True)
    _POLL_THREAD.start()


def stop_nw_poller():
    global _STOP
    _STOP = True