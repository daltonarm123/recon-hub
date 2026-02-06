import os
import threading
import time
import random
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


def _ensure_tables():
    """
    Ensures the NW tables exist with the schema your app actually uses.

    public.nw_history:
      kingdom (text)
      tick_time (timestamptz)
      networth (bigint)

    public.nw_latest:
      kingdom (text, pk)
      rank (int)          -- placeholder ok
      networth (bigint)
      updated_at (timestamptz)

    public.nw_meta:
      key (text, pk)
      value (text)
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.nw_history (
                    kingdom   TEXT NOT NULL,
                    tick_time TIMESTAMPTZ NOT NULL,
                    networth  BIGINT NOT NULL,
                    PRIMARY KEY (kingdom, tick_time)
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_nw_history_kingdom_time
                ON public.nw_history (kingdom, tick_time DESC);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.nw_latest (
                    kingdom    TEXT PRIMARY KEY,
                    rank       INT NOT NULL DEFAULT 999999,
                    networth   BIGINT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_nw_latest_networth
                ON public.nw_latest (networth DESC);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.nw_meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
        conn.commit()
    finally:
        conn.close()


def _set_meta(key: str, value: str):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.nw_meta(key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, value),
            )
        conn.commit()
    finally:
        conn.close()


def _upsert_history_points(points: List[Dict[str, Any]]):
    """
    points = [{"kingdom": str, "tick_time": datetime, "networth": int}, ...]
    """
    if not points:
        return

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO public.nw_history (kingdom, tick_time, networth)
                VALUES (%s, %s, %s)
                ON CONFLICT (kingdom, tick_time)
                DO UPDATE SET networth = EXCLUDED.networth;
                """,
                [(p["kingdom"], p["tick_time"], int(p["networth"])) for p in points],
            )
        conn.commit()
    finally:
        conn.close()


def _upsert_latest_snapshot(rows: List[Tuple[str, int]]):
    """
    rows = [(kingdom, networth), ...]
    """
    if not rows:
        return
    now = datetime.now(timezone.utc)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO public.nw_latest (kingdom, rank, networth, updated_at)
                VALUES (%s, 999999, %s, %s)
                ON CONFLICT (kingdom) DO UPDATE SET
                    networth = EXCLUDED.networth,
                    updated_at = EXCLUDED.updated_at
                """,
                [(k, int(nw), now) for (k, nw) in rows],
            )
        conn.commit()
    finally:
        conn.close()


# -------------------------
# Source fetch (DB snapshot)
# -------------------------
def _fetch_top300_from_kg_cache() -> List[Tuple[str, int]]:
    """
    Reads top 300 from rankings poller table.
    rankings_poll.py writes to public.kg_top_kingdoms. :contentReference[oaicite:1]{index=1}
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.kg_top_kingdoms') AS t;")
            reg = cur.fetchone()
            if not reg or not reg.get("t"):
                return []

            cur.execute(
                """
                SELECT kingdom, networth
                FROM public.kg_top_kingdoms
                ORDER BY ranking ASC NULLS LAST
                LIMIT 300;
                """
            )
            rows = cur.fetchall()

        out: List[Tuple[str, int]] = []
        for r in rows:
            k = (r.get("kingdom") or "").strip()
            nw = r.get("networth")
            if not k or nw is None:
                continue
            out.append((k, int(nw)))
        return out
    finally:
        conn.close()


def _fetch_top300_from_latest_fallback() -> List[Tuple[str, int]]:
    """
    Fallback if KG cache is empty/stale. Keeps charts updating even when KG 500s.
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.nw_latest') AS t;")
            reg = cur.fetchone()
            if not reg or not reg.get("t"):
                return []

            cur.execute(
                """
                SELECT kingdom, networth
                FROM public.nw_latest
                ORDER BY networth DESC
                LIMIT 300;
                """
            )
            rows = cur.fetchall()

        out: List[Tuple[str, int]] = []
        for r in rows:
            k = (r.get("kingdom") or "").strip()
            nw = r.get("networth")
            if not k or nw is None:
                continue
            out.append((k, int(nw)))
        return out
    finally:
        conn.close()


def _fetch_top300_resilient() -> List[Tuple[str, int]]:
    """
    Try kg_top_kingdoms first, then fallback to nw_latest.
    Includes light retries for transient DB issues.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            rows = _fetch_top300_from_kg_cache()
            if rows:
                return rows
            rows = _fetch_top300_from_latest_fallback()
            return rows
        except Exception as e:
            last_err = e
            # small exponential backoff with jitter
            sleep_s = min(2 ** (attempt - 1), 4) + random.uniform(0.0, 0.5)
            time.sleep(sleep_s)

    if last_err:
        raise last_err
    return []


# -------------------------
# Poll loop
# -------------------------
_POLL_THREAD: Optional[threading.Thread] = None
_STOP = False


def start_nw_poller(poll_seconds: int, world_id: str, kg_token: str):
    """
    Every poll_seconds (default 240):
      - read top-300 snapshot from DB (kg_top_kingdoms)
      - write a NW point for each kingdom into nw_history at 'now'
      - also upsert nw_latest snapshot (keeps UI list alive)
    """
    global _POLL_THREAD, _STOP
    if _POLL_THREAD and _POLL_THREAD.is_alive():
        return

    _ensure_tables()
    _STOP = False

    def loop():
        while not _STOP:
            cycle_started = datetime.now(timezone.utc)
            try:
                top = _fetch_top300_resilient()

                if not top:
                    # No snapshot available; record meta and try again later.
                    _set_meta("nw_last_error_at", cycle_started.isoformat())
                    _set_meta("nw_last_error", "No top-300 snapshot available (kg_top_kingdoms empty and nw_latest empty).")
                    print("[nw_poll] warning: no top-300 snapshot available; skipping cycle")
                else:
                    now = datetime.now(timezone.utc)

                    # Maintain latest snapshot table (helps UI even during KG outages)
                    _upsert_latest_snapshot(top)

                    # Write history points
                    points = [{"kingdom": k, "tick_time": now, "networth": nw} for (k, nw) in top]
                    _upsert_history_points(points)

                    _set_meta("nw_last_success_at", now.isoformat())
                    _set_meta("nw_last_error", "")
                    _set_meta("nw_last_error_at", "")

                    print(f"[nw_poll] ok: wrote {len(points)} points @ {now.isoformat()}")

            except Exception as e:
                try:
                    _set_meta("nw_last_error_at", cycle_started.isoformat())
                    _set_meta("nw_last_error", repr(e))
                except Exception:
                    pass
                print(f"[nw_poll] error: {repr(e)}")

            time.sleep(max(30, int(poll_seconds)))

    _POLL_THREAD = threading.Thread(target=loop, daemon=True)
    _POLL_THREAD.start()


def stop_nw_poller():
    global _STOP
    _STOP = True