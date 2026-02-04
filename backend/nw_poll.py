import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import httpx
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


def _ensure_table():
    """
    Ensure nw_history exists with the schema we actually use:
      kingdom (text)
      tick_time (timestamptz)
      networth (bigint)

    IMPORTANT: No kingdom_id anywhere.
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
            # Helpful index for "last X hours for a kingdom"
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_nw_history_kingdom_time
                ON public.nw_history (kingdom, tick_time DESC);
                """
            )
        conn.commit()
    finally:
        conn.close()


def _upsert_points(points: List[Dict[str, Any]]):
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


# -------------------------
# KG fetch
# -------------------------
def _fetch_rankings_top300(world_id: str, kg_token: str) -> List[Dict[str, Any]]:
    """
    This expects your 'rankings_poll.py' is already pulling the accurate top 300.
    Option A: read from a local cache table created by rankings_poll.py IF it exists.
    If not, fall back to calling KG directly (you can swap the URL to your real endpoint).

    NOTE: Since your exact KG endpoint may differ, the DB-cache path is the safest.
    """
    # 1) Prefer DB cache table from rankings_poll if present
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # If rankings_poll created this table, we use it.
            cur.execute(
                """
                SELECT to_regclass('public.rankings_top300') AS t;
                """
            )
            reg = cur.fetchone()
            if reg and reg.get("t"):
                cur.execute(
                    """
                    SELECT kingdom, networth
                    FROM public.rankings_top300
                    ORDER BY networth DESC NULLS LAST
                    LIMIT 300;
                    """
                )
                rows = cur.fetchall()
                out = []
                for r in rows:
                    k = (r.get("kingdom") or "").strip()
                    nw = r.get("networth")
                    if not k or nw is None:
                        continue
                    out.append({"kingdom": k, "networth": int(nw)})
                return out
    finally:
        conn.close()

    # 2) Fallback: call KG directly (update URL if needed)
    # If you already have rankings_poll working, you might never hit this.
    url = os.getenv("KG_RANKINGS_URL", "").strip()
    if not url:
        # If there is no cache table and no URL configured, return empty
        return []

    headers = {}
    if kg_token:
        headers["Authorization"] = f"Bearer {kg_token}"

    with httpx.Client(timeout=20) as client:
        r = client.get(url, headers=headers, params={"world": world_id, "limit": 300})
        r.raise_for_status()
        data = r.json()

    # Try common shapes
    items = data.get("kingdoms") if isinstance(data, dict) else data
    if not isinstance(items, list):
        return []

    out = []
    for it in items[:300]:
        k = (it.get("kingdom") or it.get("name") or "").strip()
        nw = it.get("networth") or it.get("nw")
        if not k or nw is None:
            continue
        out.append({"kingdom": k, "networth": int(nw)})
    return out


# -------------------------
# Poll loop
# -------------------------
_POLL_THREAD: Optional[threading.Thread] = None
_STOP = False


def start_nw_poller(poll_seconds: int, world_id: str, kg_token: str):
    """
    Start background thread that:
    - Ensures nw_history table
    - Every poll_seconds:
        - gets top300 kingdoms (from rankings cache)
        - inserts a new point for each kingdom at current tick_time
    """
    global _POLL_THREAD, _STOP
    if _POLL_THREAD and _POLL_THREAD.is_alive():
        return

    _ensure_table()
    _STOP = False

    def loop():
        while not _STOP:
            try:
                now = datetime.now(timezone.utc)
                top = _fetch_rankings_top300(world_id=world_id, kg_token=kg_token)

                points = []
                for it in top:
                    points.append(
                        {
                            "kingdom": it["kingdom"],
                            "tick_time": now,
                            "networth": int(it["networth"]),
                        }
                    )

                _upsert_points(points)
            except Exception as e:
                # Don't crash the whole app; just log and keep trying
                print(f"[nw_poll] error: {e}")
            time.sleep(max(30, int(poll_seconds)))

    _POLL_THREAD = threading.Thread(target=loop, daemon=True)
    _POLL_THREAD.start()


def stop_nw_poller():
    global _STOP
    _STOP = True