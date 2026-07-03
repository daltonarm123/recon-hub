import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row

from db_dsn import resolve_database_dsn

MAX_SOURCE_AGE_SECONDS = int(os.getenv("NW_MAX_SOURCE_AGE_SECONDS", "540"))


def _get_dsn() -> str:
    return resolve_database_dsn()


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_dsn(), row_factory=dict_row)


def _ensure_tables():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.nw_history (
                    kingdom_id BIGINT NOT NULL,
                    kingdom   TEXT NOT NULL,
                    tick_time TIMESTAMPTZ NOT NULL,
                    networth  BIGINT NOT NULL,
                    PRIMARY KEY (kingdom_id, tick_time)
                );
            """)

            cur.execute("""
                ALTER TABLE public.nw_history
                ADD COLUMN IF NOT EXISTS kingdom_id BIGINT;
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS ix_nw_history_kingdom_id_time
                ON public.nw_history (kingdom_id, tick_time DESC);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.nw_latest (
                    kingdom_id BIGINT PRIMARY KEY,
                    kingdom    TEXT NOT NULL,
                    rank       INT NOT NULL DEFAULT 999999,
                    networth   BIGINT NOT NULL,
                    delta      BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
            """)

            cur.execute("""
                ALTER TABLE public.nw_latest
                ADD COLUMN IF NOT EXISTS kingdom_id BIGINT;
            """)

            cur.execute("""
                ALTER TABLE public.nw_latest
                ADD COLUMN IF NOT EXISTS kingdom TEXT;
            """)

            cur.execute("""
                ALTER TABLE public.nw_latest
                ADD COLUMN IF NOT EXISTS delta BIGINT NOT NULL DEFAULT 0;
            """)
        conn.commit()
    finally:
        conn.close()


# Snapshot is (kingdom_id, kingdom, rank, networth)
Snapshot = List[Tuple[int, str, int, int]]


def _fetch_from_kg_top() -> Tuple[Snapshot, Optional[datetime]]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.kg_top_kingdoms') AS t;")
            reg = cur.fetchone()
            if not reg or not reg.get("t"):
                return ([], None)

            cur.execute("""
                SELECT kingdom_id, kingdom, COALESCE(ranking, 999999) AS rank, networth, fetched_at
                FROM public.kg_top_kingdoms
                ORDER BY ranking ASC NULLS LAST
                LIMIT 100;
            """)
            rows = cur.fetchall()

        out: Snapshot = []
        latest_fetched_at: Optional[datetime] = None
        for r in rows:
            kid = r.get("kingdom_id")
            k = (r.get("kingdom") or "").strip()
            if kid is None or not k or r.get("networth") is None:
                continue
            out.append((int(kid), k, int(r.get("rank") or 999999), int(r["networth"])))
            fa = r.get("fetched_at")
            if isinstance(fa, datetime):
                if latest_fetched_at is None or fa > latest_fetched_at:
                    latest_fetched_at = fa
        return (out, latest_fetched_at)
    finally:
        conn.close()


def _fetch_top_resilient() -> Tuple[str, Snapshot, Optional[datetime]]:
    rows, fetched_at = _fetch_from_kg_top()
    if rows:
        return ("kg_top_kingdoms", rows, fetched_at)
    return ("none", [], None)


def _is_fresh(source_ts: Optional[datetime], now: datetime) -> bool:
    if source_ts is None:
        return False
    return (now - source_ts).total_seconds() <= MAX_SOURCE_AGE_SECONDS


def _fetch_snapshot_for_tick(_tick_time: datetime) -> Tuple[str, Snapshot, Optional[datetime]]:
    return _fetch_top_resilient()


def _upsert_history(points: List[Tuple[int, str, datetime, int]]):
    if not points:
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO public.nw_history (kingdom_id, kingdom, tick_time, networth)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (kingdom_id, tick_time)
                DO UPDATE SET networth = EXCLUDED.networth;
            """, points)
        conn.commit()
    finally:
        conn.close()


def _fetch_previous_nw_by_kingdom_id() -> Dict[int, int]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT kingdom_id, networth FROM public.nw_latest")
            rows = cur.fetchall()
        return {int(r["kingdom_id"]): int(r["networth"]) for r in rows if r.get("kingdom_id") is not None and r.get("networth") is not None}
    finally:
        conn.close()


def _calculate_deltas(snapshot: Snapshot, previous_networth: Dict[int, int]) -> List[Tuple[int, str, int, int, int]]:
    out: List[Tuple[int, str, int, int, int]] = []
    for kingdom_id, kingdom, rank, networth in snapshot:
        prior = previous_networth.get(int(kingdom_id))
        delta = int(networth) - int(prior) if prior is not None else 0
        out.append((int(kingdom_id), kingdom, int(rank), int(networth), int(delta)))
    return out


def _upsert_latest(snapshot_with_delta: List[Tuple[int, str, int, int, int]], now: datetime):
    if not snapshot_with_delta:
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO public.nw_latest (kingdom_id, kingdom, rank, networth, delta, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (kingdom_id)
                DO UPDATE SET
                    kingdom = EXCLUDED.kingdom,
                    rank = EXCLUDED.rank,
                    networth = EXCLUDED.networth,
                    delta = EXCLUDED.delta,
                    updated_at = EXCLUDED.updated_at;
            """, [(kid, k, rank, nw, d, now) for (kid, k, rank, nw, d) in snapshot_with_delta])
        conn.commit()
    finally:
        conn.close()


_POLL_THREAD: Optional[threading.Thread] = None
_STOP = False


def start_nw_poller(poll_seconds: int = 60):
    """Interval NW poller driven by rankings snapshots from kg_top_kingdoms."""
    global _POLL_THREAD, _STOP

    if _POLL_THREAD and _POLL_THREAD.is_alive():
        return

    _ensure_tables()
    _STOP = False

    def loop():
        time.sleep(1.0)

        interval_seconds = max(15, int(poll_seconds or 60))

        while not _STOP:
            try:
                target = datetime.now(timezone.utc)

                source, snapshot, source_fetched_at = _fetch_snapshot_for_tick(target)

                if not snapshot:
                    print("[nw_poll] source=none no snapshot available")
                else:
                    now = datetime.now(timezone.utc)
                    if not _is_fresh(source_fetched_at, now):
                        print(
                            f"[nw_poll] stale source data: source={source} "
                            f"fetched_at={source_fetched_at} tick={now.isoformat()} "
                            f"(max_age={MAX_SOURCE_AGE_SECONDS}s)"
                        )
                        continue
                    previous_map = _fetch_previous_nw_by_kingdom_id()
                    snapshot_with_delta = _calculate_deltas(snapshot, previous_map)
                    _upsert_latest(snapshot_with_delta, now)

                    points = [(kid, k, now, nw) for (kid, k, _rank, nw) in snapshot]
                    _upsert_history(points)

                    gal = next((nw for (_kid, k, _r, nw) in snapshot if k == "Galileo"), None)
                    if gal is not None:
                        print(f"[nw_poll] source={source} ok: wrote {len(points)} points @ {now.isoformat()} GalileoNW={gal}")
                    else:
                        print(f"[nw_poll] source={source} ok: wrote {len(points)} points @ {now.isoformat()}")

            except Exception as e:
                print(f"[nw_poll] error: {repr(e)}")

            time.sleep(interval_seconds)

    _POLL_THREAD = threading.Thread(target=loop, daemon=True)
    _POLL_THREAD.start()


def stop_nw_poller():
    global _STOP
    _STOP = True
