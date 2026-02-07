import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row

KG_TICK_DELAY_SECONDS = float(os.getenv("KG_TICK_DELAY_SECONDS", "45"))
MAX_SOURCE_AGE_SECONDS = int(os.getenv("NW_MAX_SOURCE_AGE_SECONDS", "540"))
SOURCE_WAIT_TIMEOUT_SECONDS = int(os.getenv("NW_SOURCE_WAIT_TIMEOUT_SECONDS", "120"))
SOURCE_WAIT_STEP_SECONDS = float(os.getenv("NW_SOURCE_WAIT_STEP_SECONDS", "3"))


# -------------------------
# Tick scheduling helpers
# -------------------------
def _next_5min_boundary_utc(now: datetime) -> datetime:
    base = now.replace(second=0, microsecond=0)
    m = (base.minute // 5) * 5
    boundary = base.replace(minute=m)
    if boundary <= now:
        boundary += timedelta(minutes=5)
    return boundary


def _sleep_until(dt: datetime):
    while True:
        now = datetime.now(timezone.utc)
        sec = (dt - now).total_seconds()
        if sec <= 0:
            return
        time.sleep(min(2.0, sec))


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


def _fetch_from_kg_top() -> Tuple[Snapshot, Optional[datetime]]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.kg_top_kingdoms') AS t;")
            reg = cur.fetchone()
            if not reg or not reg.get("t"):
                return ([], None)

            cur.execute("""
                SELECT kingdom, COALESCE(ranking, 999999) AS rank, networth, fetched_at
                FROM public.kg_top_kingdoms
                ORDER BY ranking ASC NULLS LAST
                LIMIT 300;
            """)
            rows = cur.fetchall()

        out: Snapshot = []
        latest_fetched_at: Optional[datetime] = None
        for r in rows:
            k = (r.get("kingdom") or "").strip()
            if not k or r.get("networth") is None:
                continue
            out.append((k, int(r.get("rank") or 999999), int(r["networth"])))
            fa = r.get("fetched_at")
            if isinstance(fa, datetime):
                if latest_fetched_at is None or fa > latest_fetched_at:
                    latest_fetched_at = fa
        return (out, latest_fetched_at)
    finally:
        conn.close()


def _fetch_top300_resilient() -> Tuple[str, Snapshot, Optional[datetime]]:
    rows, fetched_at = _fetch_from_kg_top()
    if rows:
        return ("kg_top_kingdoms", rows, fetched_at)
    return ("none", [], None)


def _is_fresh(source_ts: Optional[datetime], now: datetime) -> bool:
    if source_ts is None:
        return False
    return (now - source_ts).total_seconds() <= MAX_SOURCE_AGE_SECONDS


def _fetch_snapshot_for_tick(tick_time: datetime) -> Tuple[str, Snapshot, Optional[datetime]]:
    """
    Wait briefly for rankings_poller to finish writing this tick's data.
    Accept only snapshots fetched at/after tick_time (UTC 5-min boundary).
    """
    deadline = datetime.now(timezone.utc) + timedelta(seconds=SOURCE_WAIT_TIMEOUT_SECONDS)
    latest_source = "none"
    latest_snapshot: Snapshot = []
    latest_fetched_at: Optional[datetime] = None

    while datetime.now(timezone.utc) <= deadline:
        source, snapshot, source_fetched_at = _fetch_top300_resilient()
        latest_source = source
        latest_snapshot = snapshot
        latest_fetched_at = source_fetched_at

        if snapshot and source_fetched_at is not None and source_fetched_at >= tick_time:
            return (source, snapshot, source_fetched_at)

        time.sleep(max(0.5, SOURCE_WAIT_STEP_SECONDS))

    return (latest_source, latest_snapshot, latest_fetched_at)


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


def start_nw_poller(poll_seconds: int = 300):
    """
    Tick-aligned NW poller:
    - wakes up exactly on :00/:05/:10...
    - waits KG_TICK_DELAY_SECONDS (same as rankings poller)
    - reads kg_top_kingdoms and writes nw_latest + nw_history
    - uses tick boundary time as tick_time (perfect alignment)
    """
    global _POLL_THREAD, _STOP

    if _POLL_THREAD and _POLL_THREAD.is_alive():
        return

    _ensure_tables()
    _STOP = False

    def loop():
        # small boot jitter
        time.sleep(1.0)

        while not _STOP:
            try:
                target = _next_5min_boundary_utc(datetime.now(timezone.utc))
                _sleep_until(target)

                if KG_TICK_DELAY_SECONDS > 0:
                    time.sleep(KG_TICK_DELAY_SECONDS)

                source, snapshot, source_fetched_at = _fetch_snapshot_for_tick(target)

                if not snapshot:
                    print("[nw_poll] source=none no snapshot available")
                else:
                    now = target  # <- align exactly to tick boundary
                    if not _is_fresh(source_fetched_at, now):
                        print(
                            f"[nw_poll] stale source data: source={source} "
                            f"fetched_at={source_fetched_at} tick={now.isoformat()} "
                            f"(max_age={MAX_SOURCE_AGE_SECONDS}s)"
                        )
                        continue
                    if source_fetched_at is None or source_fetched_at < target:
                        print(
                            f"[nw_poll] no in-tick snapshot ready: source={source} "
                            f"fetched_at={source_fetched_at} tick={now.isoformat()} "
                            f"(wait_timeout={SOURCE_WAIT_TIMEOUT_SECONDS}s)"
                        )
                        continue

                    _upsert_latest(snapshot, now)

                    points = [(k, now, nw) for (k, _rank, nw) in snapshot]
                    _upsert_history(points)

                    # Debug Galileo NW each tick
                    gal = next((nw for (k, _r, nw) in snapshot if k == "Galileo"), None)
                    if gal is not None:
                        print(f"[nw_poll] source={source} ok: wrote {len(points)} points @ {now.isoformat()} GalileoNW={gal}")
                    else:
                        print(f"[nw_poll] source={source} ok: wrote {len(points)} points @ {now.isoformat()}")

            except Exception as e:
                print(f"[nw_poll] error: {repr(e)}")

            # poll_seconds kept for compatibility; tick alignment drives the schedule
            _ = poll_seconds

    _POLL_THREAD = threading.Thread(target=loop, daemon=True)
    _POLL_THREAD.start()


def stop_nw_poller():
    global _STOP
    _STOP = True
