import os
import time
import json
import threading
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.rows import dict_row
import requests


KG_NWOT_URL = "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetNetworthOverTime"


def _get_dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    return dsn


def _connect():
    return psycopg.connect(_get_dsn(), row_factory=dict_row)


def _ensure_table():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.nw_history (
                    kingdom text NOT NULL,
                    networth bigint NOT NULL,
                    tick_time timestamptz NOT NULL,
                    PRIMARY KEY (kingdom, tick_time)
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS nw_history_kingdom_time_idx ON public.nw_history (kingdom, tick_time DESC);"
            )
        conn.commit()
    finally:
        conn.close()


def _fetch_nwot(session: requests.Session, cookies: dict) -> list[dict]:
    # KG returns: {"d": "{\"dataPoints\":[...]}"} (string JSON inside JSON)
    r = session.post(
        KG_NWOT_URL,
        json={},  # KG endpoint doesn't need a body for "current kingdom" context
        headers={"Accept": "application/json"},
        cookies=cookies,
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    d = payload.get("d")

    # d is a JSON string
    if isinstance(d, str):
        inner = json.loads(d)
    else:
        inner = d or {}

    return inner.get("dataPoints", []) or []


def _upsert_points(kingdom: str, points: list[dict]):
    if not kingdom or not points:
        return

    conn = _connect()
    try:
        with conn.cursor() as cur:
            for p in points:
                nw = p.get("networth")
                dt = p.get("datetime")
                if nw is None or not dt:
                    continue

                # dt looks like "2026-02-03T18:25:15" (no timezone)
                # treat as UTC (or "offset" per Paul) — adjust later if needed.
                try:
                    tick_time = datetime.fromisoformat(dt).replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                cur.execute(
                    """
                    INSERT INTO public.nw_history (kingdom, networth, tick_time)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (kingdom, tick_time) DO UPDATE
                    SET networth = EXCLUDED.networth
                    """,
                    (kingdom, int(nw), tick_time),
                )
        conn.commit()
    finally:
        conn.close()


def _poll_loop(kingdoms: list[str], poll_seconds: int, cookies: dict):
    _ensure_table()
    s = requests.Session()

    while True:
        start = time.time()
        for k in kingdoms:
            try:
                # We need kingdom context; easiest: use a separate cookies per kingdom if you have it.
                # If your dummy account can switch viewed kingdom server-side, you’ll replace this later.
                points = _fetch_nwot(s, cookies=cookies)
                _upsert_points(k, points)
            except Exception as e:
                # don’t crash the process because one kingdom failed
                print(f"[nw_poll] error kingdom={k}: {e}")

        elapsed = time.time() - start
        sleep_for = max(5, poll_seconds - elapsed)
        time.sleep(sleep_for)


_thread = None


def start_nw_poller(kingdoms: list[str], poll_seconds: int, cookies: dict):
    """
    Starts background thread that polls KG and stores points into nw_history.
    """
    global _thread
    if _thread and _thread.is_alive():
        return

    _thread = threading.Thread(
        target=_poll_loop,
        args=(kingdoms, poll_seconds, cookies),
        daemon=True,
        name="nw_poller",
    )
    _thread.start()
