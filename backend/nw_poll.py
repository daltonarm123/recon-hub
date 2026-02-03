import os
import json
import time
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx
import psycopg
from psycopg.rows import dict_row

KG_NWOT_URL = "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetNetworthOverTime"

# TEMP: put a few known ids here to prove DB writes work.
# Replace this once you capture the Rankings endpoint response that includes kingdomId.
DEFAULT_TRACK: List[Tuple[str, int]] = [
    ("Galileo", 3334),
]

def _dsn() -> str:
    return os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""

def _connect():
    dsn = _dsn()
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(dsn, row_factory=dict_row)

def _parse_kg_response(resp_json: Dict) -> List[Dict]:
    """
    KG returns: {"d": "{\"dataPoints\":[...]}"}  (stringified JSON inside "d")
    """
    d = resp_json.get("d")
    if not d:
        return []
    try:
        inner = json.loads(d)
    except Exception:
        return []
    return inner.get("dataPoints") or []

def _ensure_table():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.nw_history (
                    kingdom text NOT NULL,
                    networth bigint NOT NULL,
                    tick_time timestamptz NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS nw_history_kingdom_tick_idx
                ON public.nw_history (kingdom, tick_time DESC);
                """
            )
            conn.commit()
    finally:
        conn.close()

def _insert_points(kingdom: str, points: List[Dict]):
    if not points:
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            for p in points:
                nw = p.get("networth")
                dt = p.get("datetime")
                if nw is None or not dt:
                    continue

                # dt is like "2026-02-03T18:25:15" (no timezone)
                # We'll treat it as UTC unless you later confirm it's server-local.
                tick_time = datetime.fromisoformat(dt).replace(tzinfo=timezone.utc)

                cur.execute(
                    """
                    INSERT INTO public.nw_history (kingdom, networth, tick_time)
                    VALUES (%s, %s, %s)
                    """,
                    (kingdom, int(nw), tick_time),
                )
            conn.commit()
    finally:
        conn.close()

def _poll_once(world_id: str, kg_token: str, track: List[Tuple[str, int]]):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "world-id": str(world_id),
        "Origin": "https://www.kingdomgame.net",
        "Referer": "https://www.kingdomgame.net/rankings",
    }

    # If KG endpoints accept token header, we’ll add it. (Harmless if ignored.)
    if kg_token:
        headers["token"] = kg_token

    with httpx.Client(timeout=30.0) as client:
        for name, kid in track:
            payload = {"kingdomId": kid, "hours": 24}
            r = client.post(KG_NWOT_URL, headers=headers, json=payload)
            r.raise_for_status()
            points = _parse_kg_response(r.json())
            _insert_points(name, points)

def start_nw_poller(poll_seconds: int, world_id: str, kg_token: str):
    """
    Starts a background thread that polls NWOT every poll_seconds.
    Safe to call once at startup.
    """
    _ensure_table()

    def loop():
        while True:
            try:
                _poll_once(world_id=world_id, kg_token=kg_token, track=DEFAULT_TRACK)
            except Exception as e:
                # Don't crash the app if KG is down; just log
                print("[nw_poller] error:", repr(e))
            time.sleep(poll_seconds)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t
