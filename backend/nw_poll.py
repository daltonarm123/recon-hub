import os
import json
import time
import threading
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import httpx
import psycopg
from psycopg.rows import dict_row

KG_NWOT_URL = "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetNetworthOverTime"

# TEMP list so you can prove end-to-end works immediately.
# We'll replace this with "top 300 from rankings" once we wire rankings endpoint.
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

def _parse_kg_response(resp_json: Dict) -> List[Dict]:
    d = resp_json.get("d")
    if not d:
        return []
    try:
        inner = json.loads(d)
    except Exception:
        return []
    return inner.get("dataPoints") or []

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

    # Add token if provided (might be required for some endpoints; NWOT might ignore it)
    if kg_token:
        headers["token"] = kg_token

    with httpx.Client(timeout=30.0) as client:
        for name, kid in track:
            payload = {"kingdomId": kid, "hours": 24}
            r = client.post(KG_NWOT_URL, headers=headers, json=payload)
            r.raise_for_status()
            points = _parse_kg_response(r.json())
            _insert_points(name, points)

def start_nw_poller():
    """
    Starts a background thread.
    Configured via env vars:
      - NW_POLL_SECONDS (default 240)
      - KG_WORLD_ID (default 1)
      - KG_TOKEN (optional)
    """
    _ensure_table()

    poll_seconds = int(os.getenv("NW_POLL_SECONDS", "240"))
    world_id = os.getenv("KG_WORLD_ID", "1")
    kg_token = os.getenv("KG_TOKEN", "")

    def loop():
        while True:
            try:
                _poll_once(world_id=world_id, kg_token=kg_token, track=DEFAULT_TRACK)
            except Exception as e:
                print("[nw_poller] error:", repr(e))
            time.sleep(poll_seconds)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t
