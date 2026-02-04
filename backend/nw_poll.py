import os
import json
import time
import threading
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import httpx
import psycopg
from psycopg.rows import dict_row

KG_NWOT_URL = "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetNetworthOverTime"


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
            # ✅ add PK so we don't duplicate ticks
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.nw_history (
                    kingdom_id int NOT NULL,
                    kingdom text NOT NULL,
                    networth bigint NOT NULL,
                    tick_time timestamptz NOT NULL,
                    PRIMARY KEY (kingdom_id, tick_time)
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS nw_history_kingdom_tick_idx
                ON public.nw_history (kingdom_id, tick_time DESC);
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

def _insert_points(kingdom_id: int, kingdom: str, points: List[Dict]):
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

                # ✅ ON CONFLICT DO NOTHING = no dupes
                cur.execute(
                    """
                    INSERT INTO public.nw_history (kingdom_id, kingdom, networth, tick_time)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (kingdom_id, tick_time) DO NOTHING
                    """,
                    (int(kingdom_id), str(kingdom), int(nw), tick_time),
                )
            conn.commit()
    finally:
        conn.close()

def _load_top300_track() -> List[Tuple[str, int]]:
    """
    Reads current top 300 from public.kg_top_kingdoms.
    Returns [(kingdom_name, kingdom_id)].
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kingdom, kingdom_id
                FROM public.kg_top_kingdoms
                ORDER BY ranking ASC NULLS LAST
                LIMIT 300
                """
            )
            rows = cur.fetchall()
        return [(r["kingdom"], int(r["kingdom_id"])) for r in rows]
    except Exception as e:
        print("[nw_poller] load_top300 error:", repr(e))
        return []
    finally:
        conn.close()

def _poll_once(world_id: str, kg_token: str, track: List[Tuple[str, int]]):
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "world-id": str(world_id),
        "Origin": "https://www.kingdomgame.net",
        "Referer": "https://www.kingdomgame.net/rankings",
    }
    if kg_token:
        headers["token"] = kg_token

    with httpx.Client(timeout=30.0) as client:
        for name, kid in track:
            payload = {"kingdomId": int(kid), "hours": 24}
            r = client.post(KG_NWOT_URL, headers=headers, json=payload)
            r.raise_for_status()
            points = _parse_kg_response(r.json())
            _insert_points(kid, name, points)

def start_nw_poller(
    *,
    poll_seconds: int = 240,
    world_id: str = "1",
    kg_token: str = "",
):
    """
    Every poll:
      1) load top-300 from kg_top_kingdoms
      2) pull NWOT for each kingdomId
      3) upsert into nw_history
    """
    _ensure_table()

    def loop():
        while True:
            try:
                track = _load_top300_track()
                if not track:
                    print("[nw_poller] top300 empty — waiting for rankings poller.")
                else:
                    _poll_once(world_id=world_id, kg_token=kg_token, track=track)
            except Exception as e:
                print("[nw_poller] error:", repr(e))
            time.sleep(int(poll_seconds))

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t