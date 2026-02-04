import os
import json
import time
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx
import psycopg
from psycopg.rows import dict_row


# ✅ IMPORTANT:
# This URL must match what the KG rankings page calls in Network tab.
# If this exact endpoint name differs, just update this constant.
KG_RANKINGS_URL = os.getenv(
    "KG_RANKINGS_URL",
    "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetRankings"
)

def _dsn() -> str:
    return os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""

def _connect():
    dsn = _dsn()
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(dsn, row_factory=dict_row)

def _ensure_tables():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # Stores latest known top-300 snapshot (we overwrite/upsert these rows)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.kg_top_kingdoms (
                    kingdom_id   int PRIMARY KEY,
                    kingdom      text NOT NULL,
                    alliance     text,
                    ranking      int,
                    networth     bigint,
                    fetched_at   timestamptz NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS kg_top_kingdoms_rank_idx
                ON public.kg_top_kingdoms (ranking ASC NULLS LAST);
                """
            )
            conn.commit()
    finally:
        conn.close()

def _parse_kg_d_json(resp_json: Dict) -> Dict:
    """
    KG often returns: {"d": "<stringified json>"}.
    This normalizes to a dict.
    """
    d = resp_json.get("d")
    if not d:
        return {}
    try:
        return json.loads(d)
    except Exception:
        return {}

def _extract_top_kingdoms(payload: Dict, limit: int = 300) -> List[Dict]:
    """
    We try to be resilient to whatever key names KG uses.
    Typical shapes include: {"data": [...]} or {"kingdoms":[...]} etc.
    """
    # find the first list in common keys
    candidates = [
        payload.get("data"),
        payload.get("kingdoms"),
        payload.get("rankings"),
        payload.get("items"),
        payload.get("results"),
    ]
    rows = next((c for c in candidates if isinstance(c, list)), None)
    if rows is None and isinstance(payload, list):
        rows = payload
    if not isinstance(rows, list):
        return []

    out = []
    for r in rows[:limit]:
        if not isinstance(r, dict):
            continue

        # Try multiple possible field names
        kid = r.get("kingdomId") or r.get("kingdom_id") or r.get("id")
        name = r.get("kingdom") or r.get("name") or r.get("kingdomName")
        alliance = r.get("alliance") or r.get("allianceName") or r.get("ally")
        ranking = r.get("ranking") or r.get("rank") or r.get("position")
        networth = r.get("networth") or r.get("nettWorth") or r.get("nw")

        if kid is None or name is None:
            continue

        try:
            kid = int(kid)
        except Exception:
            continue

        try:
            ranking = int(ranking) if ranking is not None else None
        except Exception:
            ranking = None

        try:
            networth = int(networth) if networth is not None else None
        except Exception:
            networth = None

        out.append(
            {
                "kingdom_id": kid,
                "kingdom": str(name),
                "alliance": str(alliance) if alliance is not None else None,
                "ranking": ranking,
                "networth": networth,
            }
        )

    return out

def _upsert_top(rows: List[Dict]):
    if not rows:
        return
    now = datetime.now(timezone.utc)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO public.kg_top_kingdoms
                      (kingdom_id, kingdom, alliance, ranking, networth, fetched_at)
                    VALUES
                      (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (kingdom_id) DO UPDATE SET
                      kingdom    = EXCLUDED.kingdom,
                      alliance   = EXCLUDED.alliance,
                      ranking    = EXCLUDED.ranking,
                      networth   = EXCLUDED.networth,
                      fetched_at = EXCLUDED.fetched_at
                    """,
                    (
                        r["kingdom_id"],
                        r["kingdom"],
                        r["alliance"],
                        r["ranking"],
                        r["networth"],
                        now,
                    ),
                )
            conn.commit()
    finally:
        conn.close()

def _poll_rankings_once(*, world_id: str, kg_token: str):
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "world-id": str(world_id),
        "Origin": "https://www.kingdomgame.net",
        "Referer": "https://www.kingdomgame.net/rankings",
    }
    if kg_token:
        headers["token"] = kg_token

    # Some implementations need a payload; some don't.
    # We send a minimal one that many KG endpoints accept.
    payload = {"page": 1, "pageSize": 300}

    with httpx.Client(timeout=30.0) as client:
        r = client.post(KG_RANKINGS_URL, headers=headers, json=payload)
        r.raise_for_status()

        j = r.json()
        payload = _parse_kg_d_json(j) or j  # prefer parsed "d" but fallback raw

        top = _extract_top_kingdoms(payload, limit=300)
        _upsert_top(top)

def start_rankings_poller(*, poll_seconds: int = 900, world_id: str = "1", kg_token: str = ""):
    """
    Poll rankings every 15 minutes by default.
    This is plenty, and much lighter than 4-min NWOT polling.
    """
    _ensure_tables()

    def loop():
        while True:
            try:
                _poll_rankings_once(world_id=world_id, kg_token=kg_token)
            except Exception as e:
                print("[rankings_poller] error:", repr(e))
            time.sleep(int(poll_seconds))

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t
