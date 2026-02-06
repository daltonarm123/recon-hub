import os
import json
import time
import threading
import random
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx
import psycopg
from psycopg.rows import dict_row

# Browser shows: POST /WebService/Kingdoms.asmx/GetKingdomRankings
KG_RANKINGS_URL = os.getenv(
    "KG_RANKINGS_URL",
    "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetKingdomRankings",
)


# -------------------------
# DB helpers
# -------------------------
def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    return dsn


def _connect():
    return psycopg.connect(_dsn(), row_factory=dict_row)


def _ensure_tables():
    conn = _connect()
    try:
        with conn.cursor() as cur:
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


# -------------------------
# KG response parsing
# -------------------------
def _parse_kg_d_json(resp_json: Dict) -> Dict:
    """
    KG returns: {"d": "<stringified json>"}.
    Normalize to dict.
    """
    d = resp_json.get("d")
    if not d:
        return {}
    try:
        return json.loads(d)
    except Exception:
        return {}


def _extract_kingdoms(payload: Dict) -> List[Dict]:
    """
    Payload shape from GetKingdomRankings: {"kingdoms":[...], "totalKingdoms":..., ...}
    """
    rows = payload.get("kingdoms")
    if not isinstance(rows, list):
        return []

    out: List[Dict] = []
    for r in rows:
        if not isinstance(r, dict):
            continue

        kid = r.get("id")
        name = r.get("name")
        alliance = r.get("allianceName")
        ranking = r.get("rank")
        networth = r.get("networth")

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
                "kingdom": str(name).strip(),
                "alliance": str(alliance).strip() if alliance is not None else None,
                "ranking": ranking,
                "networth": networth,
            }
        )

    return out


# -------------------------
# DB upsert
# -------------------------
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


# -------------------------
# KG request builders
# -------------------------
def _kg_headers(world_id: str) -> Dict[str, str]:
    # Match browser essentials (cookies not required)
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.kingdomgame.net",
        "Referer": "https://www.kingdomgame.net/rankings",
        "World-Id": str(world_id),
        "User-Agent": "recon-hub/1.0 (rankings_poller)",
    }


def _kg_base_payload() -> Dict[str, object]:
    """
    Matches what you captured in DevTools.
    Values should be provided via env vars (do NOT hardcode secrets).
    """
    account_id = os.getenv("KG_ACCOUNT_ID", "").strip()
    token = os.getenv("KG_TOKEN", "").strip()
    kingdom_id = os.getenv("KG_KINGDOM_ID", "").strip()

    continent_id = int(os.getenv("KG_CONTINENT_ID", "-1"))
    start_number = int(os.getenv("KG_START_NUMBER", "-1"))  # we'll override during pagination

    if not account_id or not token or not kingdom_id:
        raise RuntimeError("Missing KG env vars: set KG_ACCOUNT_ID, KG_TOKEN, KG_KINGDOM_ID")

    return {
        "accountId": str(account_id),
        "token": str(token),
        "kingdomId": int(kingdom_id),
        "continentId": int(continent_id),
        "startNumber": int(start_number),
    }


# -------------------------
# Poll once (paginated to 300)
# -------------------------
def _poll_rankings_once(*, world_id: str) -> int:
    headers = _kg_headers(world_id)
    base_payload = _kg_base_payload()

    all_rows: List[Dict] = []
    seen_ids = set()

    # Most common: API returns 20 per call when startingRank increments.
    # We'll keep requesting until we reach 300 or the API stops returning new rows.
    start = 1

    with httpx.Client(timeout=30.0) as client:
        while len(all_rows) < 300:
            payload = dict(base_payload)
            payload["startNumber"] = start

            r = client.post(KG_RANKINGS_URL, headers=headers, json=payload)
            r.raise_for_status()

            raw = r.json()
            parsed = _parse_kg_d_json(raw) or raw
            chunk = _extract_kingdoms(parsed)

            if not chunk:
                break

            added_this_page = 0
            for row in chunk:
                kid = row["kingdom_id"]
                if kid in seen_ids:
                    continue
                seen_ids.add(kid)
                all_rows.append(row)
                added_this_page += 1
                if len(all_rows) >= 300:
                    break

            # Advance by what we just got (works for 20-per-page or variable page sizes)
            start += max(1, len(chunk))

            # Safety stop: if we didn't add anything new, don't loop forever
            if added_this_page == 0:
                break

            # Tiny sleep to be polite
            time.sleep(0.15)

    if not all_rows:
        snippet = str(parsed)[:350] if "parsed" in locals() else "n/a"
        raise RuntimeError(f"Parsed 0 kingdoms from KG response. Snippet: {snippet}")

    _upsert_top(all_rows[:300])
    return min(len(all_rows), 300)


# -------------------------
# Public: start poller
# -------------------------
def start_rankings_poller(*, poll_seconds: int = 900, world_id: str = "1"):
    """
    Poll rankings on an interval (default 15 min) and upsert top 300 into public.kg_top_kingdoms.

    Uses retries/backoff because KG can be flaky.
    """
    _ensure_tables()

    def loop():
        while True:
            try:
                last_err: Optional[Exception] = None
                for attempt in range(1, 7):
                    try:
                        n = _poll_rankings_once(world_id=world_id)
                        print(f"[rankings_poller] ok: upserted {n} kingdoms")
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        backoff = min(2 ** (attempt - 1), 30)
                        time.sleep(backoff + random.uniform(0.0, 1.2))

                if last_err:
                    print("[rankings_poller] error:", repr(last_err))
            except Exception as e:
                print("[rankings_poller] fatal error:", repr(e))

            time.sleep(int(poll_seconds))

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t