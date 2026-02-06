import os
import json
import time
import threading
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import httpx
import psycopg
from psycopg.rows import dict_row

KG_RANKINGS_URL = os.getenv(
    "KG_RANKINGS_URL",
    "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetKingdomRankings",
)

# How long to wait AFTER the tick boundary before hitting KG
# (important because the game UI often lags a bit after :00/:05)
KG_TICK_DELAY_SECONDS = float(os.getenv("KG_TICK_DELAY_SECONDS", "45"))


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
    d = resp_json.get("d")
    if not d:
        return {}
    try:
        return json.loads(d)
    except Exception:
        return {}


def _extract_kingdoms(payload: Dict) -> List[Dict]:
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
def _upsert_top(rows: List[Dict], fetched_at: datetime):
    if not rows:
        return

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(
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
                [
                    (
                        r["kingdom_id"],
                        r["kingdom"],
                        r["alliance"],
                        r["ranking"],
                        r["networth"],
                        fetched_at,
                    )
                    for r in rows
                ],
            )
        conn.commit()
    finally:
        conn.close()


# -------------------------
# KG request builders
# -------------------------
def _kg_headers(world_id: str) -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.kingdomgame.net",
        "Referer": "https://www.kingdomgame.net/rankings",
        "World-Id": str(world_id),
        "User-Agent": "recon-hub/1.0 (rankings_poller)",
    }


def _kg_base_payload() -> Dict[str, object]:
    account_id = os.getenv("KG_ACCOUNT_ID", "").strip()
    token = os.getenv("KG_TOKEN", "").strip()
    kingdom_id = os.getenv("KG_KINGDOM_ID", "").strip()

    continent_id = int(os.getenv("KG_CONTINENT_ID", "-1"))
    start_number = int(os.getenv("KG_START_NUMBER", "-1"))

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
def _poll_rankings_once(*, world_id: str) -> Tuple[int, Optional[int]]:
    headers = _kg_headers(world_id)
    base_payload = _kg_base_payload()

    all_rows: List[Dict] = []
    seen_ids = set()
    start = 1
    parsed_last: Dict = {}

    with httpx.Client(timeout=30.0) as client:
        while len(all_rows) < 300:
            payload = dict(base_payload)
            payload["startNumber"] = start

            r = client.post(KG_RANKINGS_URL, headers=headers, json=payload)
            r.raise_for_status()

            raw = r.json()
            parsed = _parse_kg_d_json(raw) or raw
            parsed_last = parsed if isinstance(parsed, dict) else {}

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

            start += max(1, len(chunk))

            if added_this_page == 0:
                break

            time.sleep(0.12)

    if not all_rows:
        snippet = str(parsed_last)[:350]
        raise RuntimeError(f"Parsed 0 kingdoms from KG response. Snippet: {snippet}")

    fetched_at = datetime.now(timezone.utc)
    _upsert_top(all_rows[:300], fetched_at=fetched_at)

    # Debug Galileo NW if present
    gal_nw = None
    for r in all_rows[:300]:
        if r.get("kingdom") == "Galileo":
            gal_nw = r.get("networth")
            break

    return (min(len(all_rows), 300), gal_nw)


# -------------------------
# Public: start poller
# -------------------------
_POLL_THREAD: Optional[threading.Thread] = None


def start_rankings_poller(*, poll_seconds: int = 300, world_id: str = "1"):
    """
    Tick-aligned rankings poller:
    - wakes up exactly on :00/:05/:10...
    - waits KG_TICK_DELAY_SECONDS so KG has time to settle
    - fetches/upserts top 300 into public.kg_top_kingdoms
    """
    global _POLL_THREAD
    if _POLL_THREAD and _POLL_THREAD.is_alive():
        return _POLL_THREAD

    _ensure_tables()

    def loop():
        # small boot jitter so multiple restarts don't hammer KG at once
        time.sleep(random.uniform(0.0, 2.0))

        while True:
            try:
                # Align to tick boundary
                target = _next_5min_boundary_utc(datetime.now(timezone.utc))
                _sleep_until(target)

                # Let KG settle post-tick
                if KG_TICK_DELAY_SECONDS > 0:
                    time.sleep(KG_TICK_DELAY_SECONDS)

                last_err: Optional[Exception] = None
                gal_nw: Optional[int] = None
                n: int = 0

                for attempt in range(1, 7):
                    try:
                        n, gal_nw = _poll_rankings_once(world_id=world_id)
                        if gal_nw is not None:
                            print(f"[rankings_poller] ok: upserted {n} kingdoms @ {target.isoformat()} GalileoNW={gal_nw}")
                        else:
                            print(f"[rankings_poller] ok: upserted {n} kingdoms @ {target.isoformat()}")
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

            # We ignore poll_seconds sleeping because we tick-align every cycle
            # (poll_seconds kept for compatibility with main.py)
            _ = poll_seconds

    _POLL_THREAD = threading.Thread(target=loop, daemon=True)
    _POLL_THREAD.start()
    return _POLL_THREAD