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
KG_REQUEST_TIMEOUT_SECONDS = float(os.getenv("KG_REQUEST_TIMEOUT_SECONDS", "30"))
KG_PAGE_RETRIES = max(1, int(os.getenv("KG_PAGE_RETRIES", "3")))


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


def _log(msg: str):
    print(msg, flush=True)


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
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.kingdomgame.net",
        "Referer": "https://www.kingdomgame.net/rankings",
        # Some KG endpoints/anti-bot layers appear sensitive to header casing;
        # send both variants to match browser captures.
        "World-Id": str(world_id),
        "world-id": str(world_id),
        "User-Agent": os.getenv(
            "KG_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
        ),
        "Accept-Language": os.getenv("KG_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }
    cookie = os.getenv("KG_COOKIE", "").strip()
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _kg_base_payload(creds: Dict[str, object]) -> Dict[str, object]:

    continent_id = int(os.getenv("KG_CONTINENT_ID", "-1"))
    start_number = int(os.getenv("KG_START_NUMBER", "-1"))

    account_id = str(creds["account_id"])
    token = str(creds["token"])
    kingdom_id = str(creds["kingdom_id"])

    return {
        "accountId": str(account_id),
        "token": str(token),
        "kingdomId": int(kingdom_id),
        "continentId": int(continent_id),
        "startNumber": int(start_number),
    }


def _parse_int(v: object) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(str(v).strip())
    except Exception:
        return None


def _parse_cred(raw: Dict[str, object]) -> Optional[Dict[str, object]]:
    account_id = _parse_int(raw.get("account_id"))
    kingdom_id = _parse_int(raw.get("kingdom_id"))
    token = str(raw.get("token") or "").strip()
    if account_id is None or kingdom_id is None or not token:
        return None
    return {"account_id": account_id, "kingdom_id": kingdom_id, "token": token}


def _resolve_rankings_creds() -> List[Dict[str, object]]:
    """
    Credential sources (in priority order):
    1) KG_POLLER_CREDENTIALS_JSON: JSON array of objects
       [{"account_id":16881,"kingdom_id":6045,"token":"..."}, ...]
    2) KG_POLLER_ACCOUNT_ID / KG_POLLER_TOKEN / KG_POLLER_KINGDOM_ID
    """
    creds: List[Dict[str, object]] = []

    raw_json = os.getenv("KG_POLLER_CREDENTIALS_JSON", "").strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        c = _parse_cred(item)
                        if c:
                            creds.append(c)
        except Exception:
            pass

    preferred = _parse_cred(
        {
            "account_id": os.getenv("KG_POLLER_ACCOUNT_ID"),
            "token": os.getenv("KG_POLLER_TOKEN"),
            "kingdom_id": os.getenv("KG_POLLER_KINGDOM_ID"),
        }
    )
    if preferred:
        dup = False
        for c in creds:
            if (
                c["account_id"] == preferred["account_id"]
                and c["kingdom_id"] == preferred["kingdom_id"]
                and c["token"] == preferred["token"]
            ):
                dup = True
                break
        if not dup:
            creds.insert(0, preferred)

    if not creds:
        raise RuntimeError(
            "Missing KG poller credentials. Set either KG_POLLER_CREDENTIALS_JSON "
            "or KG_POLLER_ACCOUNT_ID/KG_POLLER_TOKEN/KG_POLLER_KINGDOM_ID."
        )
    return creds


# -------------------------
# Poll once (paginated to 300)
# -------------------------
def _poll_rankings_once(*, world_id: str, creds: Dict[str, object]) -> Tuple[int, Optional[int]]:
    headers = _kg_headers(world_id)
    base_payload = _kg_base_payload(creds)

    all_rows: List[Dict] = []
    seen_ids = set()
    # Prefer configured startNumber first (often -1 works as "return top list").
    starts_to_try: List[int] = []
    configured_start = _parse_int(base_payload.get("startNumber"))
    if configured_start is not None:
        starts_to_try.append(configured_start)
    if 1 not in starts_to_try:
        starts_to_try.append(1)

    parsed_last: Dict = {}

    def post_rankings_page(client: httpx.Client, start_number: int) -> Dict:
        payload = dict(base_payload)
        payload["startNumber"] = start_number
        last_err: Optional[Exception] = None
        for attempt in range(1, KG_PAGE_RETRIES + 1):
            try:
                r = client.post(KG_RANKINGS_URL, headers=headers, json=payload)
                r.raise_for_status()
                raw = r.json()
                parsed = _parse_kg_d_json(raw) or raw
                return parsed if isinstance(parsed, dict) else {}
            except Exception as e:
                last_err = e
                if attempt < KG_PAGE_RETRIES:
                    time.sleep(min(2 ** (attempt - 1), 4) + random.uniform(0.0, 0.5))
        raise last_err or RuntimeError("rankings page request failed")

    with httpx.Client(timeout=KG_REQUEST_TIMEOUT_SECONDS) as client:
        for start in starts_to_try:
            try:
                parsed = post_rankings_page(client, start)
                parsed_last = parsed
                chunk = _extract_kingdoms(parsed)
            except Exception:
                chunk = []
            if chunk:
                for row in chunk:
                    kid = row["kingdom_id"]
                    if kid in seen_ids:
                        continue
                    seen_ids.add(kid)
                    all_rows.append(row)
                # If KG already returns the full top list, avoid extra paging requests.
                if len(all_rows) >= 250:
                    break
                # Continue paging from current offset.
                start = max(1, start) + max(1, len(chunk))
                break

        while len(all_rows) < 300:
            try:
                parsed = post_rankings_page(client, start)
            except Exception:
                break

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
_LAST_GOOD_CRED_IDX: int = 0


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
    cred_pool = _resolve_rankings_creds()
    _log(
        f"[rankings_poller] startup world_id={world_id} "
        f"tick_delay={KG_TICK_DELAY_SECONDS}s creds={len(cred_pool)}"
    )

    def loop():
        global _LAST_GOOD_CRED_IDX
        # small boot jitter so multiple restarts don't hammer KG at once
        time.sleep(random.uniform(0.0, 2.0))
        first_run = True

        while True:
            try:
                if first_run:
                    # Run once immediately after boot to avoid long cold-start staleness.
                    target = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                    first_run = False
                else:
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
                    last_cred_err: Optional[Exception] = None
                    try:
                        ordered = cred_pool[_LAST_GOOD_CRED_IDX:] + cred_pool[:_LAST_GOOD_CRED_IDX]
                        success = False
                        for i, cred in enumerate(ordered):
                            try:
                                n, gal_nw = _poll_rankings_once(world_id=world_id, creds=cred)
                                _LAST_GOOD_CRED_IDX = (i + _LAST_GOOD_CRED_IDX) % len(cred_pool)
                                acct = cred.get("account_id")
                                if gal_nw is not None:
                                    _log(
                                        f"[rankings_poller] ok: upserted {n} kingdoms @ "
                                        f"{target.isoformat()} GalileoNW={gal_nw} acct={acct}"
                                    )
                                else:
                                    _log(
                                        f"[rankings_poller] ok: upserted {n} kingdoms @ "
                                        f"{target.isoformat()} acct={acct}"
                                    )
                                last_err = None
                                success = True
                                break
                            except Exception as ce:
                                last_cred_err = ce
                        if success:
                            break
                        raise last_cred_err or RuntimeError("all credentials failed")
                    except Exception as e:
                        last_err = e
                        backoff = min(2 ** (attempt - 1), 30)
                        time.sleep(backoff + random.uniform(0.0, 1.2))

                if last_err:
                    _log(f"[rankings_poller] error: {repr(last_err)}")

            except Exception as e:
                _log(f"[rankings_poller] fatal error: {repr(e)}")

            # We ignore poll_seconds sleeping because we tick-align every cycle
            # (poll_seconds kept for compatibility with main.py)
            _ = poll_seconds

    _POLL_THREAD = threading.Thread(target=loop, daemon=True)
    _POLL_THREAD.start()
    return _POLL_THREAD
