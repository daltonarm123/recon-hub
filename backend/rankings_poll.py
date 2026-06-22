import os
import json
import time
import threading
import random
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import httpx
import psycopg
from psycopg.rows import dict_row

from db_dsn import resolve_database_dsn

KG_RANKINGS_URL = os.getenv(
    "KG_RANKINGS_URL",
    "https://kingdomgame.net/WebService/Kingdoms.asmx/GetKingdomRankings",
)


def _rankings_urls() -> List[str]:
    primary = str(KG_RANKINGS_URL or "").strip()
    urls: List[str] = []
    if primary:
        urls.append(primary)

    # Auto-fallback between host variants because KG behavior can differ by host.
    if primary.startswith("https://kingdomgame.net/"):
        urls.append(primary.replace("https://kingdomgame.net/", "https://www.kingdomgame.net/", 1))
    elif primary.startswith("https://www.kingdomgame.net/"):
        urls.append(primary.replace("https://www.kingdomgame.net/", "https://kingdomgame.net/", 1))
    else:
        urls.extend(
            [
                "https://kingdomgame.net/WebService/Kingdoms.asmx/GetKingdomRankings",
                "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetKingdomRankings",
            ]
        )

    # Keep order while removing duplicates.
    seen = set()
    out: List[str] = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

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
    return resolve_database_dsn()


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
    if isinstance(d, dict):
        return d
    if isinstance(d, list):
        return {"kingdoms": d}
    try:
        return json.loads(d)
    except Exception:
        return {}


def _extract_kingdoms(payload: Dict) -> List[Dict]:
    rows = payload.get("kingdoms")
    if not isinstance(rows, list):
        rows = payload.get("Kingdoms")
    if not isinstance(rows, list):
        rows = payload.get("rows")
    if not isinstance(rows, list):
        data = payload.get("data")
        if isinstance(data, dict):
            rows = data.get("kingdoms") or data.get("rows")
    if not isinstance(rows, list):
        d = payload.get("d")
        if isinstance(d, dict):
            rows = d.get("kingdoms") or d.get("rows")

    if not isinstance(rows, list):
        return []

    out: List[Dict] = []
    for r in rows:
        if not isinstance(r, dict):
            continue

        kid = r.get("id")
        if kid is None:
            kid = r.get("Id")
        if kid is None:
            kid = r.get("kingdomId")

        name = r.get("name")
        if name is None:
            name = r.get("Name")
        if name is None:
            name = r.get("kingdom")

        alliance = r.get("allianceName")
        if alliance is None:
            alliance = r.get("AllianceName")
        if alliance is None:
            alliance = r.get("alliance")

        ranking = r.get("rank")
        if ranking is None:
            ranking = r.get("ranking")
        if ranking is None:
            ranking = r.get("Rank")

        networth = r.get("networth")
        if networth is None:
            networth = r.get("Networth")
        if networth is None:
            networth = r.get("netWorth")

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
        # Origin/Referer are set per-request to match the target host.
        "Origin": "https://kingdomgame.net",
        "Referer": "https://kingdomgame.net/rankings",
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
    }


def _kg_header_variants(url: str, world_id: str) -> List[Dict[str, str]]:
    origin_match = re.match(r"^(https?://[^/]+)", str(url or "").strip(), flags=re.I)
    origin = origin_match.group(1) if origin_match else "https://kingdomgame.net"

    base = _kg_headers(world_id)
    base["Origin"] = origin
    base["Referer"] = f"{origin}/rankings"

    cookie = os.getenv("KG_COOKIE", "").strip()
    if cookie:
        base["Cookie"] = cookie

    variants: List[Dict[str, str]] = [dict(base)]

    enriched = dict(base)

    # Some ASP.NET setups require the antiforgery token to be echoed as a header.
    if cookie:
        m = re.search(r"(?:^|;\s*)__RequestVerificationToken=([^;]+)", cookie)
        if m:
            tok = m.group(1).strip()
            if tok:
                enriched["RequestVerificationToken"] = tok
                enriched["X-RequestVerificationToken"] = tok

    extra_headers_raw = os.getenv("KG_EXTRA_HEADERS_JSON", "").strip()
    if extra_headers_raw:
        try:
            extra = json.loads(extra_headers_raw)
            if isinstance(extra, dict):
                for k, v in extra.items():
                    ks = str(k or "").strip()
                    if not ks:
                        continue
                    enriched[ks] = str(v)
        except Exception:
            pass

    # Only add the enriched variant if it differs from the minimal proven-good set.
    if enriched != base:
        variants.append(enriched)

    return variants


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
        "worldId": int(os.getenv("KG_WORLD_ID", "1") or "1"),
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
    if account_id is None:
        account_id = _parse_int(raw.get("accountId"))

    kingdom_id = _parse_int(raw.get("kingdom_id"))
    if kingdom_id is None:
        kingdom_id = _parse_int(raw.get("kingdomId"))

    token = str(raw.get("token") or raw.get("Token") or "").strip()
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

    def _payload_variants(start_number: int) -> List[Dict[str, object]]:
        base = dict(base_payload)
        base["startNumber"] = start_number

        world_id_int = int(base.get("worldId", 1) or 1)
        account_id_int = int(base.get("accountId", 0) or 0)
        kingdom_id_int = int(base.get("kingdomId", 0) or 0)
        continent_id_int = int(base.get("continentId", -1) or -1)

        # This exact shape matched the working browser request length/profile
        # and returned rankings data during direct verification.
        v1 = {
            "accountId": str(base.get("accountId", "")),
            "token": str(base.get("token", "")),
            "kingdomId": kingdom_id_int,
            "continentId": continent_id_int,
            "startNumber": int(start_number),
        }

        v1b = dict(base)
        v2 = {
            "accountID": str(base.get("accountId", "")),
            "token": str(base.get("token", "")),
            "kingdomID": kingdom_id_int,
            "continentID": continent_id_int,
            "startNumber": int(start_number),
            "worldId": world_id_int,
        }
        v3 = {
            "accountId": str(base.get("accountId", "")),
            "token": str(base.get("token", "")),
            "kingdomId": kingdom_id_int,
            "startNumber": int(start_number),
            "worldId": world_id_int,
        }

        # Some KG nodes are strict about key casing and stringly-typed values.
        v4 = {
            "accountID": str(account_id_int),
            "token": str(base.get("token", "")),
            "kingdomID": str(kingdom_id_int),
            "continentID": str(continent_id_int),
            "startNumber": str(int(start_number)),
            "worldID": str(world_id_int),
        }

        # Minimal payload fallback (some deployments ignore optional fields).
        v5 = {
            "accountId": account_id_int,
            "token": str(base.get("token", "")),
            "kingdomId": kingdom_id_int,
        }

        # Retry with common alternate start/continent defaults when KG returns {}.
        variants: List[Dict[str, object]] = [v1, v1b, v2, v3, v4, v5]
        for alt_continent in (-1, 0, 1):
            variants.append(
                {
                    "accountId": account_id_int,
                    "token": str(base.get("token", "")),
                    "kingdomId": kingdom_id_int,
                    "continentId": alt_continent,
                    "startNumber": int(start_number),
                    "worldId": world_id_int,
                }
            )

        return variants

    def _parse_response_body(r: httpx.Response) -> Dict:
        raw: Dict = {}
        try:
            raw = r.json()
        except Exception:
            text = (r.text or "").strip()
            if text:
                try:
                    raw = json.loads(text)
                except Exception:
                    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
                    if m:
                        try:
                            raw = json.loads(m.group(0))
                        except Exception:
                            raw = {}
        parsed = _parse_kg_d_json(raw) or raw
        return parsed if isinstance(parsed, dict) else {}

    def _origin_for_url(url: str) -> str:
        m = re.match(r"^(https?://[^/]+)", str(url or "").strip(), flags=re.I)
        if m:
            return m.group(1)
        return "https://kingdomgame.net"

    def post_rankings_page(client: httpx.Client, start_number: int) -> Dict:
        last_err: Optional[Exception] = None
        for attempt in range(1, KG_PAGE_RETRIES + 1):
            for url in _rankings_urls():
                for payload in _payload_variants(start_number):
                    for req_headers in _kg_header_variants(url, world_id):
                        try:
                            r = client.post(url, headers=req_headers, json=payload)
                            r.raise_for_status()
                            parsed = _parse_response_body(r)
                            if parsed:
                                return parsed

                            body1 = (r.text or "").strip().replace("\n", " ")
                            if len(body1) > 220:
                                body1 = body1[:220]

                            # Retry same payload as text body for stricter ASMX handling.
                            req_headers2 = dict(req_headers)
                            req_headers2["Content-Type"] = "application/json; charset=UTF-8"
                            r2 = client.post(url, headers=req_headers2, content=json.dumps(payload))
                            r2.raise_for_status()
                            parsed2 = _parse_response_body(r2)
                            if parsed2:
                                return parsed2

                            body2 = (r2.text or "").strip().replace("\n", " ")
                            if len(body2) > 220:
                                body2 = body2[:220]

                            last_err = RuntimeError(
                                "empty parsed response "
                                f"url={url} status1={r.status_code} ct1={r.headers.get('content-type','')} "
                                f"body1={body1!r} status2={r2.status_code} ct2={r2.headers.get('content-type','')} "
                                f"body2={body2!r}"
                            )
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
        has_cookie = bool(str(os.getenv("KG_COOKIE", "")).strip())
        raise RuntimeError(
            "Parsed 0 kingdoms from KG response. "
            f"cookie_present={has_cookie}. Snippet: {snippet}"
        )

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
