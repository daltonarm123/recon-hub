import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
import psycopg
from psycopg.rows import dict_row

from db_dsn import resolve_database_dsn

KG_RANKINGS_URL = os.getenv(
    "KG_RANKINGS_URL",
    "https://kingdomgame.net/WebService/Kingdoms.asmx/GetKingdomRankings",
)
KG_USER_LOGIN_URL = os.getenv(
    "KG_USER_LOGIN_URL",
    "https://kingdomgame.net/WebService/User.asmx/Login",
)
KG_REQUEST_TIMEOUT_SECONDS = float(os.getenv("KG_REQUEST_TIMEOUT_SECONDS", "30"))
KG_TOKEN_TTL_SECONDS = max(30, int(os.getenv("KG_TOKEN_TTL_SECONDS", "1500")))

_POLL_THREAD: Optional[threading.Thread] = None
_LAST_GOOD_STATIC_IDX = 0

_LOGIN_CACHE: Dict[str, Any] = {
    "token": "",
    "account_id": None,
    "kingdom_id": None,
    "expires_at": 0.0,
    "last_status": "not_configured",
    "last_error": "",
}

_DIAG_LOCK = threading.Lock()
_DIAG: Dict[str, Any] = {
    "login_status": "not_configured",
    "rankings_status": "idle",
    "auth_mode": "none",
    "rows_pulled": 0,
    "last_poll": None,
    "last_error": "",
    "attempts": [],
}


def _log(msg: str):
    print(msg, flush=True)


def _compact_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def _dsn() -> str:
    return resolve_database_dsn()


def _connect():
    return psycopg.connect(_dsn(), row_factory=dict_row)


def _parse_int(v: object) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _origin_for_url(url: str) -> str:
    m = re.match(r"^(https?://[^/]+)", str(url or "").strip(), flags=re.I)
    return m.group(1) if m else "https://kingdomgame.net"


def _add_diag_attempt(
    *,
    url: str,
    start_number: Optional[int],
    status: Optional[int],
    body_preview: str,
    ok: bool,
    auth_mode: str,
    error: str = "",
):
    preview = (body_preview or "").replace("\n", " ").strip()
    if len(preview) > 240:
        preview = preview[:240]

    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "startNumber": start_number,
        "status": status,
        "ok": bool(ok),
        "authMode": auth_mode,
        "bodyPreview": preview,
    }
    if error:
        row["error"] = error

    with _DIAG_LOCK:
        attempts = list(_DIAG.get("attempts") or [])
        attempts.append(row)
        _DIAG["attempts"] = attempts[-25:]


def _set_diag(**fields: Any):
    with _DIAG_LOCK:
        for k, v in fields.items():
            _DIAG[k] = v


def get_rankings_health() -> Dict[str, Any]:
    with _DIAG_LOCK:
        return {
            "login_status": _DIAG.get("login_status"),
            "rankings_status": _DIAG.get("rankings_status"),
            "auth_mode": _DIAG.get("auth_mode"),
            "rows_pulled": int(_DIAG.get("rows_pulled") or 0),
            "last_poll": _DIAG.get("last_poll"),
            "last_error": _DIAG.get("last_error"),
            "attempts": list(_DIAG.get("attempts") or []),
        }


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


def _rankings_urls() -> List[str]:
    primary = str(KG_RANKINGS_URL or "").strip()
    urls: List[str] = [primary] if primary else []

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

    seen = set()
    out: List[str] = []
    for u in urls:
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _login_urls() -> List[str]:
    primary = str(KG_USER_LOGIN_URL or "").strip()
    urls: List[str] = [primary] if primary else []
    if primary.startswith("https://kingdomgame.net/"):
        urls.append(primary.replace("https://kingdomgame.net/", "https://www.kingdomgame.net/", 1))
    elif primary.startswith("https://www.kingdomgame.net/"):
        urls.append(primary.replace("https://www.kingdomgame.net/", "https://kingdomgame.net/", 1))
    else:
        urls.extend(
            [
                "https://kingdomgame.net/WebService/User.asmx/Login",
                "https://www.kingdomgame.net/WebService/User.asmx/Login",
            ]
        )

    seen = set()
    out: List[str] = []
    for u in urls:
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _kg_headers(world_id: str, url: str) -> Dict[str, str]:
    origin = _origin_for_url(url)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "World-Id": str(world_id),
        "Origin": origin,
        "Referer": f"{origin}/rankings",
        "User-Agent": os.getenv(
            "KG_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        ),
        "Accept-Language": os.getenv("KG_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
    }

    cookie = str(os.getenv("KG_COOKIE", "")).strip()
    if cookie:
        headers["Cookie"] = cookie
        m = re.search(r"(?:^|;\s*)__RequestVerificationToken=([^;]+)", cookie)
        if m:
            tok = m.group(1).strip()
            if tok:
                headers["RequestVerificationToken"] = tok
                headers["X-RequestVerificationToken"] = tok

    extra_headers_raw = str(os.getenv("KG_EXTRA_HEADERS_JSON", "")).strip()
    if extra_headers_raw:
        try:
            extra = json.loads(extra_headers_raw)
            if isinstance(extra, dict):
                for k, v in extra.items():
                    key = str(k or "").strip()
                    if not key:
                        continue
                    headers[key] = str(v)
        except Exception:
            pass

    return headers


def _kg_login_headers(url: str) -> Dict[str, str]:
    origin = _origin_for_url(url)
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": f"{origin}/rankings",
        "User-Agent": os.getenv(
            "KG_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        ),
        "Accept-Language": os.getenv("KG_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
    }


def _parse_kg_d_json(resp_json: Dict[str, Any]) -> Dict[str, Any]:
    d = resp_json.get("d")
    if not d:
        return {}
    if isinstance(d, dict):
        return d
    if isinstance(d, list):
        return {"kingdoms": d}
    if isinstance(d, str):
        try:
            parsed = json.loads(d)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _parse_json_response(resp: httpx.Response) -> Dict[str, Any]:
    raw: Dict[str, Any] = {}
    try:
        raw = resp.json()
    except Exception:
        text = (resp.text or "").strip()
        if text:
            try:
                loaded = json.loads(text)
                if isinstance(loaded, dict):
                    raw = loaded
            except Exception:
                raw = {}
    parsed = _parse_kg_d_json(raw)
    if parsed:
        return parsed
    return raw if isinstance(raw, dict) else {}


def _extract_kingdoms(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
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
        return []

    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        kingdom_id = row.get("id")
        if kingdom_id is None:
            kingdom_id = row.get("Id")
        if kingdom_id is None:
            kingdom_id = row.get("kingdomId")

        kingdom = row.get("name")
        if kingdom is None:
            kingdom = row.get("Name")
        if kingdom is None:
            kingdom = row.get("kingdom")

        alliance = row.get("allianceName")
        if alliance is None:
            alliance = row.get("AllianceName")
        if alliance is None:
            alliance = row.get("alliance")

        rank = row.get("rank")
        if rank is None:
            rank = row.get("ranking")
        if rank is None:
            rank = row.get("Rank")

        networth = row.get("networth")
        if networth is None:
            networth = row.get("Networth")
        if networth is None:
            networth = row.get("netWorth")

        kingdom_id_i = _parse_int(kingdom_id)
        rank_i = _parse_int(rank)
        networth_i = _parse_int(networth)
        if kingdom_id_i is None or not str(kingdom or "").strip() or rank_i is None or networth_i is None:
            continue

        out.append(
            {
                "kingdom_id": kingdom_id_i,
                "kingdom": str(kingdom).strip(),
                "alliance": str(alliance).strip() if alliance is not None else None,
                "ranking": rank_i,
                "networth": networth_i,
            }
        )
    return out


def _build_rankings_payload(
    *,
    account_id: int,
    token: str,
    kingdom_id: int,
    continent_id: int,
    start_number: int,
) -> Dict[str, Any]:
    return {
        "accountId": int(account_id),
        "token": str(token),
        "kingdomId": int(kingdom_id),
        "continentId": int(continent_id),
        "startNumber": int(start_number),
    }


def _rankings_offsets(top_n: int = 100, page_size: int = 20) -> List[int]:
    if top_n <= 0:
        return []
    out: List[int] = []
    current = 0
    while current < top_n:
        out.append(current)
        current += page_size
    return out


def _merge_rankings_pages(rows: List[Dict[str, Any]], limit: int = 100) -> List[Dict[str, Any]]:
    best_by_id: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        kid = _parse_int(row.get("kingdom_id"))
        rank = _parse_int(row.get("ranking"))
        if kid is None or rank is None:
            continue

        prev = best_by_id.get(kid)
        if prev is None:
            best_by_id[kid] = dict(row)
            continue

        prev_rank = _parse_int(prev.get("ranking"))
        if prev_rank is None or rank < prev_rank:
            best_by_id[kid] = dict(row)

    merged = sorted(best_by_id.values(), key=lambda r: (_parse_int(r.get("ranking")) or 999999, _parse_int(r.get("kingdom_id")) or 0))
    return merged[: max(0, int(limit))]


def _upsert_top(rows: List[Dict[str, Any]], fetched_at: datetime):
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
                        int(r["kingdom_id"]),
                        str(r["kingdom"]),
                        r.get("alliance"),
                        int(r["ranking"]),
                        int(r["networth"]),
                        fetched_at,
                    )
                    for r in rows
                ],
            )
        conn.commit()
    finally:
        conn.close()


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


def _resolve_static_rankings_creds() -> List[Dict[str, object]]:
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
        if not any(
            c["account_id"] == preferred["account_id"]
            and c["kingdom_id"] == preferred["kingdom_id"]
            and c["token"] == preferred["token"]
            for c in creds
        ):
            creds.insert(0, preferred)

    return creds


def _login_auth_configured() -> bool:
    email = str(os.getenv("KG_LOGIN_EMAIL", "")).strip()
    password = str(os.getenv("KG_LOGIN_PASSWORD", "")).strip()
    account_id = _parse_int(os.getenv("KG_LOGIN_ACCOUNT_ID"))
    kingdom_id = _parse_int(os.getenv("KG_LOGIN_KINGDOM_ID"))
    return bool(email and password and account_id is not None and kingdom_id is not None)


def _resolve_rankings_creds() -> List[Dict[str, object]]:
    creds = _resolve_static_rankings_creds()
    if creds:
        return creds
    if _login_auth_configured():
        return []
    raise RuntimeError(
        "Missing KG credentials. Configure login auth (KG_LOGIN_EMAIL/KG_LOGIN_PASSWORD/"
        "KG_LOGIN_ACCOUNT_ID/KG_LOGIN_KINGDOM_ID) or static fallback creds (KG_POLLER_*)."
    )


def _extract_login_token(parsed: Dict[str, Any]) -> Tuple[str, Optional[int], Optional[int]]:
    token = str(
        parsed.get("token")
        or parsed.get("Token")
        or parsed.get("accessToken")
        or parsed.get("AccessToken")
        or ""
    ).strip()
    account_id = _parse_int(parsed.get("accountId") or parsed.get("AccountId") or parsed.get("accountID"))
    kingdom_id = _parse_int(parsed.get("kingdomId") or parsed.get("KingdomId") or parsed.get("kingdomID"))
    return token, account_id, kingdom_id


def _get_login_credential(client: httpx.Client) -> Optional[Dict[str, object]]:
    if not _login_auth_configured():
        _set_diag(login_status="not_configured")
        return None

    now_mono = time.monotonic()
    cached_token = str(_LOGIN_CACHE.get("token") or "").strip()
    if cached_token and now_mono < float(_LOGIN_CACHE.get("expires_at") or 0.0):
        _set_diag(login_status="ok_cached")
        return {
            "account_id": int(_LOGIN_CACHE["account_id"]),
            "kingdom_id": int(_LOGIN_CACHE["kingdom_id"]),
            "token": cached_token,
        }

    email = str(os.getenv("KG_LOGIN_EMAIL", "")).strip()
    password = str(os.getenv("KG_LOGIN_PASSWORD", "")).strip()
    fallback_account_id = _parse_int(os.getenv("KG_LOGIN_ACCOUNT_ID"))
    fallback_kingdom_id = _parse_int(os.getenv("KG_LOGIN_KINGDOM_ID"))

    payload_variants = [
        {"email": email, "password": password},
        {"Email": email, "Password": password},
        {"username": email, "password": password},
    ]

    last_error = ""
    for url in _login_urls():
        headers = _kg_login_headers(url)
        for payload in payload_variants:
            try:
                response = client.post(url, headers=headers, content=_compact_json(payload))
                parsed = _parse_json_response(response)
                response.raise_for_status()
                token, account_id, kingdom_id = _extract_login_token(parsed)
                account_id = account_id if account_id is not None else fallback_account_id
                kingdom_id = kingdom_id if kingdom_id is not None else fallback_kingdom_id

                _add_diag_attempt(
                    url=url,
                    start_number=None,
                    status=response.status_code,
                    body_preview=response.text,
                    ok=bool(token),
                    auth_mode="login",
                )

                if token and account_id is not None and kingdom_id is not None:
                    _LOGIN_CACHE["token"] = token
                    _LOGIN_CACHE["account_id"] = account_id
                    _LOGIN_CACHE["kingdom_id"] = kingdom_id
                    _LOGIN_CACHE["expires_at"] = time.monotonic() + KG_TOKEN_TTL_SECONDS
                    _LOGIN_CACHE["last_status"] = "ok_fresh"
                    _LOGIN_CACHE["last_error"] = ""
                    _set_diag(login_status="ok_fresh")
                    return {
                        "account_id": account_id,
                        "kingdom_id": kingdom_id,
                        "token": token,
                    }

                last_error = "login response missing token/account/kingdom"
            except Exception as exc:
                last_error = str(exc)
                status = getattr(getattr(exc, "response", None), "status_code", None)
                body = ""
                resp = getattr(exc, "response", None)
                if resp is not None:
                    try:
                        body = resp.text
                    except Exception:
                        body = ""
                _add_diag_attempt(
                    url=url,
                    start_number=None,
                    status=status,
                    body_preview=body,
                    ok=False,
                    auth_mode="login",
                    error=last_error,
                )

    _LOGIN_CACHE["last_status"] = "error"
    _LOGIN_CACHE["last_error"] = last_error
    _set_diag(login_status="error", last_error=last_error)
    return None


def _poll_rankings_once(
    *,
    world_id: str,
    creds: Dict[str, object],
    client: Optional[httpx.Client] = None,
    auth_mode: str = "static",
) -> Tuple[int, Optional[int]]:
    account_id = _parse_int(creds.get("account_id"))
    kingdom_id = _parse_int(creds.get("kingdom_id"))
    token = str(creds.get("token") or "").strip()
    if account_id is None or kingdom_id is None or not token:
        raise RuntimeError("invalid rankings credentials")

    continent_id = _parse_int(os.getenv("KG_CONTINENT_ID"))
    if continent_id is None:
        continent_id = -1

    session_client = client or httpx.Client(timeout=KG_REQUEST_TIMEOUT_SECONDS)
    owns_client = client is None

    all_rows: List[Dict[str, Any]] = []
    errors: List[str] = []

    try:
        for start_number in _rankings_offsets(top_n=100, page_size=20):
            payload = _build_rankings_payload(
                account_id=account_id,
                token=token,
                kingdom_id=kingdom_id,
                continent_id=continent_id,
                start_number=start_number,
            )

            got_page = False
            for url in _rankings_urls():
                headers = _kg_headers(world_id, url)
                try:
                    response = session_client.post(url, headers=headers, content=_compact_json(payload))
                    parsed = _parse_json_response(response)
                    rows = _extract_kingdoms(parsed)
                    response.raise_for_status()
                    _add_diag_attempt(
                        url=url,
                        start_number=start_number,
                        status=response.status_code,
                        body_preview=response.text,
                        ok=bool(rows),
                        auth_mode=auth_mode,
                    )
                    if rows:
                        all_rows.extend(rows)
                        got_page = True
                        break
                    errors.append(f"empty rows for startNumber={start_number} status={response.status_code}")
                except Exception as exc:
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    body = ""
                    resp = getattr(exc, "response", None)
                    if resp is not None:
                        try:
                            body = resp.text
                        except Exception:
                            body = ""
                    msg = str(exc)
                    errors.append(f"startNumber={start_number} url={url} err={msg}")
                    _add_diag_attempt(
                        url=url,
                        start_number=start_number,
                        status=status,
                        body_preview=body,
                        ok=False,
                        auth_mode=auth_mode,
                        error=msg,
                    )
            if not got_page:
                raise RuntimeError("; ".join(errors[-3:]) or f"failed to fetch startNumber={start_number}")

        merged = _merge_rankings_pages(all_rows, limit=100)
        if not merged:
            raise RuntimeError("Parsed 0 kingdoms from rankings response")

        fetched_at = datetime.now(timezone.utc)
        _upsert_top(merged, fetched_at=fetched_at)

        gal_nw = None
        for row in merged:
            if str(row.get("kingdom") or "") == "Galileo":
                gal_nw = _parse_int(row.get("networth"))
                break
        return (len(merged), gal_nw)
    finally:
        if owns_client:
            session_client.close()


def start_rankings_poller(*, poll_seconds: int = 60, world_id: str = "1"):
    global _POLL_THREAD, _LAST_GOOD_STATIC_IDX

    if _POLL_THREAD and _POLL_THREAD.is_alive():
        return _POLL_THREAD

    _ensure_tables()
    static_creds = _resolve_rankings_creds()

    _log(
        f"[rankings_poller] startup world_id={world_id} poll_seconds={max(10, int(poll_seconds))} "
        f"static_creds={len(static_creds)} login_auth={_login_auth_configured()}"
    )

    def loop():
        global _LAST_GOOD_STATIC_IDX
        sleep_seconds = max(10, int(poll_seconds))

        with httpx.Client(timeout=KG_REQUEST_TIMEOUT_SECONDS) as client:
            while True:
                started_at = datetime.now(timezone.utc)
                rows_pulled = 0
                auth_mode = "none"
                last_error = ""

                try:
                    creds_to_try: List[Tuple[str, Dict[str, object], Optional[int]]] = []

                    login_cred = _get_login_credential(client)
                    if login_cred:
                        creds_to_try.append(("login", login_cred, None))

                    if static_creds:
                        ordered = static_creds[_LAST_GOOD_STATIC_IDX:] + static_creds[:_LAST_GOOD_STATIC_IDX]
                        for idx, cred in enumerate(ordered):
                            creds_to_try.append(("static", cred, idx))

                    if not creds_to_try:
                        raise RuntimeError("No usable rankings auth configured")

                    poll_success = False
                    for mode, cred, ordered_idx in creds_to_try:
                        try:
                            rows_pulled, gal_nw = _poll_rankings_once(
                                world_id=world_id,
                                creds=cred,
                                client=client,
                                auth_mode=mode,
                            )
                            auth_mode = mode
                            if mode == "static" and ordered_idx is not None and static_creds:
                                _LAST_GOOD_STATIC_IDX = (_LAST_GOOD_STATIC_IDX + ordered_idx) % len(static_creds)
                            acct = cred.get("account_id")
                            if gal_nw is not None:
                                _log(
                                    f"[rankings_poller] ok: rows={rows_pulled} auth={mode} acct={acct} "
                                    f"GalileoNW={gal_nw}"
                                )
                            else:
                                _log(f"[rankings_poller] ok: rows={rows_pulled} auth={mode} acct={acct}")
                            poll_success = True
                            break
                        except Exception as exc:
                            last_error = str(exc)

                    if not poll_success:
                        raise RuntimeError(last_error or "all auth credentials failed")

                    _set_diag(
                        rankings_status="ok",
                        auth_mode=auth_mode,
                        rows_pulled=int(rows_pulled),
                        last_poll=started_at.isoformat(),
                        last_error="",
                    )
                except Exception as exc:
                    last_error = str(exc)
                    _log(f"[rankings_poller] error: {last_error}")
                    _set_diag(
                        rankings_status="error",
                        auth_mode=auth_mode or "none",
                        rows_pulled=0,
                        last_poll=started_at.isoformat(),
                        last_error=last_error,
                    )

                time.sleep(sleep_seconds)

    _POLL_THREAD = threading.Thread(target=loop, daemon=True, name="rankings-poller")
    _POLL_THREAD.start()
    return _POLL_THREAD
