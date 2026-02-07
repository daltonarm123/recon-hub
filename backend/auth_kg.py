import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
import jwt
import psycopg
from cryptography.fernet import Fernet, InvalidToken
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

router = APIRouter()

DISCORD_API_BASE = "https://discord.com/api"
JWT_COOKIE_NAME = "rh_session"


class KGConnectBody(BaseModel):
    account_id: int = Field(..., gt=0)
    kingdom_id: int = Field(..., gt=0)
    token: str = Field(..., min_length=8)


class KGLoginBody(BaseModel):
    email: str = Field(..., min_length=3)
    password: str = Field(..., min_length=3)
    kingdom_id: Optional[int] = Field(default=None, gt=0)


_LOGIN_ATTEMPTS: Dict[str, List[float]] = {}


def _get_dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not dsn:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")
    return dsn


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_dsn(), row_factory=dict_row)


def ensure_auth_tables():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.user_kg_connections (
                    discord_user_id TEXT PRIMARY KEY,
                    discord_username TEXT,
                    account_id BIGINT NOT NULL,
                    kingdom_id BIGINT NOT NULL,
                    token_enc TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
        conn.commit()
    finally:
        conn.close()


def _jwt_secret() -> str:
    secret = os.getenv("JWT_SECRET", "").strip()
    if not secret:
        raise HTTPException(status_code=500, detail="JWT_SECRET is not set")
    return secret


def _jwt_exp_hours() -> int:
    try:
        return max(1, int(os.getenv("JWT_EXP_HOURS", "168")))
    except Exception:
        return 168


def _create_session_jwt(user: Dict[str, Any]) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user["id"]),
        "name": str(user.get("username") or ""),
        "avatar": user.get("avatar"),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=_jwt_exp_hours())).timestamp()),
    }
    return jwt.encode(payload, _jwt_secret(), algorithm="HS256")


def _decode_session_jwt(token: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, _jwt_secret(), algorithms=["HS256"])
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid session")


def _session_secure_cookie() -> bool:
    return os.getenv("APP_ENV", "").strip().lower() == "production"


def _discord_client_id() -> str:
    v = os.getenv("DISCORD_CLIENT_ID", "").strip()
    if not v:
        raise HTTPException(status_code=500, detail="DISCORD_CLIENT_ID is not set")
    return v


def _discord_client_secret() -> str:
    v = os.getenv("DISCORD_CLIENT_SECRET", "").strip()
    if not v:
        raise HTTPException(status_code=500, detail="DISCORD_CLIENT_SECRET is not set")
    return v


def _discord_redirect_uri() -> str:
    v = os.getenv("DISCORD_REDIRECT_URI", "").strip()
    if not v:
        raise HTTPException(status_code=500, detail="DISCORD_REDIRECT_URI is not set")
    return v


def _frontend_url() -> str:
    return (os.getenv("FRONTEND_URL", "").strip() or "/").rstrip("/") or "/"


def _auth_scope() -> str:
    return "identify"


def _admin_user_ids() -> set[str]:
    raw = os.getenv("DEV_USER_IDS", "").strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _get_current_user(request: Request) -> Dict[str, Any]:
    token = request.cookies.get(JWT_COOKIE_NAME, "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    claims = _decode_session_jwt(token)
    uid = str(claims.get("sub") or "")
    return {
        "discord_user_id": uid,
        "discord_username": str(claims.get("name") or ""),
        "avatar": claims.get("avatar"),
        "is_admin": uid in _admin_user_ids(),
    }


def _get_fernet() -> Fernet:
    key = os.getenv("KG_TOKEN_ENCRYPTION_KEY", "").strip()
    if not key:
        raise HTTPException(status_code=500, detail="KG_TOKEN_ENCRYPTION_KEY is not set")
    try:
        return Fernet(key.encode("utf-8"))
    except Exception:
        raise HTTPException(status_code=500, detail="Invalid KG_TOKEN_ENCRYPTION_KEY format")


def _encrypt_token(token: str) -> str:
    f = _get_fernet()
    return f.encrypt(token.encode("utf-8")).decode("utf-8")


def _decrypt_token(enc: str) -> str:
    f = _get_fernet()
    try:
        return f.decrypt(enc.encode("utf-8")).decode("utf-8")
    except InvalidToken:
        raise HTTPException(status_code=500, detail="Failed to decrypt KG token")


def _kg_world_id() -> str:
    return os.getenv("KG_WORLD_ID", "1").strip() or "1"


def _kg_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.kingdomgame.net",
        "Referer": "https://www.kingdomgame.net/settlements",
        "World-Id": _kg_world_id(),
        "User-Agent": "recon-hub/1.0 (settlements)",
    }


def _kg_login_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.kingdomgame.net",
        "Referer": "https://www.kingdomgame.net/login",
        "World-Id": _kg_world_id(),
        "world-id": _kg_world_id(),
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


def _kg_base_payload(conn_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "accountId": str(conn_row["account_id"]),
        "token": _decrypt_token(str(conn_row["token_enc"])),
        "kingdomId": int(conn_row["kingdom_id"]),
    }


def _parse_kg_resp_json(raw: Dict[str, Any]) -> Dict[str, Any]:
    d = raw.get("d")
    if isinstance(d, str):
        try:
            parsed = json.loads(d)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    if isinstance(raw, dict):
        return raw
    return {}


def _kg_post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    with httpx.Client(timeout=30.0) as client:
        try:
            r = client.post(url, headers=_kg_headers(), json=payload)
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            body = ""
            try:
                body = (e.response.text or "").strip().replace("\n", " ")[:220]
            except Exception:
                body = ""
            status = e.response.status_code if e.response is not None else "?"
            raise RuntimeError(f"HTTP {status} for {url} body={body}")

        j = r.json()
        return _parse_kg_resp_json(j)


def _rate_limit_login(ip: str):
    now = datetime.now(timezone.utc).timestamp()
    bucket = _LOGIN_ATTEMPTS.get(ip, [])
    bucket = [t for t in bucket if now - t <= 300.0]
    if len(bucket) >= 12:
        raise HTTPException(status_code=429, detail="Too many KG login attempts, please wait.")
    bucket.append(now)
    _LOGIN_ATTEMPTS[ip] = bucket


def _kg_login(email: str, password: str) -> Dict[str, Any]:
    url = os.getenv("KG_LOGIN_URL", "").strip() or "https://www.kingdomgame.net/WebService/User.asmx/Login"
    payload = {"email": email, "password": password}
    with httpx.Client(timeout=30.0) as client:
        try:
            r = client.post(url, headers=_kg_login_headers(), json=payload)
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response is not None else 502
            body = ""
            try:
                body = (e.response.text or "").strip().replace("\n", " ")[:220]
            except Exception:
                body = ""
            raise HTTPException(
                status_code=502,
                detail=f"KG login upstream HTTP {status}. body={body}",
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"KG login request failed: {repr(e)}")

        try:
            raw = r.json()
        except Exception:
            snippet = (r.text or "").strip().replace("\n", " ")[:220]
            raise HTTPException(status_code=502, detail=f"KG login returned non-JSON response. body={snippet}")
    parsed = _parse_kg_resp_json(raw) or raw
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=502, detail="Invalid KG login response")
    rv = parsed.get("ReturnValue")
    if rv not in (None, 1, "1"):
        msg = str(parsed.get("ReturnString") or "KG login failed")
        raise HTTPException(status_code=401, detail=msg)
    account_id = parsed.get("accountId")
    token = parsed.get("token")
    if account_id is None or not token:
        raise HTTPException(status_code=502, detail="KG login response missing accountId/token")
    return {"account_id": int(str(account_id)), "token": str(token).strip()}


def _discover_kingdom_id(account_id: int, token: str) -> Optional[int]:
    urls = [
        os.getenv("KG_KINGDOM_DISCOVERY_URL", "").strip(),
        "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetKingdomDetails",
        "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetKingdoms",
    ]
    urls = [u for u in urls if u]
    payload = {"accountId": str(account_id), "token": token}
    for url in urls:
        try:
            parsed = _kg_post_json(url, payload)
        except Exception:
            continue
        # GetKingdomDetails shape: {"id": 6045, ...}
        if parsed.get("id") is not None:
            try:
                return int(parsed["id"])
            except Exception:
                pass
        rows = parsed.get("kingdoms")
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and row.get("id") is not None:
                    try:
                        return int(row["id"])
                    except Exception:
                        continue
    return None


def _upsert_user_kg_connection(discord_user_id: str, discord_username: str, account_id: int, kingdom_id: int, token: str):
    token_enc = _encrypt_token(token.strip())
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.user_kg_connections
                  (discord_user_id, discord_username, account_id, kingdom_id, token_enc, created_at, updated_at)
                VALUES
                  (%s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (discord_user_id) DO UPDATE SET
                  discord_username = EXCLUDED.discord_username,
                  account_id = EXCLUDED.account_id,
                  kingdom_id = EXCLUDED.kingdom_id,
                  token_enc = EXCLUDED.token_enc,
                  updated_at = now()
                """,
                (
                    discord_user_id,
                    discord_username,
                    account_id,
                    kingdom_id,
                    token_enc,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _load_user_kg_connection(discord_user_id: str) -> Optional[Dict[str, Any]]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT discord_user_id, discord_username, account_id, kingdom_id, token_enc, created_at, updated_at
                FROM public.user_kg_connections
                WHERE discord_user_id = %s
                """,
                (discord_user_id,),
            )
            row = cur.fetchone()
        return row
    finally:
        conn.close()


def _require_user_kg_connection(discord_user_id: str) -> Dict[str, Any]:
    row = _load_user_kg_connection(discord_user_id)
    if not row:
        raise HTTPException(status_code=404, detail="KG account is not connected")
    return row


def _extract_list(payload: Dict[str, Any], keys: List[str]) -> List[Any]:
    keyset = {k.lower() for k in keys}
    queue: List[Any] = [payload]
    visited_ids = set()
    while queue:
        cur = queue.pop(0)
        cid = id(cur)
        if cid in visited_ids:
            continue
        visited_ids.add(cid)

        if isinstance(cur, dict):
            for k, v in cur.items():
                if k.lower() in keyset and isinstance(v, list):
                    return v
                if isinstance(v, (dict, list)):
                    queue.append(v)
        elif isinstance(cur, list):
            for item in cur:
                if isinstance(item, (dict, list)):
                    queue.append(item)
    return []


def _extract_settlements(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = _extract_list(
        payload,
        [
            "settlements",
            "cities",
            "towns",
            "kingdomSettlements",
            "settlementList",
            "cityList",
            "townList",
            "kingdomCities",
            "kingdomTowns",
        ],
    )
    out: List[Dict[str, Any]] = []

    def parse_item(item: Any):
        if not isinstance(item, dict):
            return
        sid = item.get("id") or item.get("settlementId") or item.get("cityId") or item.get("townId")
        name = item.get("name") or item.get("settlementName") or item.get("cityName") or item.get("townName")
        if sid is None:
            return
        try:
            sid_i = int(sid)
        except Exception:
            return
        out.append(
            {
                "settlement_id": sid_i,
                "name": str(name or f"Settlement {sid_i}"),
                "raw": item,
            }
        )
    
    for item in candidates:
        parse_item(item)

    # Fallback: scan generic lists for objects that look like settlements.
    if not out:
        queue: List[Any] = [payload]
        seen = set()
        while queue:
            cur = queue.pop(0)
            cid = id(cur)
            if cid in seen:
                continue
            seen.add(cid)
            if isinstance(cur, dict):
                for v in cur.values():
                    if isinstance(v, (dict, list)):
                        queue.append(v)
            elif isinstance(cur, list):
                for item in cur:
                    if isinstance(item, dict):
                        if any(k in item for k in ("settlementId", "cityId", "townId")):
                            parse_item(item)
                    if isinstance(item, (dict, list)):
                        queue.append(item)

    return out


def _extract_buildings(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = _extract_list(
        payload,
        [
            "buildings",
            "settlementBuildings",
            "cityBuildings",
            "townBuildings",
            "buildingList",
            "settlementBuildingList",
            "cityBuildingList",
            "townBuildingList",
            "slots",
        ],
    )
    out: List[Dict[str, Any]] = []

    def parse_row(row: Any):
        if not isinstance(row, dict):
            return
        btype = (
            row.get("buildingType")
            or row.get("typeName")
            or row.get("type")
            or row.get("name")
            or row.get("buildingName")
        )
        level = row.get("level") or row.get("lvl") or row.get("buildingLevel")
        effect = row.get("effect") or row.get("description") or row.get("text") or row.get("bonus")
        if not btype:
            return
        try:
            level_i = int(level) if level is not None else 0
        except Exception:
            level_i = 0
        effect_text = str(effect).strip() if effect is not None else ""
        if effect_text:
            # KG settlement building descriptions often use [LEVEL] placeholders.
            effect_text = (
                effect_text.replace("[LEVEL]", str(level_i))
                .replace("[level]", str(level_i))
            )
        out.append(
            {
                "building_type": str(btype).strip(),
                "level": level_i,
                "effect_text": effect_text,
            }
        )
    
    for row in rows:
        parse_row(row)

    # Fallback: scan nested response for likely building objects.
    if not out:
        queue: List[Any] = [payload]
        seen = set()
        while queue:
            cur = queue.pop(0)
            cid = id(cur)
            if cid in seen:
                continue
            seen.add(cid)
            if isinstance(cur, dict):
                parse_row(cur)
                for v in cur.values():
                    if isinstance(v, (dict, list)):
                        queue.append(v)
            elif isinstance(cur, list):
                for item in cur:
                    if isinstance(item, (dict, list)):
                        queue.append(item)

    return out


def _is_summary_only_buildings(buildings: List[Dict[str, Any]]) -> bool:
    """
    Some KG settlement-list responses include only one summary row like:
    "Small Town", "Large City", etc.
    That is not actual per-building data and should trigger detail fetch.
    """
    if not buildings:
        return True
    if len(buildings) > 2:
        return False

    for b in buildings:
        bt = str(b.get("building_type") or "").strip().lower()
        et = str(b.get("effect_text") or "").strip()
        if et:
            return False
        if not any(x in bt for x in ("town", "city", "settlement")):
            return False
    return True


def _fetch_settlements_live(conn_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    base = _kg_base_payload(conn_row)
    settlements_urls = [
        os.getenv("KG_SETTLEMENTS_URL", "").strip(),
        "https://www.kingdomgame.net/WebService/Settlement.asmx/GetSettlements",
        "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetSettlements",
        "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetKingdomSettlements",
        "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetKingdom",
        "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetCities",
        "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetTowns",
    ]
    settlements_urls = [u for u in settlements_urls if u]

    continent_id = int(os.getenv("KG_CONTINENT_ID", "-1"))
    variants: List[Dict[str, Any]] = [
        dict(base),
        {**base, "continentId": continent_id},
        {**base, "continentId": -1},
        {**base, "startNumber": -1},
        {**base, "continentId": continent_id, "startNumber": -1},
        {**base, "settlementId": -1},
        {**base, "cityId": -1},
        {**base, "townId": -1},
        {
            "accountID": str(base["accountId"]),
            "token": base["token"],
            "kingdomID": int(base["kingdomId"]),
        },
    ]

    settlements: List[Dict[str, Any]] = []
    attempts: List[str] = []
    for url in settlements_urls:
        for idx, payload in enumerate(variants):
            try:
                parsed = _kg_post_json(url, payload)
                settlements = _extract_settlements(parsed)
                if settlements:
                    break
                if isinstance(parsed, dict):
                    ks = ",".join(sorted(list(parsed.keys()))[:12])
                    attempts.append(f"{url} v{idx}: no-list keys=[{ks}]")
                else:
                    attempts.append(f"{url} v{idx}: no-list")
            except Exception as e:
                attempts.append(f"{url} v{idx}: {repr(e)}")
        if settlements:
            break

    if not settlements:
        tail = " | ".join(attempts[-4:]) if attempts else "no-attempts"
        detail = f"No settlements returned from KG. Last attempts: {tail}"
        raise HTTPException(status_code=502, detail=detail)

    primary_detail_url = (
        os.getenv("KG_SETTLEMENT_DETAIL_URL", "").strip()
        or "https://www.kingdomgame.net/WebService/Settlement.asmx/GetSettlementBuildings"
    )
    fallback_detail_urls = [
        "https://www.kingdomgame.net/WebService/Settlement.asmx/GetSettlement",
        "https://www.kingdomgame.net/WebService/Settlement.asmx/GetSettlementInfo",
    ]

    for s in settlements:
        s["buildings"] = []

    def fetch_detail_for_settlement(s: Dict[str, Any]) -> Tuple[int, List[Dict[str, Any]]]:
        sid = int(s["settlement_id"])
        payload = {
            "accountId": base["accountId"],
            "token": base["token"],
            "kingdomId": int(base["kingdomId"]),
            "settlementId": sid,
        }

        try:
            parsed = _kg_post_json(primary_detail_url, payload)
            buildings = _extract_buildings(parsed)
            if buildings and not _is_summary_only_buildings(buildings):
                return (sid, buildings)
        except Exception:
            pass

        for url in fallback_detail_urls:
            try:
                parsed = _kg_post_json(url, payload)
                buildings = _extract_buildings(parsed)
                if buildings and not _is_summary_only_buildings(buildings):
                    return (sid, buildings)
            except Exception:
                continue

        return (sid, [])

    workers = max(1, min(8, len(settlements)))
    sid_to_buildings: Dict[int, List[Dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(fetch_detail_for_settlement, s) for s in settlements]
        for fut in as_completed(futures):
            sid, b = fut.result()
            sid_to_buildings[sid] = b

    for s in settlements:
        s["buildings"] = sid_to_buildings.get(int(s["settlement_id"]), [])

    for s in settlements:
        if "raw" in s:
            del s["raw"]
    return settlements


def _extract_pct(text: str, level: int = 0) -> Optional[float]:
    # Handle formulas like +[LEVELx5]%
    m_formula = re.search(r"([+-]?)\s*\[\s*LEVEL\s*x\s*([0-9]+(?:\.\d+)?)\s*\]\s*%", text, flags=re.I)
    if m_formula and level > 0:
        try:
            sign = -1.0 if (m_formula.group(1) or "") == "-" else 1.0
            factor = float(m_formula.group(2))
            return sign * float(level) * factor
        except Exception:
            pass

    m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*%", text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _extract_cap(text: str) -> Optional[float]:
    patterns = [
        r"max effect amount\s*([+-]?\d+(?:\.\d+)?)\s*%",
        r"max effect\s*([+-]?\d+(?:\.\d+)?)\s*%",
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.I)
        if not m:
            continue
        try:
            return float(m.group(1))
        except Exception:
            continue
    return None


def _effect_key(building_type: str, effect_text: str) -> Tuple[str, str]:
    bt = building_type.lower()
    et = effect_text.lower()
    if "food generation" in et or bt == "granary":
        return ("food_generation_pct", "Food generation")
    if "wood maintenance" in et or bt == "carpenter":
        return ("wood_maintenance_pct", "Wood maintenance")
    if "stone maintenance" in et or bt == "mason":
        return ("stone_maintenance_pct", "Stone maintenance")
    if "houses" in et or bt == "housing":
        return ("house_population_pct", "House population")
    if "stables" in et:
        return ("stables_population_pct", "Stables population")
    if "soldiers per barracks" in et:
        return ("barracks_soldiers_pct", "Barracks soldier count")
    return (f"other:{building_type}", f"{building_type} effect")


def _aggregate_effects(settlements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    totals: Dict[str, Dict[str, Any]] = {}

    for s in settlements:
        sname = s.get("name") or f"Settlement {s.get('settlement_id')}"
        for b in s.get("buildings") or []:
            bt = str(b.get("building_type") or "").strip()
            if not bt:
                continue
            et = str(b.get("effect_text") or "").strip()
            level = int(b.get("level") or 0)
            delta = _extract_pct(et, level=level)
            if delta is None:
                continue
            cap = _extract_cap(et)
            k, label = _effect_key(bt, et)
            if k not in totals:
                totals[k] = {
                    "effect_key": k,
                    "label": label,
                    "total_pct": 0.0,
                    "cap_pct": cap,
                    "building_count": 0,
                    "sources": [],
                }
            rec = totals[k]
            rec["total_pct"] += float(delta)
            rec["building_count"] += 1
            rec["sources"].append(
                {
                    "settlement": sname,
                    "building_type": bt,
                    "level": level,
                    "delta_pct": float(delta),
                }
            )
            if cap is not None:
                existing_cap = rec.get("cap_pct")
                if existing_cap is None:
                    rec["cap_pct"] = cap
                else:
                    # Keep stricter cap if mixed data appears.
                    if cap >= 0:
                        rec["cap_pct"] = min(float(existing_cap), cap)
                    else:
                        rec["cap_pct"] = max(float(existing_cap), cap)

    out: List[Dict[str, Any]] = []
    for _k, rec in totals.items():
        total = float(rec["total_pct"])
        cap = rec.get("cap_pct")
        applied = total
        cap_reached = False
        if cap is not None:
            cap_f = float(cap)
            if cap_f >= 0:
                applied = min(total, cap_f)
                cap_reached = total > cap_f
            else:
                applied = max(total, cap_f)
                cap_reached = total < cap_f

        out.append(
            {
                "effect_key": rec["effect_key"],
                "label": rec["label"],
                "total_pct": round(total, 3),
                "cap_pct": round(float(cap), 3) if cap is not None else None,
                "applied_pct": round(applied, 3),
                "cap_reached": cap_reached,
                "building_count": rec["building_count"],
                "sources": rec["sources"],
            }
        )

    out.sort(key=lambda x: x["label"])
    return out


@router.get("/auth/discord/login")
def auth_discord_login():
    query = urlencode(
        {
            "client_id": _discord_client_id(),
            "redirect_uri": _discord_redirect_uri(),
            "response_type": "code",
            "scope": _auth_scope(),
            "prompt": "none",
        }
    )
    return RedirectResponse(url=f"{DISCORD_API_BASE}/oauth2/authorize?{query}", status_code=302)


@router.get("/auth/discord/callback")
def auth_discord_callback(code: str):
    token_url = f"{DISCORD_API_BASE}/oauth2/token"
    me_url = f"{DISCORD_API_BASE}/users/@me"

    data = {
        "client_id": _discord_client_id(),
        "client_secret": _discord_client_secret(),
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": _discord_redirect_uri(),
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        with httpx.Client(timeout=20.0) as client:
            tr = client.post(token_url, data=data, headers=headers)
            tr.raise_for_status()
            tok = tr.json()
            access_token = tok.get("access_token")
            if not access_token:
                raise HTTPException(status_code=401, detail="Discord login failed (no access token)")

            ur = client.get(me_url, headers={"Authorization": f"Bearer {access_token}"})
            ur.raise_for_status()
            user = ur.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Discord auth failed: {repr(e)}")

    jwt_token = _create_session_jwt(user)
    redirect_to = f"{_frontend_url()}/settlements"
    resp = RedirectResponse(url=redirect_to, status_code=302)
    resp.set_cookie(
        key=JWT_COOKIE_NAME,
        value=jwt_token,
        httponly=True,
        secure=_session_secure_cookie(),
        samesite="lax",
        max_age=_jwt_exp_hours() * 3600,
        path="/",
    )
    return resp


@router.post("/auth/logout")
def auth_logout():
    resp = RedirectResponse(url=f"{_frontend_url()}/", status_code=302)
    resp.delete_cookie(JWT_COOKIE_NAME, path="/")
    return resp


@router.get("/auth/me")
def auth_me(request: Request):
    token = request.cookies.get(JWT_COOKIE_NAME, "")
    if not token:
        return {"ok": True, "authenticated": False}
    try:
        claims = _decode_session_jwt(token)
        uid = str(claims.get("sub") or "")
        return {
            "ok": True,
            "authenticated": True,
            "user": {
                "discord_user_id": uid,
                "discord_username": str(claims.get("name") or ""),
                "avatar": claims.get("avatar"),
                "is_admin": uid in _admin_user_ids(),
            },
        }
    except HTTPException:
        return {"ok": True, "authenticated": False}


@router.get("/api/kg/connection")
def kg_connection(request: Request):
    user = _get_current_user(request)
    row = _load_user_kg_connection(user["discord_user_id"])
    if not row:
        return {"ok": True, "connected": False}
    return {
        "ok": True,
        "connected": True,
        "connection": {
            "account_id": int(row["account_id"]),
            "kingdom_id": int(row["kingdom_id"]),
            "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
            "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
        },
    }


@router.post("/api/kg/connect")
def kg_connect(body: KGConnectBody, request: Request):
    user = _get_current_user(request)
    _upsert_user_kg_connection(
        user["discord_user_id"],
        user["discord_username"],
        body.account_id,
        body.kingdom_id,
        body.token,
    )

    return {"ok": True, "connected": True}


@router.post("/api/kg/login")
def kg_login(body: KGLoginBody, request: Request, response: Response):
    user = _get_current_user(request)
    ip = request.client.host if request.client else "unknown"
    _rate_limit_login(ip)
    # Security policy: password is used one-time only for KG token exchange.
    # It is not persisted to DB or logs.
    response.headers["X-Password-Storage-Policy"] = "one-time-only-not-stored"

    email = body.email.strip()
    password = body.password
    account_id: Optional[int] = None
    token: Optional[str] = None
    kingdom_id = body.kingdom_id
    try:
        login = _kg_login(email, password)
        account_id = int(login["account_id"])
        token = str(login["token"])

        if kingdom_id is None:
            kingdom_id = _discover_kingdom_id(account_id, token)
        if kingdom_id is None:
            raise HTTPException(
                status_code=400,
                detail="KG login succeeded but kingdomId was not discovered automatically. Please provide kingdomId once.",
            )

        _upsert_user_kg_connection(
            user["discord_user_id"],
            user["discord_username"],
            account_id,
            int(kingdom_id),
            token,
        )
        print(
            f"[kg_login] connected discord_user_id={user['discord_user_id']} "
            f"account_id={account_id} kingdom_id={int(kingdom_id)} "
            "password_not_stored=true",
            flush=True,
        )
        return {
            "ok": True,
            "connected": True,
            "password_policy": "one-time-only-not-stored",
            "connection": {
                "account_id": account_id,
                "kingdom_id": int(kingdom_id),
            },
        }
    finally:
        # Defensive scrubbing of sensitive variables after exchange.
        password = None
        email = None
        token = None


@router.delete("/api/kg/connection")
def kg_disconnect(request: Request):
    user = _get_current_user(request)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM public.user_kg_connections WHERE discord_user_id = %s",
                (user["discord_user_id"],),
            )
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "connected": False}


@router.get("/api/kg/settlements")
def kg_settlements(request: Request):
    user = _get_current_user(request)
    conn_row = _require_user_kg_connection(user["discord_user_id"])
    settlements = _fetch_settlements_live(conn_row)
    return {"ok": True, "settlements": settlements}


@router.get("/api/kg/settlement-effects")
def kg_settlement_effects(request: Request):
    user = _get_current_user(request)
    conn_row = _require_user_kg_connection(user["discord_user_id"])
    settlements = _fetch_settlements_live(conn_row)
    effects = _aggregate_effects(settlements)
    return {
        "ok": True,
        "settlements_count": len(settlements),
        "effects": effects,
    }
