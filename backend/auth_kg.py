import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
import jwt
import psycopg
import bcrypt
from cryptography.fernet import Fernet, InvalidToken
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse, JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from db_dsn import resolve_database_dsn

logger = logging.getLogger(__name__)

class AuthLoginBody(BaseModel):
    username: str = Field(..., min_length=3)
    password: str = Field(..., min_length=6)

def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))
    except Exception:
        return False


def _normalize_username(raw_username: str) -> str:
    username = str(raw_username or "").strip()
    if len(username) < 3:
        raise HTTPException(status_code=400, detail="Username must be at least 3 non-space characters")
    if len(username) > 64:
        raise HTTPException(status_code=400, detail="Username is too long")
    if any(ch in username for ch in {"\r", "\n", "\t"}):
        raise HTTPException(status_code=400, detail="Username contains invalid whitespace")
    return username

router = APIRouter()

DISCORD_API_BASE = "https://discord.com/api"
JWT_COOKIE_NAME = "rh_session"


class KGConnectBody(BaseModel):
    account_id: int = Field(..., gt=0)
    kingdom_id: int = Field(..., gt=0)
    token: str = Field(..., min_length=8)


class KGLoginBody(BaseModel):
    email: str = Field(..., min_length=3)
    password: str = Field(..., min_length=6)


class AllianceSwitchBody(BaseModel):
    alliance_id: int = Field(..., gt=0)


class PayPalCreateOrderBody(BaseModel):
    tier: str = Field(default="monthly", min_length=2, max_length=40)


class PayPalCaptureBody(BaseModel):
    order_id: str = Field(..., min_length=8, max_length=200)


def _get_dsn() -> str:
    try:
        return resolve_database_dsn()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
            cur.execute(
                """
                ALTER TABLE public.app_users
                ADD COLUMN IF NOT EXISTS is_premium BOOLEAN NOT NULL DEFAULT false;
                """
            )
            cur.execute(
                """
                ALTER TABLE public.app_users
                ADD COLUMN IF NOT EXISTS premium_tier TEXT;
                """
            )
            cur.execute(
                """
                ALTER TABLE public.app_users
                ADD COLUMN IF NOT EXISTS premium_since TIMESTAMPTZ;
                """
            )
            cur.execute(
                """
                ALTER TABLE public.app_users
                ADD COLUMN IF NOT EXISTS premium_expires_at TIMESTAMPTZ;
                """
            )
            cur.execute(
                """
                ALTER TABLE public.app_users
                ADD COLUMN IF NOT EXISTS premium_source TEXT;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.premium_payments (
                    id BIGSERIAL PRIMARY KEY,
                    discord_user_id TEXT NOT NULL REFERENCES public.app_users(discord_user_id) ON DELETE CASCADE,
                    provider TEXT NOT NULL DEFAULT 'paypal',
                    tier TEXT NOT NULL DEFAULT 'premium',
                    paypal_order_id TEXT,
                    paypal_capture_id TEXT,
                    payer_email TEXT,
                    status TEXT NOT NULL DEFAULT 'created',
                    amount NUMERIC(12,2),
                    currency TEXT,
                    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    activated_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(paypal_order_id),
                    UNIQUE(paypal_capture_id)
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS premium_payments_user_idx
                ON public.premium_payments (discord_user_id, created_at DESC);
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


def _set_session_cookie(response: Response, jwt_token: str):
    response.set_cookie(
        key=JWT_COOKIE_NAME,
        value=jwt_token,
        httponly=True,
        secure=_session_secure_cookie(),
        samesite="lax",
        max_age=_jwt_exp_hours() * 3600,
        path="/",
    )


def _clear_session_cookie(response: Response):
    response.delete_cookie(JWT_COOKIE_NAME, path="/")


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


def _admin_usernames() -> set[str]:
    raw = os.getenv("ADMIN_USERNAMES", "").strip()
    names = {"elixer"}
    if raw:
        names.update(x.strip().lower() for x in raw.split(",") if x.strip())
    return names


def _is_admin_identity(user_id: str, username: str) -> bool:
    uname = str(username or "").strip().lower()
    return user_id in _admin_user_ids() or uname in _admin_usernames()


def _paypal_mode() -> str:
    m = os.getenv("PAYPAL_MODE", "live").strip().lower()
    return "sandbox" if m == "sandbox" else "live"


def _paypal_base_url() -> str:
    return "https://api-m.sandbox.paypal.com" if _paypal_mode() == "sandbox" else "https://api-m.paypal.com"


def _paypal_client_id() -> str:
    v = os.getenv("PAYPAL_CLIENT_ID", "").strip()
    if not v:
        raise HTTPException(status_code=500, detail="PAYPAL_CLIENT_ID is not set")
    return v


def _paypal_client_secret() -> str:
    v = os.getenv("PAYPAL_CLIENT_SECRET", "").strip()
    if not v:
        raise HTTPException(status_code=500, detail="PAYPAL_CLIENT_SECRET is not set")
    return v


def _paypal_webhook_id() -> str:
    return os.getenv("PAYPAL_WEBHOOK_ID", "").strip()


def _billing_enabled() -> bool:
    return os.getenv("ENABLE_BILLING", "false").strip().lower() in {"1", "true", "yes", "on"}


def _require_billing_enabled():
    if not _billing_enabled():
        raise HTTPException(status_code=503, detail="Billing is not enabled")


def _money_text(value: Any, default_value: float) -> str:
    try:
        n = float(value)
    except Exception:
        n = float(default_value)
    if n <= 0:
        n = float(default_value)
    return f"{n:.2f}"


def _int_env(name: str, default_value: int) -> int:
    try:
        n = int(str(os.getenv(name, str(default_value))).strip())
    except Exception:
        n = default_value
    return n if n > 0 else default_value


def _normalize_premium_tier(tier_raw: Optional[str]) -> str:
    t = str(tier_raw or "monthly").strip().lower()
    alias = {
        "premium": "monthly",
        "month": "monthly",
        "1m": "monthly",
        "monthly": "monthly",
        "year": "annual",
        "12m": "annual",
        "annual": "annual",
    }
    return alias.get(t, "")


def _premium_plan_for_tier(tier_raw: Optional[str]) -> Dict[str, Any]:
    tier = _normalize_premium_tier(tier_raw)
    if not tier:
        raise HTTPException(status_code=400, detail="Invalid premium tier")

    if tier == "monthly":
        legacy_monthly = os.getenv("PREMIUM_PRICE_USD", "").strip()
        monthly_env = os.getenv("PREMIUM_MONTHLY_USD", "").strip() or legacy_monthly or "1.00"
        return {
            "tier": "monthly",
            "label": "Monthly",
            "amount_usd": _money_text(monthly_env, 1.00),
            "duration_days": _int_env("PREMIUM_MONTHLY_DAYS", 30),
        }
    return {
        "tier": "annual",
        "label": "Annual",
        "amount_usd": _money_text(os.getenv("PREMIUM_ANNUAL_USD", "10.00"), 10.00),
        "duration_days": _int_env("PREMIUM_ANNUAL_DAYS", 365),
    }


def _premium_plans() -> List[Dict[str, Any]]:
    return [
        _premium_plan_for_tier("monthly"),
        _premium_plan_for_tier("annual"),
    ]


def _compute_premium_expires_at(discord_user_id: str, duration_days: int) -> datetime:
    now_utc = datetime.now(timezone.utc)
    pctx = _load_premium_context(discord_user_id)
    current_expires = pctx.get("premium_expires_at")
    if isinstance(current_expires, datetime):
        if current_expires.tzinfo is None:
            current_expires = current_expires.replace(tzinfo=timezone.utc)
        base = current_expires if current_expires > now_utc else now_utc
    else:
        base = now_utc
    return base + timedelta(days=max(1, int(duration_days)))


def _load_premium_context(discord_user_id: str) -> Dict[str, Any]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT is_premium, premium_tier, premium_since, premium_expires_at, premium_source
                FROM public.app_users
                WHERE discord_user_id = %s
                """,
                (discord_user_id,),
            )
            row = cur.fetchone() or {}
            return {
                "is_premium": bool(row.get("is_premium") or False),
                "premium_tier": row.get("premium_tier"),
                "premium_since": row.get("premium_since"),
                "premium_expires_at": row.get("premium_expires_at"),
                "premium_source": row.get("premium_source"),
            }
    finally:
        conn.close()


def _set_user_premium(
    discord_user_id: str,
    *,
    enabled: bool,
    tier: str = "monthly",
    source: str = "paypal",
    expires_at: Optional[datetime] = None,
):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.app_users
                SET is_premium = %s,
                    premium_tier = %s,
                    premium_source = %s,
                    premium_since = CASE WHEN %s THEN COALESCE(premium_since, now()) ELSE NULL END,
                    premium_expires_at = %s,
                    updated_at = now()
                WHERE discord_user_id = %s
                """,
                (
                    bool(enabled),
                    (tier if enabled else None),
                    (source if enabled else None),
                    bool(enabled),
                    expires_at,
                    discord_user_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _has_premium_access(user: Dict[str, Any]) -> bool:
    return bool(user.get("is_admin") or user.get("is_premium"))


def _get_current_user(request: Request) -> Dict[str, Any]:
    token = request.cookies.get(JWT_COOKIE_NAME, "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated.")
    
    claims = _decode_session_jwt(token)
    uid = str(claims.get("sub") or "")
    base = {
        "discord_user_id": uid,
        "discord_username": str(claims.get("name") or ""),
        "avatar": claims.get("avatar"),
        "is_admin": _is_admin_identity(uid, str(claims.get("name") or "")),
    }
    try:
        pctx = _load_premium_context(uid)
        base.update(
            {
                "is_premium": bool(pctx.get("is_premium") or False),
                "premium_tier": pctx.get("premium_tier"),
                "premium_since": pctx.get("premium_since"),
                "premium_expires_at": pctx.get("premium_expires_at"),
                "premium_source": pctx.get("premium_source"),
            }
        )
    except Exception:
        base.update({"is_premium": False, "premium_tier": None})
    base["has_premium_access"] = _has_premium_access(base)
    return base


def _ensure_app_user(discord_user_id: str, discord_username: str):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.app_users
                  (discord_user_id, discord_username, created_at, updated_at)
                VALUES
                  (%s, %s, now(), now())
                ON CONFLICT (discord_user_id) DO UPDATE SET
                  discord_username = EXCLUDED.discord_username,
                  updated_at = now()
                """,
                (discord_user_id, discord_username),
            )
        conn.commit()
    finally:
        conn.close()


def _load_alliance_context(discord_user_id: str) -> Dict[str, Any]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id, a.slug, a.name, m.role, m.status
                FROM public.alliance_memberships m
                JOIN public.alliances a ON a.id = m.alliance_id
                WHERE m.discord_user_id = %s
                ORDER BY a.name
                """,
                (discord_user_id,),
            )
            memberships = cur.fetchall() or []

            cur.execute(
                """
                SELECT alliance_id
                FROM public.user_active_alliance
                WHERE discord_user_id = %s
                """,
                (discord_user_id,),
            )
            active_row = cur.fetchone() or {}

        active_id = int(active_row.get("alliance_id") or 0) or None
        return {
            "memberships": memberships,
            "active_alliance_id": active_id,
        }
    finally:
        conn.close()


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


def _compact_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def _origin_for_url(url: str) -> str:
    m = re.match(r"^(https?://[^/]+)", str(url or "").strip(), flags=re.I)
    return m.group(1) if m else "https://kingdomgame.net"


def _kg_headers(url: str, referer_path: str = "/settlements") -> Dict[str, str]:
    origin = _origin_for_url(url)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": f"{origin}{referer_path}",
        "World-Id": _kg_world_id(),
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
            token = m.group(1).strip()
            if token:
                headers["RequestVerificationToken"] = token
                headers["X-RequestVerificationToken"] = token

    extra_headers_raw = str(os.getenv("KG_EXTRA_HEADERS_JSON", "")).strip()
    if extra_headers_raw:
        try:
            extra = json.loads(extra_headers_raw)
            if isinstance(extra, dict):
                for key, value in extra.items():
                    key_str = str(key or "").strip()
                    if key_str:
                        headers[key_str] = str(value)
        except Exception:
            pass

    return headers


def _kg_login_urls() -> List[str]:
    primary = str(
        os.getenv("KG_USER_LOGIN_URL", "https://kingdomgame.net/WebService/User.asmx/Login")
    ).strip()
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

    out: List[str] = []
    seen = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _kg_login_headers(url: str) -> Dict[str, str]:
    return _kg_headers(url, "/login")


def _extract_request_verification_token(text: str) -> str:
    raw = str(text or "")
    patterns = [
        r'name="__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"',
        r'value="([^"]+)"\s+name="__RequestVerificationToken"',
        r'__RequestVerificationToken["\']?\s*[:=]\s*["\']([^"\']+)',
    ]
    for pattern in patterns:
        m = re.search(pattern, raw, flags=re.I)
        if m and m.group(1):
            return str(m.group(1)).strip()
    return ""


def _kg_bootstrap_login_session(client: httpx.Client, login_url: str) -> str:
    origin = _origin_for_url(login_url)
    seed_paths = ["/", "/login", "/rankings"]

    for path in seed_paths:
        try:
            response = client.get(
                f"{origin}{path}",
                headers=_kg_headers(login_url, path),
                follow_redirects=True,
            )
        except Exception:
            continue

        cookie_token = ""
        try:
            cookie_token = str(client.cookies.get("__RequestVerificationToken") or "").strip()
        except Exception:
            cookie_token = ""
        if cookie_token:
            return cookie_token

        html_token = _extract_request_verification_token(getattr(response, "text", ""))
        if html_token:
            return html_token

    return ""


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


def _parse_kg_response_text(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
    except Exception:
        return {}
    if isinstance(loaded, dict):
        parsed = _parse_kg_resp_json(loaded)
        return parsed if parsed else loaded
    return {}


def _parse_kg_response(resp: httpx.Response) -> Dict[str, Any]:
    raw: Dict[str, Any] = {}
    try:
        loaded = resp.json()
        if isinstance(loaded, dict):
            raw = loaded
    except Exception:
        text = (resp.text or "").strip()
        if text:
            try:
                loaded = json.loads(text)
                if isinstance(loaded, dict):
                    raw = loaded
            except Exception:
                raw = {}
    parsed = _parse_kg_resp_json(raw)
    return parsed if parsed else raw


def _extract_login_token(parsed: Dict[str, Any]) -> Tuple[str, Optional[int], Optional[int]]:
    token = str(
        parsed.get("token")
        or parsed.get("Token")
        or parsed.get("accessToken")
        or parsed.get("AccessToken")
        or ""
    ).strip()
    account_id = parsed.get("accountId") or parsed.get("AccountId") or parsed.get("accountID")
    kingdom_id = parsed.get("kingdomId") or parsed.get("KingdomId") or parsed.get("kingdomID")

    try:
        account_id_i = int(account_id) if account_id is not None else None
    except Exception:
        account_id_i = None
    try:
        kingdom_id_i = int(kingdom_id) if kingdom_id is not None else None
    except Exception:
        kingdom_id_i = None

    return token, account_id_i, kingdom_id_i


def _first_non_none(d: Any, *keys: str) -> Any:
    """Return the first non-None value from a dictionary for the provided keys."""
    if not isinstance(d, dict):
        raise TypeError(f"Expected a dictionary, got {type(d).__name__}")
    for key in keys:
        value = d.get(key)
        if value is not None:
            return value
    return None


def _fetch_kingdom_id_from_kg_api(login_url: str, account_id: int, token: str) -> Optional[int]:
    """Fetch KG kingdoms for a login token and return the first kingdom id found."""
    try:
        kingdoms = _kg_post_json(
            f"{_origin_for_url(login_url)}/WebService/Kingdoms.asmx/GetKingdoms",
            {
                "accountId": account_id,
                "token": token,
            },
        )
    except Exception as exc:
        logger.debug("Failed to resolve KG login kingdom id: %s", exc, exc_info=True)
        return None

    for row in _extract_list(kingdoms, ["kingdoms", "Kingdoms"]):
        if not isinstance(row, dict):
            continue
        kingdom_id = _first_non_none(row, "id", "Id", "kingdomId", "KingdomId")
        try:
            if kingdom_id is not None:
                return int(kingdom_id)
        except (TypeError, ValueError) as exc:
            logger.debug("Ignoring invalid KG kingdom id %r: %s", kingdom_id, exc)
            continue
    return None


def _kg_login_page_url(login_url: str) -> str:
    """Build the browser login page URL from the KG API login URL."""
    return f"{_origin_for_url(login_url)}/login"


def _kg_login_credential(email: str, password: str) -> Dict[str, Any]:
    email = str(email or "").strip()
    password = str(password or "")
    if not email or not password:
        raise HTTPException(status_code=400, detail="KG email and password are required")

    payload_variants = [
        {"email": email, "password": password},
        {"Email": email, "Password": password},
        {"username": email, "password": password},
    ]

    last_error = "KG login failed"
    found_partial_login = False
    with httpx.Client(timeout=30.0) as client:
        bootstrap_token = ""
        for url in _kg_login_urls():
            bootstrap_token = _kg_bootstrap_login_session(client, url)
            headers = _kg_login_headers(url)
            if bootstrap_token:
                headers["RequestVerificationToken"] = bootstrap_token
                headers["X-RequestVerificationToken"] = bootstrap_token
            for payload in payload_variants:
                try:
                    response = client.post(url, headers=headers, content=_compact_json(payload))
                    parsed = _parse_kg_response(response)
                    response.raise_for_status()
                    token, account_id, kingdom_id = _extract_login_token(parsed)
                    if token and account_id is not None and kingdom_id is None:
                        kingdom_id = _fetch_kingdom_id_from_kg_api(url, account_id, token)
                    if token and account_id is not None and kingdom_id is not None:
                        return {
                            "token": token,
                            "account_id": account_id,
                            "kingdom_id": kingdom_id,
                        }
                    if token and account_id is not None:
                        last_error = "KG login response missing kingdom id"
                        found_partial_login = True
                        break
                    last_error = "KG login response missing token/account"
                except Exception as exc:
                    resp = getattr(exc, "response", None)
                    body = ""
                    if resp is not None:
                        try:
                            body = (resp.text or "").strip().replace("\n", " ")[:220]
                        except Exception:
                            body = ""
                        status = getattr(resp, "status_code", "?")
                        last_error = f"HTTP {status} for {url} body={body}"
                    else:
                        last_error = str(exc)
            if found_partial_login:
                break

    raise HTTPException(status_code=502, detail=last_error or "KG login failed")


def _kg_browser_login_credential(email: str, password: str) -> Dict[str, Any]:
    email = str(email or "").strip()
    password = str(password or "")
    if not email or not password:
        raise HTTPException(status_code=400, detail="KG email and password are required")

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="KG browser login is not available on this deployment",
        ) from exc

    login_url = _kg_login_urls()[0]
    captured: Dict[str, Any] = {"token": "", "account_id": None, "kingdom_id": None}
    browser_error = "KG browser login failed"

    def capture_from_text(text: str):
        nonlocal captured
        parsed = _parse_kg_response_text(text)
        if not parsed:
            return
        token, account_id, kingdom_id = _extract_login_token(parsed)
        if token:
            captured["token"] = token
        if account_id is not None:
            captured["account_id"] = account_id
        if kingdom_id is not None:
            captured["kingdom_id"] = kingdom_id

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=os.getenv(
                    "KG_USER_AGENT",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
                )
            )
            page = context.new_page()

            def handle_response(response):
                nonlocal browser_error
                url = str(response.url or "")
                low = url.lower()
                if "/webservice/user.asmx/login" not in low and "/webservice/kingdoms.asmx/getkingdoms" not in low:
                    return
                try:
                    capture_from_text(response.text())
                except Exception as exc:
                    browser_error = str(exc)

            page.on("response", handle_response)

            try:
                page.goto(_kg_login_page_url(login_url), wait_until="domcontentloaded", timeout=45000)
                page.wait_for_load_state("networkidle", timeout=20000)

                email_selectors = [
                    'input[type="email"]',
                    'input[name="email"]',
                    'input[placeholder*="email" i]',
                    'input[autocomplete="username"]',
                ]
                password_selectors = [
                    'input[type="password"]',
                    'input[name="password"]',
                    'input[autocomplete="current-password"]',
                ]
                button_selectors = [
                    'button:has-text("Login")',
                    'button:has-text("Log In")',
                    'button:has-text("Sign In")',
                    'input[type="submit"]',
                ]

                email_locator = None
                for selector in email_selectors:
                    locator = page.locator(selector).first
                    try:
                        locator.wait_for(state="visible", timeout=5000)
                        email_locator = locator
                        break
                    except Exception:
                        continue
                if email_locator is None:
                    raise HTTPException(status_code=502, detail="KG login form email field not found")

                password_locator = None
                for selector in password_selectors:
                    locator = page.locator(selector).first
                    try:
                        locator.wait_for(state="visible", timeout=5000)
                        password_locator = locator
                        break
                    except Exception:
                        continue
                if password_locator is None:
                    raise HTTPException(status_code=502, detail="KG login form password field not found")

                email_locator.fill(email)
                password_locator.fill(password)

                clicked = False
                for selector in button_selectors:
                    locator = page.locator(selector).first
                    try:
                        locator.wait_for(state="visible", timeout=3000)
                        locator.click()
                        clicked = True
                        break
                    except Exception:
                        continue
                if not clicked:
                    password_locator.press("Enter")

                try:
                    page.wait_for_load_state("networkidle", timeout=30000)
                except PlaywrightTimeoutError:
                    pass

                deadline = time.time() + 30.0
                while time.time() < deadline:
                    if captured.get("token") and captured.get("account_id") is not None:
                        break
                    page.wait_for_timeout(250)

                if not captured.get("token") or captured.get("account_id") is None:
                    raise HTTPException(status_code=502, detail=browser_error or "KG browser login did not return a token")

                if captured.get("kingdom_id") is None:
                    captured["kingdom_id"] = _fetch_kingdom_id_from_kg_api(
                        login_url,
                        int(captured["account_id"]),
                        str(captured["token"]),
                    )

                if captured.get("kingdom_id") is None:
                    raise HTTPException(status_code=502, detail="KG browser login succeeded but no kingdom id was found")

                return {
                    "token": str(captured["token"]),
                    "account_id": int(captured["account_id"]),
                    "kingdom_id": int(captured["kingdom_id"]),
                }
            finally:
                try:
                    context.close()
                except Exception:
                    pass
                browser.close()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"KG browser login failed: {exc}") from exc


def _kg_post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    with httpx.Client(timeout=30.0) as client:
        try:
            r = client.post(url, headers=_kg_headers(url), content=_compact_json(payload))
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


def _ci_get(d: Dict[str, Any], *keys: str) -> Any:
    """
    Case-insensitive dict getter across multiple candidate keys.
    """
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    lower_map = {str(k).lower(): v for k, v in d.items()}
    for k in keys:
        lk = str(k).lower()
        if lk in lower_map:
            return lower_map[lk]
    return None


def _extract_settlements(payload: Dict[str, Any], account_id: Optional[int] = None) -> List[Dict[str, Any]]:
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
        
        # Filter out settlements that do not belong to the user's account
        if account_id is not None:
            owner_id = _ci_get(item, "accountId", "accountID", "ownerId", "ownerID", "playerId", "playerID")
            if owner_id is not None:
                try:
                    if int(owner_id) != int(account_id):
                        return
                except Exception:
                    pass

        sid_key = None
        sid = None
        for candidate_key in ("settlementId", "settlementID", "cityId", "cityID", "townId", "townID", "id"):
            value = _ci_get(item, candidate_key)
            if value is not None:
                sid_key = candidate_key
                sid = value
                break
        name = _ci_get(item, "name", "settlementName", "cityName", "townName")
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
                "detail_id_key": sid_key or "settlementId",
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
                        if any(_ci_get(item, k) is not None for k in ("settlementId", "settlementID", "cityId", "cityID", "townId", "townID")):
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
        btype = _ci_get(
            row,
            "buildingType",
            "building_type",
            "buildingtypename",
            "typeName",
            "type",
            "name",
            "buildingName",
        )
        level = _ci_get(
            row,
            "level",
            "lvl",
            "buildingLevel",
            "building_lvl",
            "currentLevel",
        )
        effect = _ci_get(
            row,
            "effect",
            "effectText",
            "description",
            "text",
            "bonus",
            "effectDescription",
        )
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
                settlements = _extract_settlements(parsed, account_id=int(base["accountId"]))
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
        payload_base = {
            "accountId": base["accountId"],
            "token": base["token"],
            "kingdomId": int(base["kingdomId"]),
        }
        source_id_key = str(s.get("detail_id_key") or "settlementId")
        if source_id_key.lower().startswith("city"):
            detail_id_keys = ["cityId", "cityID", "settlementId", "settlementID"]
        elif source_id_key.lower().startswith("town"):
            detail_id_keys = ["townId", "townID", "settlementId", "settlementID"]
        else:
            detail_id_keys = ["settlementId", "settlementID"]

        payload_variants: List[Dict[str, Any]] = []
        seen_payloads = set()

        for detail_id_key in detail_id_keys:
            payload = {**payload_base, detail_id_key: sid}
            payload_sig = tuple(sorted(payload.items()))
            if payload_sig not in seen_payloads:
                payload_variants.append(payload)
                seen_payloads.add(payload_sig)

        for detail_id_key in detail_id_keys:
            payload = {
                "accountID": str(base["accountId"]),
                "token": base["token"],
                "kingdomID": int(base["kingdomId"]),
                detail_id_key: sid,
            }
            payload_sig = tuple(sorted(payload.items()))
            if payload_sig not in seen_payloads:
                payload_variants.append(payload)
                seen_payloads.add(payload_sig)

        for p in payload_variants:
            try:
                parsed = _kg_post_json(primary_detail_url, p)
                buildings = _extract_buildings(parsed)
                if buildings and not _is_summary_only_buildings(buildings):
                    return (sid, buildings)
            except Exception:
                pass

        for url in fallback_detail_urls:
            for p in payload_variants:
                try:
                    parsed = _kg_post_json(url, p)
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
        if "detail_id_key" in s:
            del s["detail_id_key"]
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



@router.post("/auth/register")
def auth_register(body: AuthLoginBody):
    username = _normalize_username(body.username)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT discord_user_id FROM public.app_users WHERE LOWER(discord_username) = LOWER(%s)", (username,))
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="Username already exists")
            
            hashed = hash_password(body.password)
            import uuid
            user_id = str(uuid.uuid4())
            
            cur.execute(
                """
                INSERT INTO public.app_users (discord_user_id, discord_username, password_hash, created_at, updated_at)
                VALUES (%s, %s, %s, now(), now())
                """,
                (user_id, username, hashed)
            )
        conn.commit()
    finally:
        conn.close()

    jwt_token = _create_session_jwt({"id": user_id, "username": username, "avatar": None})
    resp = JSONResponse(content={"ok": True, "message": "Account created"})
    _set_session_cookie(resp, jwt_token)
    return resp

@router.post("/auth/login")
def auth_login(body: AuthLoginBody):
    username = _normalize_username(body.username)
    conn = _connect()
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT discord_user_id, discord_username, password_hash FROM public.app_users WHERE LOWER(discord_username) = LOWER(%s)", (username,))
            user = cur.fetchone()
            if not user or not user["password_hash"] or not verify_password(body.password, user["password_hash"]):
                raise HTTPException(status_code=401, detail="Invalid username or password")
    finally:
        conn.close()

    jwt_token = _create_session_jwt(
        {"id": user["discord_user_id"], "username": user["discord_username"], "avatar": None}
    )
    
    resp = JSONResponse(content={"ok": True, "message": "Logged in"})
    _set_session_cookie(resp, jwt_token)
    return resp

@router.post("/auth/logout")
def auth_logout(request: Request):
    accept = (request.headers.get("accept") or "").lower()
    if "text/html" in accept and "application/json" not in accept:
        resp = RedirectResponse(url=_frontend_url(), status_code=302)
    else:
        resp = JSONResponse(content={"ok": True})
    _clear_session_cookie(resp)
    return resp


def _paypal_get_access_token() -> str:
    url = f"{_paypal_base_url()}/v1/oauth2/token"
    with httpx.Client(timeout=20.0) as client:
        r = client.post(
            url,
            data={"grant_type": "client_credentials"},
            auth=(_paypal_client_id(), _paypal_client_secret()),
            headers={"Accept": "application/json", "Accept-Language": "en_US"},
        )
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"PayPal token failed ({r.status_code})")
        j = r.json()
        tok = str(j.get("access_token") or "").strip()
        if not tok:
            raise HTTPException(status_code=502, detail="PayPal token missing")
        return tok


def _paypal_verify_webhook_signature(headers: Dict[str, str], body: Dict[str, Any]) -> bool:
    webhook_id = _paypal_webhook_id()
    if not webhook_id:
        return True
    token = _paypal_get_access_token()
    payload = {
        "auth_algo": headers.get("paypal-auth-algo"),
        "cert_url": headers.get("paypal-cert-url"),
        "transmission_id": headers.get("paypal-transmission-id"),
        "transmission_sig": headers.get("paypal-transmission-sig"),
        "transmission_time": headers.get("paypal-transmission-time"),
        "webhook_id": webhook_id,
        "webhook_event": body,
    }
    with httpx.Client(timeout=20.0) as client:
        r = client.post(
            f"{_paypal_base_url()}/v1/notifications/verify-webhook-signature",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=payload,
        )
        if r.status_code >= 400:
            return False
        j = r.json()
        return str(j.get("verification_status") or "").upper() == "SUCCESS"


def _premium_upsert_payment(
    discord_user_id: str,
    *,
    tier: str = "monthly",
    order_id: Optional[str] = None,
    capture_id: Optional[str] = None,
    status: str = "created",
    amount: Optional[str] = None,
    currency: Optional[str] = None,
    payer_email: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    activate: bool = False,
):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if not order_id and capture_id:
                cur.execute(
                    """
                    UPDATE public.premium_payments
                    SET
                      discord_user_id = COALESCE(%s, discord_user_id),
                      tier = COALESCE(%s, tier),
                      payer_email = COALESCE(%s, payer_email),
                      status = %s,
                      amount = COALESCE(%s, amount),
                      currency = COALESCE(%s, currency),
                      payload_json = %s::jsonb,
                      activated_at = COALESCE(%s, activated_at),
                      updated_at = now()
                    WHERE paypal_capture_id = %s
                    RETURNING id
                    """,
                    (
                        discord_user_id,
                        tier,
                        payer_email,
                        status,
                        amount,
                        currency,
                        json.dumps(payload or {}),
                        (datetime.now(timezone.utc) if activate else None),
                        capture_id,
                    ),
                )
                existing = cur.fetchone()
                if existing:
                    conn.commit()
                    if activate:
                        plan = _premium_plan_for_tier(tier)
                        expires_at = _compute_premium_expires_at(discord_user_id, int(plan["duration_days"]))
                        _set_user_premium(
                            discord_user_id,
                            enabled=True,
                            tier=plan["tier"],
                            source="paypal",
                            expires_at=expires_at,
                        )
                    return

            cur.execute(
                """
                INSERT INTO public.premium_payments
                  (discord_user_id, provider, tier, paypal_order_id, paypal_capture_id, payer_email, status, amount, currency, payload_json, activated_at, updated_at)
                VALUES
                  (%s, 'paypal', %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, now())
                ON CONFLICT (paypal_order_id) DO UPDATE SET
                  tier = COALESCE(EXCLUDED.tier, public.premium_payments.tier),
                  paypal_capture_id = COALESCE(EXCLUDED.paypal_capture_id, public.premium_payments.paypal_capture_id),
                  payer_email = COALESCE(EXCLUDED.payer_email, public.premium_payments.payer_email),
                  status = EXCLUDED.status,
                  amount = COALESCE(EXCLUDED.amount, public.premium_payments.amount),
                  currency = COALESCE(EXCLUDED.currency, public.premium_payments.currency),
                  payload_json = EXCLUDED.payload_json,
                  activated_at = COALESCE(EXCLUDED.activated_at, public.premium_payments.activated_at),
                  updated_at = now()
                """,
                (
                    discord_user_id,
                    tier,
                    order_id,
                    capture_id,
                    payer_email,
                    status,
                    amount,
                    currency,
                    json.dumps(payload or {}),
                    (datetime.now(timezone.utc) if activate else None),
                ),
            )
        conn.commit()
    finally:
        conn.close()

    if activate:
        plan = _premium_plan_for_tier(tier)
        expires_at = _compute_premium_expires_at(discord_user_id, int(plan["duration_days"]))
        _set_user_premium(
            discord_user_id,
            enabled=True,
            tier=plan["tier"],
            source="paypal",
            expires_at=expires_at,
        )


@router.get("/api/billing/premium-status")
def billing_premium_status(request: Request):
    user = _get_current_user(request)
    p = _load_premium_context(user["discord_user_id"])
    is_premium = bool(p.get("is_premium") or False)
    has_access = bool(user.get("is_admin") or is_premium)
    return {
        "ok": True,
        "enabled": _billing_enabled(),
        "premium": {
            "is_premium": is_premium,
            "tier": p.get("premium_tier"),
            "since": p.get("premium_since"),
            "expires_at": p.get("premium_expires_at"),
            "source": p.get("premium_source"),
            "has_access": has_access,
        },
        "plans": (_premium_plans() if _billing_enabled() else []),
    }


@router.post("/api/billing/paypal/create-order")
def billing_paypal_create_order(body: PayPalCreateOrderBody, request: Request):
    _require_billing_enabled()
    user = _get_current_user(request)
    _ensure_app_user(user["discord_user_id"], user["discord_username"])
    token = _paypal_get_access_token()

    frontend = _frontend_url()
    return_url = f"{frontend}/research?billing=paypal_return"
    cancel_url = f"{frontend}/research?billing=paypal_cancel"
    plan = _premium_plan_for_tier(body.tier)
    amount = str(plan["amount_usd"])
    tier = str(plan["tier"])

    with httpx.Client(timeout=25.0) as client:
        r = client.post(
            f"{_paypal_base_url()}/v2/checkout/orders",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "PayPal-Request-Id": f"rh_{user['discord_user_id']}_{int(datetime.now(timezone.utc).timestamp())}",
            },
            json={
                "intent": "CAPTURE",
                "purchase_units": [
                    {
                        "reference_id": tier,
                        "custom_id": f"{user['discord_user_id']}|{tier}",
                        "description": f"Recon Hub Premium - {plan['label']}",
                        "amount": {"currency_code": "USD", "value": amount},
                    }
                ],
                "application_context": {
                    "return_url": return_url,
                    "cancel_url": cancel_url,
                    "brand_name": "Recon Hub",
                    "user_action": "PAY_NOW",
                    "shipping_preference": "NO_SHIPPING",
                },
            },
        )
        if r.status_code >= 400:
            detail = r.text[:300] if r.text else f"PayPal order create failed ({r.status_code})"
            raise HTTPException(status_code=502, detail=detail)
        j = r.json()
        order_id = str(j.get("id") or "")
        approve_url = None
        for link in (j.get("links") or []):
            if str(link.get("rel") or "").lower() == "approve":
                approve_url = link.get("href")
                break
        if not order_id:
            raise HTTPException(status_code=502, detail="PayPal order id missing")

        _premium_upsert_payment(
            user["discord_user_id"],
            tier=tier,
            order_id=order_id,
            status="created",
            amount=amount,
            currency="USD",
            payload=j,
            activate=False,
        )

        return {
            "ok": True,
            "order_id": order_id,
            "approve_url": approve_url,
            "tier": tier,
            "amount_usd": amount,
            "duration_days": plan["duration_days"],
            "label": plan["label"],
        }


@router.post("/api/billing/paypal/capture")
def billing_paypal_capture(body: PayPalCaptureBody, request: Request):
    _require_billing_enabled()
    user = _get_current_user(request)
    order_id = str(body.order_id or "").strip()
    if not order_id:
        raise HTTPException(status_code=400, detail="order_id is required")

    token = _paypal_get_access_token()
    with httpx.Client(timeout=25.0) as client:
        r = client.post(
            f"{_paypal_base_url()}/v2/checkout/orders/{order_id}/capture",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        if r.status_code >= 400:
            detail = r.text[:300] if r.text else f"PayPal capture failed ({r.status_code})"
            raise HTTPException(status_code=502, detail=detail)
        j = r.json()
    status = str(j.get("status") or "").upper()
    if status != "COMPLETED":
        raise HTTPException(status_code=400, detail=f"Capture not completed: {status or 'UNKNOWN'}")

    cap_id = None
    payer_email = None
    amount = None
    currency = None
    tier = "monthly"
    try:
        payer_email = (j.get("payer") or {}).get("email_address")
        pu = (j.get("purchase_units") or [None])[0] or {}
        tier = _normalize_premium_tier(pu.get("reference_id")) or "monthly"
        captures = (((pu.get("payments") or {}).get("captures")) or [None])[0] or {}
        cap_id = captures.get("id")
        money = captures.get("amount") or {}
        amount = money.get("value")
        currency = money.get("currency_code")
    except Exception:
        pass

    _premium_upsert_payment(
        user["discord_user_id"],
        tier=tier,
        order_id=order_id,
        capture_id=cap_id,
        payer_email=payer_email,
        status="captured",
        amount=amount,
        currency=currency,
        payload=j,
        activate=True,
    )
    return {
        "ok": True,
        "order_id": order_id,
        "capture_id": cap_id,
        "status": "captured",
        "is_premium": True,
        "tier": tier,
    }


@router.post("/api/billing/paypal/webhook")
async def billing_paypal_webhook(request: Request):
    _require_billing_enabled()
    body = await request.json()
    hdr = {k.lower(): v for k, v in request.headers.items()}
    if not _paypal_verify_webhook_signature(hdr, body):
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    event_type = str(body.get("event_type") or "").upper()
    resource = body.get("resource") or {}
    if event_type != "PAYMENT.CAPTURE.COMPLETED":
        return {"ok": True, "ignored": True, "event_type": event_type}

    cap_id = str(resource.get("id") or "")
    amount = (resource.get("amount") or {}).get("value")
    currency = (resource.get("amount") or {}).get("currency_code")
    payer_email = ((resource.get("payer") or {}).get("email_address")) or None
    custom_id = str(resource.get("custom_id") or "").strip()
    order_id = str((resource.get("supplementary_data") or {}).get("related_ids", {}).get("order_id") or "").strip()

    discord_user_id = ""
    tier = "monthly"
    if custom_id:
        parts = custom_id.split("|", 1)
        discord_user_id = parts[0].strip()
        if len(parts) > 1:
            tier = _normalize_premium_tier(parts[1]) or "monthly"
    if not discord_user_id and order_id:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT discord_user_id, tier FROM public.premium_payments WHERE paypal_order_id = %s LIMIT 1",
                    (order_id,),
                )
                row = cur.fetchone() or {}
                discord_user_id = str(row.get("discord_user_id") or "").strip()
                tier = _normalize_premium_tier(row.get("tier")) or tier
        finally:
            conn.close()

    if not discord_user_id:
        return {"ok": True, "ignored": True, "reason": "no_user_mapping", "event_type": event_type}

    _premium_upsert_payment(
        discord_user_id,
        tier=tier,
        order_id=(order_id or None),
        capture_id=(cap_id or None),
        payer_email=payer_email,
        status="captured_webhook",
        amount=amount,
        currency=currency,
        payload=body,
        activate=True,
    )
    return {
        "ok": True,
        "event_type": event_type,
        "discord_user_id": discord_user_id,
        "is_premium": True,
        "tier": tier,
    }


@router.get("/auth/me")
def auth_me(request: Request, response: Response):
    token = request.cookies.get(JWT_COOKIE_NAME, "")
    if not token:
        return {
            "ok": True,
            "authenticated": False,
            "user": None,
        }
    try:
        claims = _decode_session_jwt(token)
        uid = str(claims.get("sub") or "")
        uname = str(claims.get("name") or "")
        _ensure_app_user(uid, uname)
        actx = _load_alliance_context(uid)
        pctx = _load_premium_context(uid)
        return {
            "ok": True,
            "authenticated": True,
            "user": {
                "discord_user_id": uid,
                "discord_username": uname,
                "avatar": claims.get("avatar"),
                "is_admin": _is_admin_identity(uid, uname),
                "is_premium": bool(pctx.get("is_premium") or False),
                "has_premium_access": _is_admin_identity(uid, uname) or bool(pctx.get("is_premium") or False),
                "premium_tier": pctx.get("premium_tier"),
                "premium_since": pctx.get("premium_since"),
                "premium_expires_at": pctx.get("premium_expires_at"),
                "premium_source": pctx.get("premium_source"),
                "active_alliance_id": actx.get("active_alliance_id"),
                "alliances": [
                    {
                        "id": int(m["id"]),
                        "slug": str(m["slug"]),
                        "name": str(m["name"]),
                        "role": str(m["role"] or "member"),
                        "status": str(m["status"] or "active"),
                    }
                    for m in (actx.get("memberships") or [])
                ],
            },
        }
    except HTTPException:
        return {"ok": True, "authenticated": False}


@router.get("/api/alliance/me")
def alliance_me(request: Request):
    user = _get_current_user(request)
    _ensure_app_user(user["discord_user_id"], user["discord_username"])
    actx = _load_alliance_context(user["discord_user_id"])
    return {
        "ok": True,
        "active_alliance_id": actx.get("active_alliance_id"),
        "alliances": [
            {
                "id": int(m["id"]),
                "slug": str(m["slug"]),
                "name": str(m["name"]),
                "role": str(m["role"] or "member"),
                "status": str(m["status"] or "active"),
            }
            for m in (actx.get("memberships") or [])
        ],
    }


@router.post("/api/alliance/switch")
def alliance_switch(body: AllianceSwitchBody, request: Request):
    user = _get_current_user(request)
    _ensure_app_user(user["discord_user_id"], user["discord_username"])

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM public.alliance_memberships
                WHERE discord_user_id = %s
                  AND alliance_id = %s
                  AND status = 'active'
                """,
                (user["discord_user_id"], body.alliance_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=403, detail="Not a member of that alliance")

            cur.execute(
                """
                INSERT INTO public.user_active_alliance
                  (discord_user_id, alliance_id, updated_at)
                VALUES
                  (%s, %s, now())
                ON CONFLICT (discord_user_id) DO UPDATE SET
                  alliance_id = EXCLUDED.alliance_id,
                  updated_at = now()
                """,
                (user["discord_user_id"], body.alliance_id),
            )
        conn.commit()
    finally:
        conn.close()

    actx = _load_alliance_context(user["discord_user_id"])
    return {
        "ok": True,
        "active_alliance_id": actx.get("active_alliance_id"),
    }


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
def kg_login(body: KGLoginBody, request: Request):
    user = _get_current_user(request)
    try:
        cred = _kg_browser_login_credential(body.email, body.password)
    except HTTPException as exc:
        if exc.status_code == 503:
            raise
        cred = _kg_login_credential(body.email, body.password)
    _upsert_user_kg_connection(
        user["discord_user_id"],
        user["discord_username"],
        int(cred["account_id"]),
        int(cred["kingdom_id"]),
        str(cred["token"]),
    )
    return {
        "ok": True,
        "connected": True,
        "connection": {
            "account_id": int(cred["account_id"]),
            "kingdom_id": int(cred["kingdom_id"]),
        },
    }


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
