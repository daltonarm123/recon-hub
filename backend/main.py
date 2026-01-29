import os
import time
import secrets
from typing import Optional, Set, Dict, Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

import jwt  # PyJWT

# -------------------- Config --------------------
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI = os.getenv("DISCORD_REDIRECT_URI")  # https://.../auth/discord/callback
DISCORD_GUILD_ID = os.getenv("DISCORD_GUILD_ID")
DISCORD_RECON_ROLE_ID = os.getenv("DISCORD_RECON_ROLE_ID") or os.getenv("RECON_ROLE_ID")
ADMIN_DISCORD_IDS = os.getenv("ADMIN_DISCORD_IDS") or os.getenv("DEV_USER_IDS", "")
JWT_SECRET = os.getenv("JWT_SECRET")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

if not all([DISCORD_CLIENT_ID, DISCORD_CLIENT_SECRET, DISCORD_REDIRECT_URI, DISCORD_GUILD_ID, DISCORD_RECON_ROLE_ID, JWT_SECRET]):
    raise RuntimeError("Missing required env vars. Check DISCORD_* and JWT_SECRET.")

ADMIN_SET: Set[str] = set(x.strip() for x in ADMIN_DISCORD_IDS.split(",") if x.strip())

JWT_ISSUER = "recon-hub-api"
JWT_AUDIENCE = "recon-hub-web"
SESSION_COOKIE = "rh_session"
STATE_COOKIE = "rh_oauth_state"

# -------------------- App --------------------
app = FastAPI(title="Recon Hub API")

# CORS (allow your web app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------- Helpers --------------------
def _make_state() -> str:
    return secrets.token_urlsafe(32)

def _jwt_encode(payload: Dict[str, Any]) -> str:
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def _jwt_decode(token: str) -> Dict[str, Any]:
    return jwt.decode(token, JWT_SECRET, algorithms=["HS256"], issuer=JWT_ISSUER, audience=JWT_AUDIENCE)

def _cookie_opts(prod: bool) -> Dict[str, Any]:
    # Render uses HTTPS => secure cookies in prod
    return {
        "httponly": True,
        "secure": prod,
        "samesite": "lax",
        "path": "/",
    }

def _is_prod() -> bool:
    return os.getenv("RENDER", "").lower() == "true" or os.getenv("RENDER_EXTERNAL_URL") is not None

async def discord_exchange_code(code: str) -> Dict[str, Any]:
    token_url = "https://discord.com/api/oauth2/token"
    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(token_url, data=data, headers=headers)
        if r.status_code != 200:
            raise HTTPException(status_code=401, detail=f"Discord token exchange failed: {r.text}")
        return r.json()

async def discord_get_user(access_token: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            "https://discord.com/api/users/@me",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if r.status_code != 200:
            raise HTTPException(status_code=401, detail=f"Discord /users/@me failed: {r.text}")
        return r.json()

async def discord_get_member_roles(access_token: str, user_id: str) -> Set[str]:
    """
    Uses the OAuth token to call:
      GET /users/@me/guilds/{guild_id}/member
    Which returns the member object including roles[] (role IDs).
    Requires the user to authorize with scope: guilds.members.read
    """
    url = f"https://discord.com/api/users/@me/guilds/{DISCORD_GUILD_ID}/member"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, headers={"Authorization": f"Bearer {access_token}"})
        if r.status_code != 200:
            # If this fails, usually missing guilds.members.read scope or user not in server
            raise HTTPException(status_code=403, detail=f"Guild member lookup failed (is user in server / scopes ok?): {r.text}")

        data = r.json()
        roles = data.get("roles", []) or []
        return set(str(x) for x in roles)

def compute_access(user_id: str, roles: Set[str]) -> Dict[str, bool]:
    is_admin = str(user_id) in ADMIN_SET
    has_recon = str(DISCORD_RECON_ROLE_ID) in roles
    allowed = is_admin or has_recon
    return {"allowed": allowed, "is_admin": is_admin, "has_recon": has_recon}

def session_payload(user: Dict[str, Any], access: Dict[str, bool]) -> Dict[str, Any]:
    now = int(time.time())
    return {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "iat": now,
        "exp": now + 60 * 60 * 24 * 7,  # 7 days
        "discord_id": str(user["id"]),
        "username": user.get("username"),
        "global_name": user.get("global_name"),
        "avatar": user.get("avatar"),
        "is_admin": bool(access["is_admin"]),
        "has_recon": bool(access["has_recon"]),
    }


def get_session(request: Request) -> Optional[Dict[str, Any]]:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    try:
        return _jwt_decode(token)
    except Exception:
        return None


# -------------------- Routes --------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/auth/discord/login")
def discord_login():
    # IMPORTANT: include guilds.members.read so we can fetch roles
    # Also include identify.
    state = _make_state()
    scope = "identify guilds.members.read"
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": scope,
        "state": state,
        "prompt": "none",
    }

    # Build URL manually
    q = "&".join([f"{k}={httpx.QueryParams({k:v})[k]}" for k, v in params.items()])
    url = f"https://discord.com/api/oauth2/authorize?{q}"

    resp = RedirectResponse(url=url, status_code=302)
    resp.set_cookie(STATE_COOKIE, state, max_age=600, **_cookie_opts(_is_prod()))
    return resp

@app.get("/auth/discord/callback")
async def discord_callback(request: Request, code: str, state: str):
    expected = request.cookies.get(STATE_COOKIE)
    if not expected or state != expected:
        raise HTTPException(status_code=400, detail="Invalid OAuth state. Try logging in again.")

    token_data = await discord_exchange_code(code)
    access_token = token_data.get("access_token")
    if not access_token:
        raise HTTPException(status_code=401, detail="Missing access_token from Discord.")

    user = await discord_get_user(access_token)
    user_id = str(user["id"])

    roles = await discord_get_member_roles(access_token, user_id)
    access = compute_access(user_id, roles)

    if not access["allowed"]:
        # Redirect to frontend with denied flag
        return RedirectResponse(url=f"{FRONTEND_URL}/denied", status_code=302)

    payload = session_payload(user, access)
    session_jwt = _jwt_encode(payload)

    resp = RedirectResponse(url=f"{FRONTEND_URL}/", status_code=302)
    resp.delete_cookie(STATE_COOKIE, path="/")
    resp.set_cookie(SESSION_COOKIE, session_jwt, max_age=60 * 60 * 24 * 7, **_cookie_opts(_is_prod()))
    return resp

@app.post("/auth/logout")
def logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp

@app.get("/me")
def me(request: Request):
    sess = get_session(request)
    if not sess:
        return JSONResponse({"authenticated": False})
    return {
        "authenticated": True,
        "discord_id": sess.get("discord_id"),
        "username": sess.get("username"),
        "global_name": sess.get("global_name"),
        "avatar": sess.get("avatar"),
        "is_admin": sess.get("is_admin", False),
        "has_recon": sess.get("has_recon", False),
    }

def require_access(request: Request, admin_only: bool = False) -> Dict[str, Any]:
    sess = get_session(request)
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated.")
    if admin_only and not sess.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin only.")
    # Any logged-in user here already passed access check at login time,
    # but we keep this structure for later.
    return sess


# ---- Example protected endpoints (wire these to your DB logic) ----

@app.get("/api/kingdoms")
def list_kingdoms(request: Request):
    require_access(request)
    # TODO: query DB: SELECT DISTINCT kingdom FROM spy_reports WHERE kingdom IS NOT NULL ORDER BY kingdom;
    return {"kingdoms": []}

@app.get("/api/spy/latest")
def latest_spy(request: Request, kingdom: str):
    require_access(request)
    # TODO: query DB latest report for kingdom
    return {"kingdom": kingdom, "report": None}

@app.get("/api/admin/reindex")
def admin_reindex(request: Request):
    require_access(request, admin_only=True)
    # TODO: run techindex/backfill in background job or queued task
    return {"ok": True, "message": "Admin reindex requested."}
 
