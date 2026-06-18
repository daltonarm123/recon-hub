with open("auth_kg_base.py", "r") as f:
    text = f.read()

import re

# 1. Update IMPORTS and models
imports_injection = """
import bcrypt
from fastapi.responses import RedirectResponse, JSONResponse

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
"""
text = text.replace("from fastapi.responses import RedirectResponse", imports_injection)

# 2. Replace discord login & callback with register & login
auth_endpoints = """
@router.post("/auth/register")
def auth_register(body: AuthLoginBody):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT discord_user_id FROM public.app_users WHERE LOWER(discord_username) = LOWER(%s)", (body.username.strip(),))
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="Username already exists")
            
            hashed = hash_password(body.password)
            import uuid
            user_id = str(uuid.uuid4())
            uname = body.username.strip()
            
            cur.execute(
                \"\"\"
                INSERT INTO public.app_users (discord_user_id, discord_username, password_hash, created_at, updated_at)
                VALUES (%s, %s, %s, now(), now())
                \"\"\",
                (user_id, uname, hashed)
            )
        conn.commit()
    finally:
        conn.close()

    payload = {
        "sub": user_id,
        "name": uname,
        "avatar": None,
        "iat": int(datetime.utcnow().timestamp()),
        "exp": int((datetime.utcnow() + timedelta(hours=_jwt_exp_hours())).timestamp()),
    }
    jwt_token = jwt.encode(payload, _jwt_secret(), algorithm="HS256")
    resp = JSONResponse(content={"ok": True, "message": "Account created"})
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

@router.post("/auth/login")
def auth_login(body: AuthLoginBody):
    conn = _connect()
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT discord_user_id, discord_username, password_hash FROM public.app_users WHERE LOWER(discord_username) = LOWER(%s)", (body.username.strip(),))
            user = cur.fetchone()
            if not user or not user["password_hash"] or not verify_password(body.password, user["password_hash"]):
                raise HTTPException(status_code=401, detail="Invalid username or password")
    finally:
        conn.close()

    payload = {
        "sub": user["discord_user_id"],
        "name": user["discord_username"],
        "avatar": None,
        "iat": int(datetime.utcnow().timestamp()),
        "exp": int((datetime.utcnow() + timedelta(hours=_jwt_exp_hours())).timestamp()),
    }
    jwt_token = jwt.encode(payload, _jwt_secret(), algorithm="HS256")
    
    resp = JSONResponse(content={"ok": True, "message": "Logged in"})
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
"""

# We'll use regex to replace auth_discord_login through to auth_discord_callback (before auth_logout)
text = re.sub(r"@router\.get\(\"/auth/discord/login\"\).*?(?=@router\.post\(\"/auth/logout\"\))", auth_endpoints + "\n", text, flags=re.DOTALL)

# 3. Replace auth_me cleanly without matching other functions
# Find where auth_me starts and ends (it ends right before _ensure_app_user, wait no, let's just replace the whole auth_me block)
new_auth_me = """@router.get("/auth/me")
def auth_me(request: Request, response: Response):
    token = request.cookies.get(JWT_COOKIE_NAME, "")
    if not token:
        return {"ok": True, "authenticated": False}
    try:
        claims = _decode_session_jwt(token)
        uid = str(claims.get("sub") or "")
        uname = str(claims.get("name") or "")
        actx = _load_alliance_context(uid)
        pctx = _load_premium_context(uid)
        return {
            "ok": True,
            "authenticated": True,
            "user": {
                "discord_user_id": uid,
                "discord_username": uname,
                "avatar": claims.get("avatar"),
                "is_admin": uid in _admin_user_ids(),
                "is_premium": bool(pctx.get("is_premium")),
                "has_premium_access": bool(uid in _admin_user_ids() or pctx.get("is_premium")),
            },
            "alliance": actx,
        }
    except Exception:
        return {"ok": True, "authenticated": False}
"""
text = re.sub(r"@router\.get\(\"/auth/me\"\).*?(?=\n\ndef _ensure_app_user)", new_auth_me, text, flags=re.DOTALL)


# 4. Modify _get_current_user to cleanly error if unauthenticated
new_get_current_user = """def _get_current_user(request: Request) -> Dict[str, Any]:
    token = request.cookies.get(JWT_COOKIE_NAME, "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated.")
    
    claims = _decode_session_jwt(token)"""
text = re.sub(r"def _get_current_user\(request: Request\) -> Dict\[str, Any\]:.*?(?=    uid = str\(claims\.get\(\"sub\"\))", new_get_current_user + "\n", text, flags=re.DOTALL)

with open("../backend/auth_kg_fixed.py", "w") as f:
    f.write(text)
