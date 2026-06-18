with open("auth_kg.py", "r") as f:
    text = f.read()

import re
text = re.sub(
    r"@router.get\(\"/auth/me\"\)\ndef auth_me.*?def _get_current_user",
    """@router.get("/auth/me")
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
                "is_premium": bool(pctx.get("is_premium") or False),
                "has_premium_access": bool(uid in _admin_user_ids() or pctx.get("is_premium")),
            },
            "alliance": actx,
        }
    except Exception:
        return {"ok": True, "authenticated": False}

def _get_current_user""",
    text,
    flags=re.DOTALL
)

with open("auth_kg.py", "w") as f:
    f.write(text)

