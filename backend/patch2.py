with open("auth_kg.py", "r") as f:
    lines = f.readlines()

out = []
skip = False
for line in lines:
    if line.startswith('def auth_me(request: Request, response: Response):'):
        skip = True
        out.append(line)
        out.append('    token = request.cookies.get(JWT_COOKIE_NAME, "")\n')
        out.append('    if not token:\n')
        out.append('        return {"ok": True, "authenticated": False}\n')
        out.append('    try:\n')
        out.append('        claims = _decode_session_jwt(token)\n')
        out.append('        uid = str(claims.get("sub") or "")\n')
        out.append('        uname = str(claims.get("name") or "")\n')
        out.append('        actx = _load_alliance_context(uid)\n')
        out.append('        pctx = _load_premium_context(uid)\n')
        out.append('        return {\n')
        out.append('            "ok": True,\n')
        out.append('            "authenticated": True,\n')
        out.append('            "user": {\n')
        out.append('                "discord_user_id": uid,\n')
        out.append('                "discord_username": uname,\n')
        out.append('                "avatar": claims.get("avatar"),\n')
        out.append('                "is_admin": uid in _admin_user_ids(),\n')
        out.append('                "is_premium": bool(pctx.get("is_premium") or False),\n')
        out.append('                "has_premium_access": bool(uid in _admin_user_ids() or pctx.get("is_premium")),\n')
        out.append('            },\n')
        out.append('            "alliance": actx,\n')
        out.append('        }\n')
        out.append('    except Exception:\n')
        out.append('        return {"ok": True, "authenticated": False}\n\n')
    elif skip:
        if line.startswith('def _get_current_user('):
            skip = False
            out.append(line)
    else:
        out.append(line)

with open("auth_kg.py", "w") as f:
    f.writelines(out)
