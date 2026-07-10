import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

import admin_api
import auth_kg

router = APIRouter()


class AdminResetLinkBody(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=128)


class CompleteResetBody(BaseModel):
    token: str = Field(..., min_length=32, max_length=256)
    new_password: str = Field(..., min_length=8, max_length=128)


def ensure_password_reset_table() -> None:
    conn = admin_api._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.password_reset_tokens (
                    id BIGSERIAL PRIMARY KEY,
                    discord_user_id TEXT NOT NULL REFERENCES public.app_users(discord_user_id) ON DELETE CASCADE,
                    token_hash TEXT NOT NULL UNIQUE,
                    created_by TEXT NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    used_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS password_reset_tokens_user_idx
                ON public.password_reset_tokens (discord_user_id, created_at DESC)
                """
            )
        conn.commit()
    finally:
        conn.close()


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@router.post("/api/admin/users/create-reset-link")
def create_reset_link(body: AdminResetLinkBody, request: Request) -> Dict[str, Any]:
    admin = admin_api._require_admin(request)
    user_id = body.user_id.strip()
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)

    conn = admin_api._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT discord_user_id, discord_username FROM public.app_users WHERE discord_user_id = %s",
                (user_id,),
            )
            user = cur.fetchone()
            if not user:
                raise HTTPException(status_code=404, detail="User account not found")

            cur.execute(
                "UPDATE public.password_reset_tokens SET used_at = now() WHERE discord_user_id = %s AND used_at IS NULL",
                (user_id,),
            )
            cur.execute(
                """
                INSERT INTO public.password_reset_tokens
                  (discord_user_id, token_hash, created_by, expires_at)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    user_id,
                    _token_hash(token),
                    admin.get("discord_username") or admin.get("discord_user_id"),
                    expires_at,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    origin = str(request.base_url).rstrip("/")
    return {
        "ok": True,
        "user": user,
        "reset_url": f"{origin}/reset-password?token={token}",
        "expires_at": expires_at.isoformat(),
    }


@router.post("/api/auth/complete-password-reset")
def complete_password_reset(body: CompleteResetBody) -> Dict[str, Any]:
    if body.new_password.isspace():
        raise HTTPException(status_code=400, detail="Password cannot be only whitespace")

    now = datetime.now(timezone.utc)
    conn = admin_api._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, discord_user_id
                FROM public.password_reset_tokens
                WHERE token_hash = %s
                  AND used_at IS NULL
                  AND expires_at > %s
                FOR UPDATE
                """,
                (_token_hash(body.token), now),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=400, detail="Reset link is invalid or expired")

            cur.execute(
                """
                UPDATE public.app_users
                SET password_hash = %s, updated_at = now()
                WHERE discord_user_id = %s
                """,
                (auth_kg.hash_password(body.new_password), row["discord_user_id"]),
            )
            cur.execute(
                "UPDATE public.password_reset_tokens SET used_at = now() WHERE id = %s",
                (row["id"],),
            )
        conn.commit()
    finally:
        conn.close()

    return {"ok": True, "message": "Password updated"}


@router.get("/reset-password", response_class=HTMLResponse, include_in_schema=False)
def password_reset_page() -> HTMLResponse:
    return HTMLResponse(
        """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Reset Password - Recon Hub</title>
  <style>
    body{margin:0;background:#0d1220;color:#e7ecff;font-family:Arial,sans-serif;min-height:100vh;display:grid;place-items:center}
    .card{width:min(420px,calc(100% - 32px));background:#171d2d;border:1px solid rgba(255,255,255,.12);border-radius:16px;padding:24px;box-sizing:border-box}
    h1{font-size:22px;margin:0 0 8px} p{opacity:.75;font-size:14px;line-height:1.5}
    input,button{width:100%;box-sizing:border-box;border-radius:10px;padding:11px 12px;font-size:14px}
    input{margin:8px 0;background:#0f1524;color:#fff;border:1px solid rgba(255,255,255,.14)}
    button{margin-top:10px;background:#3d68ff;color:white;border:0;font-weight:700;cursor:pointer}
    #msg{margin-top:12px;font-size:13px;min-height:18px}
  </style>
</head>
<body>
  <div class="card">
    <h1>Reset your password</h1>
    <p>Choose a new password for your Recon Hub account. This link can only be used once and expires after 30 minutes.</p>
    <input id="password" type="password" minlength="8" placeholder="New password" autocomplete="new-password" />
    <input id="confirm" type="password" minlength="8" placeholder="Confirm new password" autocomplete="new-password" />
    <button id="submit">Update Password</button>
    <div id="msg"></div>
  </div>
  <script>
    const params = new URLSearchParams(location.search);
    const token = params.get('token') || '';
    const button = document.getElementById('submit');
    const msg = document.getElementById('msg');
    button.addEventListener('click', async () => {
      const password = document.getElementById('password').value;
      const confirm = document.getElementById('confirm').value;
      msg.style.color = '#ff9a9a';
      if (!token) { msg.textContent = 'This reset link is missing its token.'; return; }
      if (password.length < 8) { msg.textContent = 'Password must be at least 8 characters.'; return; }
      if (password !== confirm) { msg.textContent = 'Passwords do not match.'; return; }
      button.disabled = true;
      msg.textContent = 'Updating password...';
      try {
        const response = await fetch('/api/auth/complete-password-reset', {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify({token, new_password: password})
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(data?.detail || `HTTP ${response.status}`);
        msg.style.color = '#8be28b';
        msg.textContent = 'Password updated. You can now return to Recon Hub and log in.';
      } catch (error) {
        msg.textContent = error?.message || 'Password reset failed.';
      } finally {
        button.disabled = false;
      }
    });
  </script>
</body>
</html>"""
    )
