import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
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
