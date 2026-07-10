"""Application entrypoint with production compatibility extensions."""

import threading

import auth_kg


def _login_headers_without_world_id(url: str):
    headers = auth_kg._kg_headers(url, "/login")
    headers.pop("World-Id", None)
    return headers


auth_kg._kg_login_headers = _login_headers_without_world_id

from main import app  # noqa: E402  (patch must run before app/router import)
from password_reset import ensure_password_reset_table, router as password_reset_router  # noqa: E402

app.include_router(password_reset_router)
threading.Thread(target=ensure_password_reset_table, daemon=True, name="password-reset-db-init").start()
