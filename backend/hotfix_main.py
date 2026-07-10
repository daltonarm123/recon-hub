"""Application entrypoint with a narrow KingdomGame login compatibility patch.

KingdomGame's login endpoint can return HTTP 500 when the pre-auth request
contains the World-Id header. Authenticated game-data requests still require
that header, so only the login-header helper is changed here.
"""

import auth_kg


def _login_headers_without_world_id(url: str):
    headers = auth_kg._kg_headers(url, "/login")
    headers.pop("World-Id", None)
    return headers


auth_kg._kg_login_headers = _login_headers_without_world_id

from main import app  # noqa: E402,F401  (patch must run before app/router import)
