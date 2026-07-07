import unittest
import os

from fastapi import HTTPException, Request

import auth_kg


class FakeConn:
    def __init__(self):
        self.executed = []

    def cursor(self, row_factory=None):
        return FakeCursor(self)

    def commit(self):
        return None

    def close(self):
        return None


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))

    def fetchone(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class AuthBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.orig_connect = auth_kg._connect
        self.orig_secret = auth_kg._jwt_secret
        self.orig_billing_enabled = auth_kg._billing_enabled
        self.orig_get_current_user = getattr(auth_kg, "_get_current_user", None)
        self.orig_load_premium_context = getattr(auth_kg, "_load_premium_context", None)
        self.fake_conn = FakeConn()
        auth_kg._connect = lambda: self.fake_conn
        auth_kg._jwt_secret = lambda: "secret"
        auth_kg._billing_enabled = lambda: False

    def tearDown(self):
        auth_kg._connect = self.orig_connect
        auth_kg._jwt_secret = self.orig_secret
        auth_kg._billing_enabled = self.orig_billing_enabled
        if self.orig_get_current_user is not None:
            auth_kg._get_current_user = self.orig_get_current_user
        if self.orig_load_premium_context is not None:
            auth_kg._load_premium_context = self.orig_load_premium_context

    def test_register_rejects_whitespace_username(self):
        body = auth_kg.AuthLoginBody(username="   ", password="testpassword123")

        with self.assertRaises(HTTPException) as ctx:
            auth_kg.auth_register(body)

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(self.fake_conn.executed, [])

    def test_logout_returns_json_for_fetch_clients(self):
        request = Request({"type": "http", "headers": [(b"accept", b"*/*")]})

        response = auth_kg.auth_logout(request)

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'{"ok":true}', response.body)
        self.assertIn("rh_session=\"\"", response.headers.get("set-cookie", ""))

    def test_logout_redirects_for_html_clients(self):
        request = Request({"type": "http", "headers": [(b"accept", b"text/html")]})

        response = auth_kg.auth_logout(request)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("location"), "/")

    def test_billing_status_reports_disabled_when_flag_off(self):
        auth_kg._get_current_user = lambda request: {"discord_user_id": "u1", "is_admin": False}
        auth_kg._load_premium_context = lambda user_id: {"is_premium": False}
        request = Request({"type": "http", "headers": []})

        response = auth_kg.billing_premium_status(request)

        self.assertEqual(response["enabled"], False)
        self.assertEqual(response["plans"], [])

    def test_paypal_order_creation_is_blocked_when_billing_disabled(self):
        request = Request({"type": "http", "headers": []})
        body = auth_kg.PayPalCreateOrderBody(tier="monthly")

        with self.assertRaises(HTTPException) as ctx:
            auth_kg.billing_paypal_create_order(body, request)

        self.assertEqual(ctx.exception.status_code, 503)
        self.assertEqual(ctx.exception.detail, "Billing is not enabled")

    def test_elixer_username_is_treated_as_admin(self):
        self.assertEqual(auth_kg._is_admin_identity("random-user-id", "elixer"), True)
        self.assertEqual(auth_kg._is_admin_identity("random-user-id", "Elixer"), True)

    def test_fetch_settlements_live_uses_source_city_id_for_detail_requests(self):
        conn_row = {"account_id": 32, "kingdom_id": 41, "token_enc": "enc-token"}
        original_decrypt = auth_kg._decrypt_token
        original_post = auth_kg._kg_post_json
        try:
            auth_kg._decrypt_token = lambda token: "live-token"

            def fake_post(url, payload):
                if "GetSettlements" in url:
                    return {
                        "cities": [
                            {
                                "cityId": 917,
                                "cityName": "MINI DUDE 2",
                                "accountId": 32,
                            }
                        ]
                    }
                if "GetSettlementBuildings" in url:
                    if payload.get("cityId") == 917:
                        return {
                            "cityBuildings": [
                                {
                                    "buildingType": "Lumber Mill",
                                    "level": 5,
                                    "effectText": "+5% Wood",
                                }
                            ]
                        }
                    return {"cityBuildings": []}
                return {}

            auth_kg._kg_post_json = fake_post

            settlements = auth_kg._fetch_settlements_live(conn_row)

            self.assertEqual(len(settlements), 1)
            self.assertEqual(settlements[0]["settlement_id"], 917)
            self.assertEqual(len(settlements[0]["buildings"]), 1)
            self.assertEqual(settlements[0]["buildings"][0]["building_type"], "Lumber Mill")
        finally:
            auth_kg._decrypt_token = original_decrypt
            auth_kg._kg_post_json = original_post

    def test_kg_headers_include_cookie_verification_and_extra_headers(self):
        old_cookie = os.environ.get("KG_COOKIE")
        old_extra = os.environ.get("KG_EXTRA_HEADERS_JSON")
        old_lang = os.environ.get("KG_ACCEPT_LANGUAGE")
        old_agent = os.environ.get("KG_USER_AGENT")
        try:
            os.environ["KG_COOKIE"] = "foo=bar; __RequestVerificationToken=req-123"
            os.environ["KG_EXTRA_HEADERS_JSON"] = '{"X-Test-Header":"settlements"}'
            os.environ["KG_ACCEPT_LANGUAGE"] = "en-US"
            os.environ["KG_USER_AGENT"] = "kg-test-agent"

            headers = auth_kg._kg_headers("https://kingdomgame.net/WebService/Settlement.asmx/GetSettlementBuildings")

            self.assertEqual(headers["Origin"], "https://kingdomgame.net")
            self.assertEqual(headers["Referer"], "https://kingdomgame.net/settlements")
            self.assertEqual(headers["Cookie"], "foo=bar; __RequestVerificationToken=req-123")
            self.assertEqual(headers["RequestVerificationToken"], "req-123")
            self.assertEqual(headers["X-RequestVerificationToken"], "req-123")
            self.assertEqual(headers["X-Test-Header"], "settlements")
            self.assertEqual(headers["User-Agent"], "kg-test-agent")
            self.assertEqual(headers["Accept-Language"], "en-US")
        finally:
            if old_cookie is None:
                os.environ.pop("KG_COOKIE", None)
            else:
                os.environ["KG_COOKIE"] = old_cookie
            if old_extra is None:
                os.environ.pop("KG_EXTRA_HEADERS_JSON", None)
            else:
                os.environ["KG_EXTRA_HEADERS_JSON"] = old_extra
            if old_lang is None:
                os.environ.pop("KG_ACCEPT_LANGUAGE", None)
            else:
                os.environ["KG_ACCEPT_LANGUAGE"] = old_lang
            if old_agent is None:
                os.environ.pop("KG_USER_AGENT", None)
            else:
                os.environ["KG_USER_AGENT"] = old_agent

    def test_kg_login_headers_include_login_referer_and_world_id(self):
        headers = auth_kg._kg_login_headers("https://kingdomgame.net/WebService/User.asmx/Login")

        self.assertEqual(headers["Referer"], "https://kingdomgame.net/login")
        self.assertEqual(headers["World-Id"], "1")

    def test_kg_login_credential_extracts_token_account_and_kingdom(self):
        original_client = auth_kg.httpx.Client

        class FakeResponse:
            status_code = 200
            text = '{"d":"{\"token\":\"kg-token\",\"accountId\":32,\"kingdomId\":41}"}'

            def json(self):
                return {"d": '{"token":"kg-token","accountId":32,"kingdomId":41}'}

            def raise_for_status(self):
                return None

        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

            def post(self, url, headers=None, content=None):
                return FakeResponse()

        try:
            auth_kg.httpx.Client = FakeClient
            cred = auth_kg._kg_login_credential("user@example.com", "hunter22")
            self.assertEqual(cred["token"], "kg-token")
            self.assertEqual(cred["account_id"], 32)
            self.assertEqual(cred["kingdom_id"], 41)
        finally:
            auth_kg.httpx.Client = original_client

    def test_kg_login_credential_resolves_missing_kingdom_from_login_response(self):
        original_client = auth_kg.httpx.Client
        original_post = auth_kg._kg_post_json
        calls = []

        class FakeResponse:
            status_code = 200
            text = '{"d":"{\\"accountId\\":\\"32\\",\\"token\\":\\"kg-token\\",\\"ReturnValue\\":1}"}'

            def json(self):
                return {"d": '{"accountId":"32","token":"kg-token","ReturnValue":1}'}

            def raise_for_status(self):
                return None

        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

            def post(self, url, headers=None, content=None):
                calls.append((url, headers, content))
                return FakeResponse()

        try:
            auth_kg.httpx.Client = FakeClient
            auth_kg._kg_post_json = lambda url, payload: {"kingdoms": [{"id": "41"}]}

            cred = auth_kg._kg_login_credential("user@example.com", "hunter22")

            self.assertEqual(cred["token"], "kg-token")
            self.assertEqual(cred["account_id"], 32)
            self.assertEqual(cred["kingdom_id"], 41)
            self.assertEqual(len(calls), 1)
            self.assertIn('"email":"user@example.com"', calls[0][2])
        finally:
            auth_kg.httpx.Client = original_client
            auth_kg._kg_post_json = original_post

    def test_kg_login_route_saves_server_side_login_result(self):
        original_current_user = auth_kg._get_current_user
        original_browser_login = getattr(auth_kg, "_kg_browser_login_credential", None)
        original_login = auth_kg._kg_login_credential
        original_upsert = auth_kg._upsert_user_kg_connection
        saved = {}
        try:
            auth_kg._get_current_user = lambda request: {
                "discord_user_id": "u1",
                "discord_username": "tester",
            }
            auth_kg._kg_browser_login_credential = lambda email, password: {
                "token": "kg-token",
                "account_id": 32,
                "kingdom_id": 41,
            }
            auth_kg._kg_login_credential = lambda email, password: {
                "token": "kg-token",
                "account_id": 32,
                "kingdom_id": 41,
            }

            def fake_upsert(discord_user_id, discord_username, account_id, kingdom_id, token):
                saved.update(
                    {
                        "discord_user_id": discord_user_id,
                        "discord_username": discord_username,
                        "account_id": account_id,
                        "kingdom_id": kingdom_id,
                        "token": token,
                    }
                )

            auth_kg._upsert_user_kg_connection = fake_upsert

            request = Request({"type": "http", "headers": []})
            body = auth_kg.KGLoginBody(email="user@example.com", password="hunter22")
            response = auth_kg.kg_login(body, request)

            self.assertEqual(response["ok"], True)
            self.assertEqual(response["connection"]["account_id"], 32)
            self.assertEqual(response["connection"]["kingdom_id"], 41)
            self.assertEqual(saved["discord_user_id"], "u1")
            self.assertEqual(saved["account_id"], 32)
            self.assertEqual(saved["kingdom_id"], 41)
            self.assertEqual(saved["token"], "kg-token")
        finally:
            auth_kg._get_current_user = original_current_user
            if original_browser_login is not None:
                auth_kg._kg_browser_login_credential = original_browser_login
            auth_kg._kg_login_credential = original_login
            auth_kg._upsert_user_kg_connection = original_upsert

    def test_kg_login_route_falls_back_to_direct_login_when_browser_login_fails(self):
        original_current_user = auth_kg._get_current_user
        original_browser_login = getattr(auth_kg, "_kg_browser_login_credential", None)
        original_login = auth_kg._kg_login_credential
        original_upsert = auth_kg._upsert_user_kg_connection
        saved = {}
        try:
            auth_kg._get_current_user = lambda request: {
                "discord_user_id": "u1",
                "discord_username": "tester",
            }

            def fail_browser_login(email, password):
                raise HTTPException(status_code=502, detail="browser failed")

            auth_kg._kg_browser_login_credential = fail_browser_login
            auth_kg._kg_login_credential = lambda email, password: {
                "token": "kg-token",
                "account_id": 32,
                "kingdom_id": 41,
            }

            def fake_upsert(discord_user_id, discord_username, account_id, kingdom_id, token):
                saved.update({"account_id": account_id, "kingdom_id": kingdom_id, "token": token})

            auth_kg._upsert_user_kg_connection = fake_upsert

            request = Request({"type": "http", "headers": []})
            body = auth_kg.KGLoginBody(email="user@example.com", password="hunter22")
            response = auth_kg.kg_login(body, request)

            self.assertEqual(response["ok"], True)
            self.assertEqual(saved["account_id"], 32)
            self.assertEqual(saved["kingdom_id"], 41)
            self.assertEqual(saved["token"], "kg-token")
        finally:
            auth_kg._get_current_user = original_current_user
            if original_browser_login is not None:
                auth_kg._kg_browser_login_credential = original_browser_login
            auth_kg._kg_login_credential = original_login
            auth_kg._upsert_user_kg_connection = original_upsert

    def test_kg_bootstrap_login_session_reads_verification_token_from_cookie(self):
        class FakeCookies:
            def get(self, key, default=None):
                if key == "__RequestVerificationToken":
                    return "cookie-token-123"
                return default

        class FakeResponse:
            text = "<html></html>"

        class FakeClient:
            def __init__(self):
                self.cookies = FakeCookies()
                self.calls = []

            def get(self, url, headers=None, follow_redirects=None):
                self.calls.append((url, headers, follow_redirects))
                return FakeResponse()

        client = FakeClient()

        token = auth_kg._kg_bootstrap_login_session(
            client,
            "https://kingdomgame.net/WebService/User.asmx/Login",
        )

        self.assertEqual(token, "cookie-token-123")
        self.assertTrue(client.calls)

    def test_parse_kg_response_text_decodes_nested_d_payload(self):
        payload = os.linesep.join([])
        payload = '{"d": "{\\"token\\":\\"kg-token\\",\\"accountId\\":32}"}'
        parsed = auth_kg._parse_kg_response_text(payload)
        self.assertEqual(parsed["token"], "kg-token")
        self.assertEqual(parsed["accountId"], 32)


if __name__ == "__main__":
    unittest.main()