import unittest

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


if __name__ == "__main__":
    unittest.main()