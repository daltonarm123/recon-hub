import asyncio
import json
import unittest

from fastapi import HTTPException, Request
import psycopg

import main


class ApiBehaviorTests(unittest.TestCase):
    def test_api_http_exception_payload_is_consistent(self):
        request = Request({"type": "http", "method": "POST", "path": "/api/reports/spy", "headers": []})

        response = asyncio.run(main.http_exception_handler(request, HTTPException(status_code=400, detail="raw_text is empty")))
        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["ok"], False)
        self.assertEqual(payload["detail"], "raw_text is empty")
        self.assertEqual(payload["error"]["code"], "bad_request")

    def test_db_exception_mapping_handles_timeout(self):
        http_exc = main._db_exception_to_http(psycopg.errors.QueryCanceled("statement timeout"))

        self.assertEqual(http_exc.status_code, 504)
        self.assertEqual(http_exc.detail, "Database query timed out")

    def test_connect_wraps_operational_error(self):
        orig_connect = main.psycopg.connect
        orig_get_dsn = main._get_dsn
        orig_logger_exception = main.logger.exception
        main._get_dsn = lambda: "postgresql://example"
        main.logger.exception = lambda *args, **kwargs: None

        def boom(*args, **kwargs):
            raise psycopg.OperationalError("db down")

        main.psycopg.connect = boom
        try:
            with self.assertRaises(HTTPException) as ctx:
                main._connect()
        finally:
            main.psycopg.connect = orig_connect
            main._get_dsn = orig_get_dsn
            main.logger.exception = orig_logger_exception

        self.assertEqual(ctx.exception.status_code, 503)
        self.assertEqual(ctx.exception.detail, "Database unavailable")


if __name__ == "__main__":
    unittest.main()