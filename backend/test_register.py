from fastapi import Request
from pydantic import BaseModel
import sys

from auth_kg import auth_register, AuthLoginBody

class FakeConn:
    def cursor(self, row_factory=None):
        return FakeCursor()
    def commit(self):
        pass
    def close(self):
        pass

class FakeCursor:
    def execute(self, sql, params=None):
        pass
    def fetchone(self):
        return None
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

# mock _connect
import auth_kg
auth_kg._connect = lambda: FakeConn()
auth_kg._jwt_secret = lambda: "secret"

body = AuthLoginBody(username="testuser", password="testpassword123")
try:
    resp = auth_register(body)
    print("SUCCESS", resp.status_code)
except Exception as e:
    import traceback
    traceback.print_exc()

