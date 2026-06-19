import os
import unittest

from db_dsn import resolve_database_dsn


class ResolveDatabaseDsnTests(unittest.TestCase):
    def setUp(self):
        self.keys = [
            "DATABASE_URL",
            "DATABASE_PUBLIC_URL",
            "POSTGRES_URL",
            "PGHOST",
            "PGPORT",
            "PGDATABASE",
            "PGUSER",
            "PGPASSWORD",
        ]
        self.snapshot = {key: os.environ.get(key) for key in self.keys}
        for key in self.keys:
            os.environ.pop(key, None)

    def tearDown(self):
        for key in self.keys:
            os.environ.pop(key, None)
        for key, value in self.snapshot.items():
            if value is not None:
                os.environ[key] = value

    def test_prefers_database_url(self):
        os.environ["DATABASE_URL"] = "postgresql://private-db"
        os.environ["DATABASE_PUBLIC_URL"] = "postgresql://public-db"

        self.assertEqual(resolve_database_dsn(), "postgresql://private-db")

    def test_builds_from_pg_env_before_public_url(self):
        os.environ["PGHOST"] = "railway.internal"
        os.environ["PGPORT"] = "5432"
        os.environ["PGDATABASE"] = "railway"
        os.environ["PGUSER"] = "postgres"
        os.environ["PGPASSWORD"] = "secret value"
        os.environ["DATABASE_PUBLIC_URL"] = "postgresql://public-db"

        self.assertEqual(
            resolve_database_dsn(),
            "postgresql://postgres:secret%20value@railway.internal:5432/railway",
        )

    def test_uses_public_then_legacy_fallbacks(self):
        os.environ["DATABASE_PUBLIC_URL"] = "postgresql://public-db"
        self.assertEqual(resolve_database_dsn(), "postgresql://public-db")

        os.environ.pop("DATABASE_PUBLIC_URL", None)
        os.environ["POSTGRES_URL"] = "postgresql://legacy-db"
        self.assertEqual(resolve_database_dsn(), "postgresql://legacy-db")

    def test_raises_when_unconfigured(self):
        with self.assertRaises(RuntimeError):
            resolve_database_dsn()


if __name__ == "__main__":
    unittest.main()