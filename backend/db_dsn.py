import os
from urllib.parse import quote


def resolve_database_dsn() -> str:
    direct_private = str(os.getenv("DATABASE_URL") or "").strip()
    if direct_private:
        return direct_private

    pg_host = str(os.getenv("PGHOST") or "").strip()
    pg_port = str(os.getenv("PGPORT") or "5432").strip() or "5432"
    pg_db = str(os.getenv("PGDATABASE") or "").strip()
    pg_user = str(os.getenv("PGUSER") or "").strip()
    pg_password = str(os.getenv("PGPASSWORD") or "").strip()
    if pg_host and pg_db and pg_user and pg_password:
        return (
            f"postgresql://{quote(pg_user, safe='')}:{quote(pg_password, safe='')}"
            f"@{pg_host}:{pg_port}/{quote(pg_db, safe='')}"
        )

    direct_public = str(os.getenv("DATABASE_PUBLIC_URL") or "").strip()
    if direct_public:
        return direct_public

    legacy = str(os.getenv("POSTGRES_URL") or "").strip()
    if legacy:
        return legacy

    raise RuntimeError(
        "Database DSN is not configured. Set DATABASE_URL, or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD, or DATABASE_PUBLIC_URL."
    )