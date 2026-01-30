import os
import re
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, Json

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

# ---- FastAPI ----
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Static (SPA + assets) ----
if (STATIC_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets")), name="assets")


@app.get("/")
def root():
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        return JSONResponse({"ok": True, "service": "recon-hub", "note": "static index.html not found"})
    return FileResponse(str(index_path))


@app.get("/calc")
def calc_redirect():
    # Option A: always use the static v2 calculator
    return RedirectResponse(url="/kg-calc.html", status_code=302)


@app.get("/kg-calc.html")
def serve_calc():
    p = STATIC_DIR / "kg-calc.html"
    if not p.exists():
        raise HTTPException(status_code=404, detail="kg-calc.html not found")
    return FileResponse(str(p))


@app.get("/api/status")
def status():
    return {"ok": True, "service": "recon-hub", "ts": datetime.utcnow().isoformat() + "Z"}


# -------------------------
# Postgres helpers
# -------------------------
def _get_dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not dsn:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set on the server")
    return dsn


def _connect():
    return psycopg2.connect(_get_dsn(), cursor_factory=RealDictCursor)


def _ensure_tables(conn) -> None:
    """
    Creates Recon Hub tables (prefixed rh_) that do NOT conflict with your existing bot tables.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rh_kingdoms (
                kingdom_name TEXT PRIMARY KEY,
                alliance TEXT,
                last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rh_spy_reports (
                id BIGSERIAL PRIMARY KEY,
                kingdom_name TEXT NOT NULL,
                alliance TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                defender_dp BIGINT,
                castles BIGINT,
                troops JSONB,
                resources JSONB,
                raw_text TEXT
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS rh_spy_reports_kingdom_created_idx ON rh_spy_reports (kingdom_name, created_at DESC);"
        )
    conn.commit()


# -------------------------
# Spy report parsing (basic, consistent with your JS parser)
# -------------------------
def _grab_line(text: str, label: str) -> Optional[str]:
    m = re.search(rf"^\s*{re.escape(label)}\s*:\s*(.+?)\s*$", text, flags=re.I | re.M)
    return m.group(1).strip() if m else None


def _num(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s2 = re.sub(r"[,\s]+", "", s.strip())
    if not s2:
        return None
    try:
        return int(float(s2))
    except Exception:
        return None


def _section(text: str, header: str, stop_headers: List[str]) -> str:
    m = re.search(rf"^\s*{re.escape(header)}\s*$", text, flags=re.I | re.M)
    if not m:
        return ""
    start = m.end()
    tail = text[start:]
    end = len(tail)
    for sh in stop_headers:
        sm = re.search(rf"^\s*{re.escape(sh)}\s*$", tail, flags=re.I | re.M)
        if sm:
            end = min(end, sm.start())
    return tail[:end].strip()


def _parse_kv_lines(chunk: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for line in chunk.splitlines():
        m = re.match(r"^\s*([^:]{1,80}?)\s*:\s*([0-9][0-9,\s]*)\s*$", line)
        if not m:
            continue
        k = m.group(1).strip()
        v = _num(m.group(2))
        if v is None:
            continue
        out[k] = v
    return out


def parse_spy_report(text: str) -> Dict[str, Any]:
    target = _grab_line(text, "Target")
    alliance = _grab_line(text, "Alliance")
    castles = _num(_grab_line(text, "Number of Castles"))
    dp = None
    m = re.search(r"Approximate defensive power\*?\s*:\s*([0-9,]+)", text, flags=re.I)
    if m:
        dp = _num(m.group(1))

    resources_chunk = _section(
        text,
        "Our spies also found the following information about the kingdom's resources:",
        ["Our spies also found the following information about the kingdom's troops:"],
    )
    troops_chunk = _section(
        text,
        "Our spies also found the following information about the kingdom's troops:",
        ["The following information was found regarding troop movements", "Select Ruleset:", "Standard Rules"],
    )

    resources_kv = _parse_kv_lines(resources_chunk)
    troops_kv = _parse_kv_lines(troops_chunk)

    # filter out population / approximate lines
    troops: Dict[str, int] = {}
    for k, v in troops_kv.items():
        lk = k.lower()
        if lk.startswith("population"):
            continue
        if "defensive power" in lk:
            continue
        troops[k] = v

    resources: Dict[str, Any] = {}
    # keep original keys but normalize a few common ones
    for k, v in resources_kv.items():
        resources[k] = v

    return {
        "target": target,
        "alliance": alliance,
        "castles": castles,
        "defenderDP": dp,
        "troops": troops,
        "resources": resources,
        "raw_text": text,
    }


# -------------------------
# API: Kingdoms
# -------------------------
@app.get("/api/kingdoms")
def list_kingdoms(search: str = "", limit: int = 500):
    conn = _connect()
    try:
        _ensure_tables(conn)
        s = search.strip()
        like = f"%{s}%" if s else None
        with conn.cursor() as cur:
            if like:
                cur.execute(
                    """
                    SELECT kingdom_name, alliance, last_seen
                    FROM rh_kingdoms
                    WHERE kingdom_name ILIKE %s OR COALESCE(alliance,'') ILIKE %s
                    ORDER BY COALESCE(alliance,''), kingdom_name
                    LIMIT %s
                    """,
                    (like, like, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT kingdom_name, alliance, last_seen
                    FROM rh_kingdoms
                    ORDER BY COALESCE(alliance,''), kingdom_name
                    LIMIT %s
                    """,
                    (limit,),
                )
            kingdoms = cur.fetchall()

            # attach report counts + latest timestamp
            names = [k["kingdom_name"] for k in kingdoms]
            counts: Dict[str, Any] = {}
            if names:
                cur.execute(
                    """
                    SELECT kingdom_name, COUNT(*)::int AS report_count, MAX(created_at) AS latest_report_at
                    FROM rh_spy_reports
                    WHERE kingdom_name = ANY(%s)
                    GROUP BY kingdom_name
                    """,
                    (names,),
                )
                for r in cur.fetchall():
                    counts[r["kingdom_name"]] = r

        out = []
        for k in kingdoms:
            c = counts.get(k["kingdom_name"], {})
            out.append(
                {
                    "name": k["kingdom_name"],
                    "alliance": k.get("alliance"),
                    "last_seen": k.get("last_seen"),
                    "report_count": c.get("report_count", 0),
                    "latest_report_at": c.get("latest_report_at"),
                }
            )
        return {"ok": True, "kingdoms": out}
    finally:
        conn.close()


@app.get("/api/kingdoms/{kingdom_name}/spy-reports")
def list_spy_reports(kingdom_name: str, limit: int = 50):
    conn = _connect()
    try:
        _ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, created_at, alliance, defender_dp, castles, troops, resources
                FROM rh_spy_reports
                WHERE kingdom_name = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (kingdom_name, limit),
            )
            rows = cur.fetchall()
        return {"ok": True, "kingdom": kingdom_name, "reports": rows}
    finally:
        conn.close()


@app.get("/api/spy-reports/{report_id}")
def get_spy_report(report_id: int):
    conn = _connect()
    try:
        _ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, created_at, kingdom_name, alliance, defender_dp, castles, troops, resources, raw_text
                FROM rh_spy_reports
                WHERE id = %s
                """,
                (report_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Report not found")
        return {"ok": True, "report": row}
    finally:
        conn.close()


@app.post("/api/reports/spy")
def ingest_spy_report(payload: Dict[str, Any]):
    """
    Paste a KG Spy Report -> we parse -> store into rh_spy_reports + upsert rh_kingdoms.
    This is additive and uses rh_* tables only.
    """
    raw = (payload.get("raw_text") or payload.get("text") or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="raw_text is required")

    parsed = parse_spy_report(raw)
    if not parsed.get("target"):
        raise HTTPException(status_code=400, detail="Could not parse Target: from the spy report")

    conn = _connect()
    try:
        _ensure_tables(conn)
        with conn.cursor() as cur:
            # Upsert kingdom
            cur.execute(
                """
                INSERT INTO rh_kingdoms (kingdom_name, alliance, last_seen)
                VALUES (%s, %s, NOW())
                ON CONFLICT (kingdom_name) DO UPDATE
                  SET alliance = EXCLUDED.alliance,
                      last_seen = NOW()
                """,
                (parsed["target"], parsed.get("alliance")),
            )
            # Insert report
            cur.execute(
                """
                INSERT INTO rh_spy_reports (kingdom_name, alliance, defender_dp, castles, troops, resources, raw_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at
                """,
                (
                    parsed["target"],
                    parsed.get("alliance"),
                    parsed.get("defenderDP"),
                    parsed.get("castles"),
                    Json(parsed.get("troops") or {}),
                    Json(parsed.get("resources") or {}),
                    parsed.get("raw_text"),
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return {"ok": True, "stored": {"id": row["id"], "created_at": row["created_at"]}, "parsed": parsed}
    finally:
        conn.close()


# ---- SPA fallback: serve index.html for client-side routes ----
@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    # Let API routes 404 normally
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not Found")

    # Serve real static files if they exist
    candidate = STATIC_DIR / full_path
    if candidate.exists() and candidate.is_file():
        return FileResponse(str(candidate))

    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))

    raise HTTPException(status_code=404, detail="Not Found")
