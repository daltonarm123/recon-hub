import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

# -------------------------
# FastAPI
# -------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Static (SPA + assets)
# -------------------------
if (STATIC_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets")), name="assets")


@app.get("/")
def root():
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        return JSONResponse({"ok": True, "service": "recon-hub",'static': False, "note": "static index.html not found"})
    return FileResponse(str(index_path))


@app.get("/calc")
def calc_redirect():
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


@app.get("/healthz")
def healthz():
    return {"ok": True}


# -------------------------
# Postgres helpers
# -------------------------
def _get_dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not dsn:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")
    return dsn


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_dsn(), row_factory=dict_row)


def _ensure_tables(conn: psycopg.Connection) -> None:
    """
    Creates Recon Hub tables (rh_*) and safely auto-migrates extended spy fields.
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

        # ---- Extended fields ----
        cur.execute(
            """
            ALTER TABLE rh_spy_reports
            ADD COLUMN IF NOT EXISTS spies_sent INTEGER,
            ADD COLUMN IF NOT EXISTS spies_lost INTEGER,
            ADD COLUMN IF NOT EXISTS result_level TEXT,
            ADD COLUMN IF NOT EXISTS honour REAL,
            ADD COLUMN IF NOT EXISTS ranking INTEGER,
            ADD COLUMN IF NOT EXISTS networth BIGINT;
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS rh_spy_reports_kingdom_created_idx
            ON rh_spy_reports (kingdom_name, created_at DESC);
            """
        )

    conn.commit()


# -------------------------
# Spy report parsing helpers
# -------------------------
def extract_int(pattern: str, text: str) -> Optional[int]:
    m = re.search(pattern, text, re.I)
    return int(m.group(1).replace(",", "")) if m else None


def extract_float(pattern: str, text: str) -> Optional[float]:
    m = re.search(pattern, text, re.I)
    return float(m.group(1)) if m else None


def extract_text(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text, re.I)
    return m.group(1).strip() if m else None


def _grab_line(text: str, label: str) -> Optional[str]:
    m = re.search(rf"^\s*{re.escape(label)}\s*:\s*(.+?)\s*$", text, flags=re.I | re.M)
    return m.group(1).strip() if m else None


def _num(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s2 = re.sub(r"[,\s]+", "", s)
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
        v = _num(m.group(2))
        if v is not None:
            out[m.group(1).strip()] = v
    return out


def parse_spy_report(text: str) -> Dict[str, Any]:
    troops_chunk = _section(
        text,
        "Our spies also found the following information about the kingdom's troops:",
        ["The following information was found regarding troop movements", "Select Ruleset:", "Standard Rules"],
    )
    resources_chunk = _section(
        text,
        "Our spies also found the following information about the kingdom's resources:",
        ["Our spies also found the following information about the kingdom's troops:"],
    )

    troops_kv = _parse_kv_lines(troops_chunk)
    resources_kv = _parse_kv_lines(resources_chunk)

    # Filter troop garbage keys if they appear
    troops: Dict[str, int] = {}
    for k, v in (troops_kv or {}).items():
        lk = k.lower()
        if lk.startswith("population"):
            continue
        if "defensive power" in lk:
            continue
        troops[k] = v

    return {
        "target": _grab_line(text, "Target"),
        "alliance": _grab_line(text, "Alliance"),
        "castles": _num(_grab_line(text, "Number of Castles")),
        "defender_dp": extract_int(r"Approximate defensive power.*?:\s*([0-9,]+)", text),
        "spies_sent": extract_int(r"Spies Sent\s*:\s*([0-9,]+)", text),
        "spies_lost": extract_int(r"Spies Lost\s*:\s*([0-9,]+)", text),
        "result_level": extract_text(r"Result Level\s*:\s*(.+)", text),
        "honour": extract_float(r"Honour\s*:\s*([0-9.]+)", text),
        "ranking": extract_int(r"Ranking\s*:\s*([0-9,]+)", text),
        "networth": extract_int(r"Networth\s*:\s*([0-9,]+)", text),
        "troops": troops,
        "resources": dict(resources_kv or {}),
        "raw_text": text,
    }


# -------------------------
# API: Kingdoms (LIST)
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


# -------------------------
# API: Kingdom reports (DETAIL)
# -------------------------
@app.get("/api/kingdoms/{kingdom_name}/spy-reports")
def list_spy_reports(kingdom_name: str, limit: int = 50):
    conn = _connect()
    try:
        _ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    created_at,
                    kingdom_name,
                    alliance,
                    defender_dp,
                    castles,
                    spies_sent,
                    spies_lost,
                    result_level,
                    honour,
                    ranking,
                    networth,
                    troops,
                    resources
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


# -------------------------
# API: Spy reports
# -------------------------
@app.get("/api/spy-reports/{report_id}/raw", response_class=PlainTextResponse)
def get_spy_report_raw(report_id: int):
    conn = _connect()
    try:
        _ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT raw_text FROM rh_spy_reports WHERE id=%s", (report_id,))
            row = cur.fetchone()
        if not row or not row.get("raw_text"):
            raise HTTPException(status_code=404, detail="Raw report not found")
        return row["raw_text"]
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
                SELECT
                    id, created_at, kingdom_name, alliance,
                    defender_dp, castles,
                    spies_sent, spies_lost, result_level, honour, ranking, networth,
                    troops, resources, raw_text
                FROM rh_spy_reports
                WHERE id=%s
                """,
                (report_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Not found")
        return {"ok": True, "report": row}
    finally:
        conn.close()


@app.post("/api/reports/spy")
def ingest_spy_report(payload: Dict[str, Any]):
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

            cur.execute(
                """
                INSERT INTO rh_spy_reports
                (kingdom_name, alliance, defender_dp, castles,
                 spies_sent, spies_lost, result_level, honour, ranking, networth,
                 troops, resources, raw_text)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id, created_at
                """,
                (
                    parsed["target"],
                    parsed.get("alliance"),
                    parsed.get("defender_dp"),
                    parsed.get("castles"),
                    parsed.get("spies_sent"),
                    parsed.get("spies_lost"),
                    parsed.get("result_level"),
                    parsed.get("honour"),
                    parsed.get("ranking"),
                    parsed.get("networth"),
                    Jsonb(parsed.get("troops") or {}),
                    Jsonb(parsed.get("resources") or {}),
                    parsed.get("raw_text"),
                ),
            )
            row = cur.fetchone()

        conn.commit()
        return {"ok": True, "stored": {"id": row["id"], "created_at": row["created_at"]}, "parsed": parsed}
    finally:
        conn.close()


# -------------------------
# SPA fallback
# -------------------------
@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not Found")

    candidate = STATIC_DIR / full_path
    if candidate.exists() and candidate.is_file():
        return FileResponse(str(candidate))

    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))

    raise HTTPException(status_code=404, detail="Not Found")

