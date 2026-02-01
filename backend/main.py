import os
import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row

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
        return JSONResponse({"ok": True, "service": "recon-hub", "note": "static index.html not found"})
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


def _ensure_bot_table(conn: psycopg.Connection) -> None:
    """
    Your bot already created public.spy_reports, but we keep this safe check
    so the API doesn't crash if someone points at an empty DB.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.spy_reports (
                id serial4 NOT NULL,
                kingdom text NULL,
                alliance text NULL,
                created_at timestamp NULL,
                raw text NULL,
                report_hash text NULL,
                defense_power int4 NULL,
                castles int4 NULL,
                raw_gz bytea NULL,
                CONSTRAINT spy_reports_pkey PRIMARY KEY (id),
                CONSTRAINT spy_reports_report_hash_key UNIQUE (report_hash)
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS spy_reports_kingdom_created_at_idx
            ON public.spy_reports (kingdom, created_at DESC, id DESC);
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS spy_reports_report_hash_uq
            ON public.spy_reports (report_hash);
            """
        )
    conn.commit()


# -------------------------
# Spy report parsing
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


def _float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s2 = s.strip()
    if not s2:
        return None
    try:
        return float(s2)
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


def parse_spy_report(raw_text: str) -> Dict[str, Any]:
    text = (raw_text or "").strip()

    target = _grab_line(text, "Target")
    alliance = _grab_line(text, "Alliance")

    honour = _float(_grab_line(text, "Honour"))
    ranking = _num(_grab_line(text, "Ranking"))
    networth = _num(_grab_line(text, "Networth"))
    spies_sent = _num(_grab_line(text, "Spies Sent"))
    spies_lost = _num(_grab_line(text, "Spies Lost"))
    result_level = _grab_line(text, "Result Level")
    castles = _num(_grab_line(text, "Number of Castles"))

    dp = None
    m = re.search(r"Approximate defensive power\*?\s*:\s*([0-9,\.eE\+]+)", text, flags=re.I)
    if m:
        # dp line can be like 1.05728e+006
        dp = _num(m.group(1))

    resources_chunk = _section(
        text,
        "Our spies also found the following information about the kingdom's resources:",
        ["Our spies also found the following information about the kingdom's troops:"],
    )
    troops_chunk = _section(
        text,
        "Our spies also found the following information about the kingdom's troops:",
        ["The following information was found regarding troop movements", "The following recent market transactions", "The following technology information"],
    )

    resources_kv = _parse_kv_lines(resources_chunk)
    troops_kv = _parse_kv_lines(troops_chunk)

    # Remove non-troop lines that sometimes appear
    troops: Dict[str, int] = {}
    for k, v in troops_kv.items():
        lk = k.lower()
        if lk.startswith("population"):
            continue
        if "defensive power" in lk:
            continue
        troops[k] = v

    return {
        "target": target,
        "alliance": alliance,
        "honour": honour,
        "ranking": ranking,
        "networth": networth,
        "spies_sent": spies_sent,
        "spies_lost": spies_lost,
        "result_level": result_level,
        "castles": castles,
        "defender_dp": dp,
        "troops": troops,
        "resources": resources_kv,
        "raw_text": text,
    }


def _hash_report(raw_text: str) -> str:
    # Normalize so tiny spacing diffs don't create dupes
    norm = re.sub(r"\r\n?", "\n", (raw_text or "").strip())
    norm = re.sub(r"[ \t]+", " ", norm)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


# -------------------------
# API: Kingdoms (from bot table)
# -------------------------
@app.get("/api/kingdoms")
def list_kingdoms(search: str = "", limit: int = 500):
    conn = _connect()
    try:
        _ensure_bot_table(conn)
        s = search.strip()
        like = f"%{s}%" if s else None

        with conn.cursor() as cur:
            if like:
                cur.execute(
                    """
                    SELECT
                        kingdom AS kingdom_name,
                        MAX(alliance) AS alliance,
                        COUNT(*)::int AS report_count,
                        MAX(created_at) AS latest_report_at
                    FROM public.spy_reports
                    WHERE COALESCE(kingdom,'') ILIKE %s OR COALESCE(alliance,'') ILIKE %s
                    GROUP BY kingdom
                    ORDER BY latest_report_at DESC NULLS LAST, kingdom ASC
                    LIMIT %s
                    """,
                    (like, like, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        kingdom AS kingdom_name,
                        MAX(alliance) AS alliance,
                        COUNT(*)::int AS report_count,
                        MAX(created_at) AS latest_report_at
                    FROM public.spy_reports
                    GROUP BY kingdom
                    ORDER BY latest_report_at DESC NULLS LAST, kingdom ASC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cur.fetchall()

        out = []
        for r in rows:
            if not r.get("kingdom_name"):
                continue
            out.append(
                {
                    "name": r["kingdom_name"],
                    "alliance": r.get("alliance"),
                    "report_count": r.get("report_count", 0),
                    "latest_report_at": r.get("latest_report_at"),
                }
            )
        return {"ok": True, "kingdoms": out}
    finally:
        conn.close()


@app.get("/api/kingdoms/{kingdom_name}/spy-reports")
def list_spy_reports(kingdom_name: str, limit: int = 50):
    conn = _connect()
    try:
        _ensure_bot_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, created_at, kingdom, alliance, defense_power, castles, raw
                FROM public.spy_reports
                WHERE kingdom = %s
                ORDER BY created_at DESC NULLS LAST, id DESC
                LIMIT %s
                """,
                (kingdom_name, limit),
            )
            rows = cur.fetchall()

        # Return parsed summary per row (UI-friendly)
        reports = []
        for r in rows:
            parsed = parse_spy_report(r.get("raw") or "")
            reports.append(
                {
                    "id": r["id"],
                    "created_at": r.get("created_at"),
                    "kingdom": r.get("kingdom"),
                    "alliance": r.get("alliance") or parsed.get("alliance"),
                    "defender_dp": r.get("defense_power") or parsed.get("defender_dp"),
                    "castles": r.get("castles") or parsed.get("castles"),
                    "spies_sent": parsed.get("spies_sent"),
                    "spies_lost": parsed.get("spies_lost"),
                    "result_level": parsed.get("result_level"),
                    "honour": parsed.get("honour"),
                    "ranking": parsed.get("ranking"),
                    "networth": parsed.get("networth"),
                    "troops": parsed.get("troops") or {},
                    "resources": parsed.get("resources") or {},
                }
            )

        return {"ok": True, "kingdom": kingdom_name, "reports": reports}
    finally:
        conn.close()


# -------------------------
# API: Spy reports
# -------------------------
@app.get("/api/spy-reports/{report_id}/raw", response_class=PlainTextResponse)
def get_spy_report_raw(report_id: int):
    conn = _connect()
    try:
        _ensure_bot_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT raw FROM public.spy_reports WHERE id = %s",
                (report_id,),
            )
            row = cur.fetchone()
        if not row or not row.get("raw"):
            raise HTTPException(status_code=404, detail="Raw report not found")
        return row["raw"]
    finally:
        conn.close()


@app.get("/api/spy-reports/{report_id}")
def get_spy_report(report_id: int):
    conn = _connect()
    try:
        _ensure_bot_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, created_at, kingdom, alliance, defense_power, castles, raw FROM public.spy_reports WHERE id = %s",
                (report_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Report not found")

        parsed = parse_spy_report(row.get("raw") or "")
        return {
            "ok": True,
            "report": {
                "id": row["id"],
                "created_at": row.get("created_at"),
                "kingdom": row.get("kingdom") or parsed.get("target"),
                "alliance": row.get("alliance") or parsed.get("alliance"),
                "defender_dp": row.get("defense_power") or parsed.get("defender_dp"),
                "castles": row.get("castles") or parsed.get("castles"),
                "spies_sent": parsed.get("spies_sent"),
                "spies_lost": parsed.get("spies_lost"),
                "result_level": parsed.get("result_level"),
                "honour": parsed.get("honour"),
                "ranking": parsed.get("ranking"),
                "networth": parsed.get("networth"),
                "troops": parsed.get("troops") or {},
                "resources": parsed.get("resources") or {},
                "raw_text": parsed.get("raw_text") or "",
            },
        }
    finally:
        conn.close()


# Manual paste endpoint (optional, but handy)
@app.post("/api/reports/spy")
def ingest_spy_report(payload: Dict[str, Any]):
    raw = (payload.get("raw_text") or payload.get("text") or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="raw_text is required")

    parsed = parse_spy_report(raw)
    if not parsed.get("target"):
        raise HTTPException(status_code=400, detail="Could not parse Target: from the spy report")

    report_hash = _hash_report(raw)

    conn = _connect()
    try:
        _ensure_bot_table(conn)
        with conn.cursor() as cur:
            # Insert (dedupe by report_hash)
            cur.execute(
                """
                INSERT INTO public.spy_reports (kingdom, alliance, created_at, raw, report_hash, defense_power, castles)
                VALUES (%s, %s, NOW(), %s, %s, %s, %s)
                ON CONFLICT (report_hash) DO NOTHING
                RETURNING id, created_at
                """,
                (
                    parsed.get("target"),
                    parsed.get("alliance"),
                    parsed.get("raw_text"),
                    report_hash,
                    parsed.get("defender_dp"),
                    parsed.get("castles"),
                ),
            )
            row = cur.fetchone()

        conn.commit()

        # If it was a duplicate, row will be None
        return {
            "ok": True,
            "stored": ({"id": row["id"], "created_at": row["created_at"]} if row else {"duplicate": True}),
            "parsed": parsed,
        }
    finally:
        conn.close()


# -------------------------
# SPA fallback
# -------------------------
@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not Found")

    p = STATIC_DIR / full_path
    if p.exists() and p.is_file():
        return FileResponse(str(p))

    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))

    raise HTTPException(status_code=404, detail="Not Found")
