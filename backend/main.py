import os
import re
import gzip
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from rankings_poll import start_rankings_poller
from nw_poll import start_nw_poller

WORLD_ID = os.getenv("KG_WORLD_ID", "1")
KG_TOKEN = os.getenv("KG_TOKEN", "")

start_rankings_poller(poll_seconds=900, world_id=WORLD_ID, kg_token=KG_TOKEN)
start_nw_poller(poll_seconds=240, world_id=WORLD_ID, kg_token=KG_TOKEN)

# NEW
from nw_api import router as nw_router
from nw_poll import start_nw_poller

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

@app.get("/api/routes")
def list_routes():
    out = []
    for r in app.routes:
        methods = sorted(list(getattr(r, "methods", []) or []))
        path = getattr(r, "path", "")
        name = getattr(r, "name", "")
        out.append({"path": path, "methods": methods, "name": name})
    return {"ok": True, "routes": out}


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


# -------------------------
# Spy report parsing (raw -> structured)
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


def _num_float(s: Optional[str]) -> Optional[float]:
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


def parse_spy_report(text: str) -> Dict[str, Any]:
    target = _grab_line(text, "Target")
    alliance = _grab_line(text, "Alliance")
    honour = _num_float(_grab_line(text, "Honour"))
    ranking = _num(_grab_line(text, "Ranking"))
    networth = _num(_grab_line(text, "Networth"))
    spies_sent = _num(_grab_line(text, "Spies Sent"))
    spies_lost = _num(_grab_line(text, "Spies Lost"))
    result_level = _grab_line(text, "Result Level")
    castles = _num(_grab_line(text, "Number of Castles"))

    defender_dp = None
    m = re.search(r"Approximate defensive power\*?\s*:\s*([0-9,\.e\+]+)", text, flags=re.I)
    if m:
        try:
            defender_dp = int(float(m.group(1).replace(",", "")))
        except Exception:
            defender_dp = None

    resources_chunk = _section(
        text,
        "Our spies also found the following information about the kingdom's resources:",
        ["Our spies also found the following information about the kingdom's troops:"],
    )
    troops_chunk = _section(
        text,
        "Our spies also found the following information about the kingdom's troops:",
        [
            "The following information was found regarding troop movements",
            "The following recent market transactions were also discovered:",
            "The following technology information was also discovered:",
            "Our spies also found the following information about the small town",
            "Our spies also found the following information about the medium town",
            "Our spies also found the following information about the large town",
        ],
    )

    resources = _parse_kv_lines(resources_chunk)
    troops_raw = _parse_kv_lines(troops_chunk)

    troops: Dict[str, int] = {}
    for k, v in troops_raw.items():
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
        "defender_dp": defender_dp,
        "resources": resources,
        "troops": troops,
    }


def _load_raw_text(row: Dict[str, Any]) -> str:
    raw = row.get("raw")
    if raw and isinstance(raw, str) and raw.strip():
        return raw

    raw_gz = row.get("raw_gz")
    if raw_gz:
        try:
            return gzip.decompress(raw_gz).decode("utf-8", errors="replace")
        except Exception:
            pass

    return ""


# -------------------------
# API: Kingdom list
# -------------------------
@app.get("/api/kingdoms")
def list_kingdoms(search: str = "", limit: int = 500):
    s = search.strip()

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if s:
                like = f"%{s}%"
                cur.execute(
                    """
                    SELECT
                        kingdom,
                        COALESCE(alliance, '') AS alliance,
                        COUNT(*)::int AS report_count,
                        MAX(created_at) AS latest_report_at
                    FROM public.spy_reports
                    WHERE kingdom ILIKE %s OR COALESCE(alliance,'') ILIKE %s
                    GROUP BY kingdom, COALESCE(alliance,'')
                    ORDER BY latest_report_at DESC
                    LIMIT %s
                    """,
                    (like, like, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        kingdom,
                        COALESCE(alliance, '') AS alliance,
                        COUNT(*)::int AS report_count,
                        MAX(created_at) AS latest_report_at
                    FROM public.spy_reports
                    GROUP BY kingdom, COALESCE(alliance,'')
                    ORDER BY latest_report_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append(
                {
                    "name": r["kingdom"],
                    "alliance": r["alliance"] or None,
                    "report_count": r["report_count"],
                    "latest_report_at": r["latest_report_at"],
                }
            )

        return {"ok": True, "kingdoms": out}
    finally:
        conn.close()


# -------------------------
# API: Spy reports for kingdom
# -------------------------
@app.get("/api/kingdoms/{kingdom}/spy-reports")
def list_spy_reports(kingdom: str, limit: int = 50):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, created_at, kingdom, alliance, defense_power, castles, raw, raw_gz
                FROM public.spy_reports
                WHERE kingdom = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (kingdom, limit),
            )
            rows = cur.fetchall()

        reports = []
        for r in rows:
            raw_text = _load_raw_text(r)
            parsed = parse_spy_report(raw_text) if raw_text else {}
            reports.append(
                {
                    "id": r["id"],
                    "created_at": r["created_at"],
                    "kingdom": r["kingdom"],
                    "alliance": r.get("alliance"),
                    "defense_power": r.get("defense_power"),
                    "castles": r.get("castles"),
                    "parsed": parsed,
                    "troop_keys": sorted(list((parsed.get("troops") or {}).keys()))[:50],
                    "resource_keys": sorted(list((parsed.get("resources") or {}).keys()))[:50],
                }
            )

        return {"ok": True, "kingdom": kingdom, "reports": reports}
    finally:
        conn.close()


# -------------------------
# API: Raw report
# -------------------------
@app.get("/api/spy-reports/{report_id}/raw", response_class=PlainTextResponse)
def get_spy_report_raw(report_id: int):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT raw, raw_gz FROM public.spy_reports WHERE id = %s", (report_id,))
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Raw report not found")

        raw_text = _load_raw_text(row)
        if not raw_text:
            raise HTTPException(status_code=404, detail="Raw report not found")

        return raw_text
    finally:
        conn.close()


@app.get("/api/spy-reports/{report_id}")
def get_spy_report(report_id: int):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, created_at, kingdom, alliance, defense_power, castles, raw, raw_gz
                FROM public.spy_reports
                WHERE id = %s
                """,
                (report_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Report not found")

        raw_text = _load_raw_text(row)
        parsed = parse_spy_report(raw_text) if raw_text else {}
        return {
            "ok": True,
            "report": {
                "id": row["id"],
                "created_at": row["created_at"],
                "kingdom": row["kingdom"],
                "alliance": row.get("alliance"),
                "defense_power": row.get("defense_power"),
                "castles": row.get("castles"),
                "parsed": parsed,
                "raw_text": raw_text,
            },
        }
    finally:
        conn.close()


# -------------------------
# NEW: Mount NW API + start poller
# -------------------------
app.include_router(nw_router, prefix="/api/nw", tags=["nw"])

@app.on_event("startup")
def _startup():
    # Poll interval in seconds (4 minutes)
    poll_seconds = int(os.getenv("NW_POLL_SECONDS", "240"))

    # World id
    world_id = os.getenv("KG_WORLD_ID", "1")

    # Token (optional for NWOT, likely required for rankings once we add it)
    kg_token = os.getenv("KG_TOKEN", "")

    # Start poller safely
    start_nw_poller(
        poll_seconds=poll_seconds,
        world_id=world_id,
        kg_token=kg_token,
    )


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
