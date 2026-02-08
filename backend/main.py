import os
import re
import gzip
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from nw_api import router as nw_router
from nw_poll import start_nw_poller
from rankings_poll import start_rankings_poller
from auth_kg import router as auth_kg_router, ensure_auth_tables
from admin_api import router as admin_router, ensure_admin_tables

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

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


class RawReportBody(BaseModel):
    raw_text: str = Field(..., min_length=1, max_length=250000)


def ensure_recon_tables():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.attack_reports (
                    id BIGSERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    observed_at TIMESTAMPTZ,
                    target_kingdom TEXT NOT NULL,
                    target_networth BIGINT,
                    attack_result TEXT,
                    gains_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    casualties_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    raw_text TEXT NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.settlement_observations (
                    id BIGSERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    source_type TEXT NOT NULL,
                    source_report_id BIGINT,
                    kingdom TEXT NOT NULL,
                    settlement_name TEXT NOT NULL,
                    settlement_level INT,
                    settlement_tier TEXT,
                    event_type TEXT NOT NULL,
                    event_detail TEXT
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS settlement_observations_kingdom_idx
                ON public.settlement_observations (kingdom, created_at DESC);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS settlement_observations_settlement_idx
                ON public.settlement_observations (settlement_name, created_at DESC);
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS settlement_observations_source_unique_idx
                ON public.settlement_observations
                (source_type, source_report_id, kingdom, settlement_name, COALESCE(settlement_level, -1), event_type)
                WHERE source_report_id IS NOT NULL;
                """
            )
        conn.commit()
    finally:
        conn.close()


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


def _parse_received_at(text: str) -> Optional[datetime]:
    m = re.search(r"^\s*Received\s*:\s*(.+?)\s*$", text, flags=re.I | re.M)
    if not m:
        return None
    raw = m.group(1).strip()
    for fmt in ("%b %d, %Y, %I:%M:%S %p", "%B %d, %Y, %I:%M:%S %p"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            continue
    return None


def _parse_gain_list(chunk: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for part in chunk.split(","):
        p = part.strip()
        if not p:
            continue
        m = re.match(r"^([0-9][0-9,\s]*)\s+(.+?)$", p)
        if not m:
            continue
        n = _num(m.group(1))
        if n is None:
            continue
        out[m.group(2).strip()] = n
    return out


def _parse_casualty_list(chunk: str) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for part in chunk.split(","):
        p = part.strip()
        if not p:
            continue
        m = re.match(r"^([0-9][0-9,\s]*)\s*/\s*([0-9][0-9,\s]*)\s+(.+?)$", p)
        if not m:
            continue
        lost = _num(m.group(1))
        sent = _num(m.group(2))
        if lost is None or sent is None:
            continue
        unit = m.group(3).strip()
        out[unit] = {"lost": lost, "sent": sent}
    return out


def _parse_settlement_mentions(text: str) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []

    p1 = re.compile(
        r"(?i)the\s+(small|medium|large)\s+(?:town|city)\s+(.+?)\s+\(level\s+(\d+)\s+settlement\)"
    )
    p2 = re.compile(
        r"(?i)\b(.+?)\s+\(level\s+(\d+)\s+settlement\)"
    )
    for m in p1.finditer(text):
        tier = m.group(1).strip().lower()
        name = m.group(2).strip()
        lvl = _num(m.group(3))
        if not name or lvl is None:
            continue
        found.append(
            {
                "settlement_name": name,
                "settlement_level": lvl,
                "settlement_tier": tier,
            }
        )

    if not found:
        for line in text.splitlines():
            if "level" not in line.lower() or "settlement" not in line.lower():
                continue
            m = p2.search(line.strip())
            if not m:
                continue
            name = m.group(1).strip().lstrip("the ").strip()
            lvl = _num(m.group(2))
            if not name or lvl is None:
                continue
            found.append(
                {
                    "settlement_name": name,
                    "settlement_level": lvl,
                    "settlement_tier": None,
                }
            )
            break

    dedup = set()
    out: List[Dict[str, Any]] = []
    for r in found:
        key = (r["settlement_name"].lower(), r["settlement_level"], r["settlement_tier"] or "")
        if key in dedup:
            continue
        dedup.add(key)
        out.append(r)
    return out


def parse_attack_report(text: str) -> Dict[str, Any]:
    received_at = _parse_received_at(text)

    target = None
    target_networth = None
    m = re.search(r"^\s*Attack Report:\s*(.+?)\s*\(NW:\s*\+?\s*([0-9,]+)\)\s*$", text, flags=re.I | re.M)
    if m:
        target = m.group(1).strip()
        target_networth = _num(m.group(2))
    else:
        m2 = re.search(r"^\s*Subject:\s*Attack Report:\s*(.+?)\s*$", text, flags=re.I | re.M)
        if m2:
            target = m2.group(1).strip()

    result = _grab_line(text, "Attack Result")

    gains: Dict[str, int] = {}
    gm = re.search(
        r"You have gained the following during the attack:\s*(.+?)\s*$",
        text,
        flags=re.I | re.M,
    )
    if gm:
        gains = _parse_gain_list(gm.group(1))

    casualties: Dict[str, Dict[str, int]] = {}
    cm = re.search(
        r"We regret to inform you of the following casualties during the attack:\s*(.+?)\s*$",
        text,
        flags=re.I | re.M,
    )
    if cm:
        casualties = _parse_casualty_list(cm.group(1))

    settlement_mentions = _parse_settlement_mentions(text)
    settlement_event_type = "seen"
    line = ""
    for ln in text.splitlines():
        if "settlement" in ln.lower() and ("battle" in ln.lower() or "take the town" in ln.lower()):
            line = ln.strip()
            break
    low_line = line.lower()
    if "unable to take" in low_line:
        settlement_event_type = "take_attempt_failed"
    elif "captured" in low_line or "took the town" in low_line:
        settlement_event_type = "captured"
    elif "breach" in low_line:
        settlement_event_type = "breached"

    return {
        "target": target,
        "target_networth": target_networth,
        "attack_result": result,
        "gains": gains,
        "casualties": casualties,
        "received_at": received_at,
        "settlement_mentions": settlement_mentions,
        "settlement_event_type": settlement_event_type,
        "settlement_event_detail": line or None,
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


def _insert_settlement_observation(
    cur,
    *,
    source_type: str,
    source_report_id: Optional[int],
    kingdom: str,
    settlement_name: str,
    settlement_level: Optional[int],
    settlement_tier: Optional[str],
    event_type: str,
    event_detail: Optional[str],
)-> bool:
    cur.execute(
        """
        INSERT INTO public.settlement_observations
          (source_type, source_report_id, kingdom, settlement_name, settlement_level, settlement_tier, event_type, event_detail, created_at)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT DO NOTHING
        """,
        (
            source_type,
            source_report_id,
            kingdom,
            settlement_name,
            settlement_level,
            settlement_tier,
            event_type,
            event_detail,
        ),
    )
    return cur.rowcount > 0


@app.post("/api/reports/spy")
def ingest_report(body: RawReportBody):
    raw_text = body.raw_text.strip()
    if not raw_text:
        raise HTTPException(status_code=400, detail="raw_text is empty")

    is_attack = bool(re.search(r"^\s*Attack Report:\s*", raw_text, flags=re.I | re.M))

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if is_attack:
                parsed = parse_attack_report(raw_text)
                target = str(parsed.get("target") or "").strip()
                if not target:
                    raise HTTPException(status_code=400, detail="Could not parse attack target kingdom")

                cur.execute(
                    """
                    INSERT INTO public.attack_reports
                      (observed_at, target_kingdom, target_networth, attack_result, gains_json, casualties_json, raw_text, created_at)
                    VALUES
                      (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, now())
                    RETURNING id, created_at
                    """,
                    (
                        parsed.get("received_at"),
                        target,
                        parsed.get("target_networth"),
                        parsed.get("attack_result"),
                        json.dumps(parsed.get("gains") or {}),
                        json.dumps(parsed.get("casualties") or {}),
                        raw_text,
                    ),
                )
                stored = cur.fetchone()

                events = 0
                for s in parsed.get("settlement_mentions") or []:
                    inserted = _insert_settlement_observation(
                        cur,
                        source_type="attack",
                        source_report_id=stored["id"] if stored else None,
                        kingdom=target,
                        settlement_name=str(s.get("settlement_name") or "").strip(),
                        settlement_level=s.get("settlement_level"),
                        settlement_tier=s.get("settlement_tier"),
                        event_type=str(parsed.get("settlement_event_type") or "seen"),
                        event_detail=parsed.get("settlement_event_detail"),
                    )
                    if inserted:
                        events += 1

                conn.commit()
                return {
                    "ok": True,
                    "report_type": "attack",
                    "stored": stored,
                    "parsed": parsed,
                    "settlement_events": events,
                }

            parsed = parse_spy_report(raw_text)
            kingdom = str(parsed.get("target") or "").strip()
            if not kingdom:
                raise HTTPException(status_code=400, detail="Could not parse spy report target kingdom")

            cur.execute(
                """
                INSERT INTO public.spy_reports
                  (created_at, kingdom, alliance, defense_power, castles, raw)
                VALUES
                  (now(), %s, %s, %s, %s, %s)
                RETURNING id, created_at
                """,
                (
                    kingdom,
                    parsed.get("alliance"),
                    parsed.get("defender_dp"),
                    parsed.get("castles"),
                    raw_text,
                ),
            )
            stored = cur.fetchone()

            events = 0
            for s in _parse_settlement_mentions(raw_text):
                inserted = _insert_settlement_observation(
                    cur,
                    source_type="spy",
                    source_report_id=stored["id"] if stored else None,
                    kingdom=kingdom,
                    settlement_name=str(s.get("settlement_name") or "").strip(),
                    settlement_level=s.get("settlement_level"),
                    settlement_tier=s.get("settlement_tier"),
                    event_type="seen",
                    event_detail=None,
                )
                if inserted:
                    events += 1

        conn.commit()
        return {
            "ok": True,
            "report_type": "spy",
            "stored": stored,
            "parsed": parsed,
            "settlement_events": events,
        }
    finally:
        conn.close()


@app.get("/api/settlements/tracked")
def tracked_settlements(kingdom: str = "", limit: int = 500):
    s = kingdom.strip()
    lim = max(1, min(int(limit), 1000))

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if s:
                cur.execute(
                    """
                    SELECT
                        kingdom,
                        settlement_name,
                        MAX(settlement_level) AS latest_level,
                        MAX(created_at) AS last_seen_at,
                        COUNT(*)::int AS sightings,
                        COUNT(*) FILTER (WHERE event_type = 'take_attempt_failed')::int AS failed_take_attempts,
                        COUNT(*) FILTER (WHERE event_type = 'captured')::int AS captures
                    FROM public.settlement_observations
                    WHERE kingdom ILIKE %s
                    GROUP BY kingdom, settlement_name
                    ORDER BY last_seen_at DESC
                    LIMIT %s
                    """,
                    (f"%{s}%", lim),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        kingdom,
                        settlement_name,
                        MAX(settlement_level) AS latest_level,
                        MAX(created_at) AS last_seen_at,
                        COUNT(*)::int AS sightings,
                        COUNT(*) FILTER (WHERE event_type = 'take_attempt_failed')::int AS failed_take_attempts,
                        COUNT(*) FILTER (WHERE event_type = 'captured')::int AS captures
                    FROM public.settlement_observations
                    GROUP BY kingdom, settlement_name
                    ORDER BY last_seen_at DESC
                    LIMIT %s
                    """,
                    (lim,),
                )
            rows = cur.fetchall()
        return {"ok": True, "items": rows}
    finally:
        conn.close()


@app.post("/api/settlements/backfill")
def backfill_settlement_observations(
    token: str = "",
    from_id: int = 0,
    limit: int = 250000,
):
    expected = (os.getenv("SETTLEMENT_BACKFILL_TOKEN", "") or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="SETTLEMENT_BACKFILL_TOKEN is not set")
    if token != expected:
        raise HTTPException(status_code=403, detail="Invalid backfill token")

    lim = max(1, min(int(limit), 1_000_000))
    start_id = max(0, int(from_id))

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, kingdom, raw, raw_gz
                FROM public.spy_reports
                WHERE id > %s
                ORDER BY id ASC
                LIMIT %s
                """,
                (start_id, lim),
            )
            rows = cur.fetchall()

            scanned = 0
            reports_with_settlements = 0
            inserted_events = 0
            last_id = start_id

            for r in rows:
                scanned += 1
                rid = int(r.get("id"))
                last_id = max(last_id, rid)
                kingdom = str(r.get("kingdom") or "").strip()
                if not kingdom:
                    continue

                raw_text = _load_raw_text(r)
                if not raw_text:
                    continue

                mentions = _parse_settlement_mentions(raw_text)
                if not mentions:
                    continue

                reports_with_settlements += 1
                for s in mentions:
                    inserted = _insert_settlement_observation(
                        cur,
                        source_type="spy",
                        source_report_id=rid,
                        kingdom=kingdom,
                        settlement_name=str(s.get("settlement_name") or "").strip(),
                        settlement_level=s.get("settlement_level"),
                        settlement_tier=s.get("settlement_tier"),
                        event_type="seen",
                        event_detail="backfill",
                    )
                    if inserted:
                        inserted_events += 1

        conn.commit()
        return {
            "ok": True,
            "scanned_spy_reports": scanned,
            "reports_with_settlements": reports_with_settlements,
            "inserted_settlement_events": inserted_events,
            "next_from_id": last_id,
            "done": scanned < lim,
        }
    finally:
        conn.close()


# -------------------------
# Mount NW API
# -------------------------
app.include_router(nw_router, prefix="/api/nw", tags=["nw"])
app.include_router(auth_kg_router, tags=["auth", "kg"])
app.include_router(admin_router, tags=["admin"])


# -------------------------
# Startup: start pollers
# -------------------------
@app.on_event("startup")
def _startup():
    world_id = os.getenv("KG_WORLD_ID", "1")

    rankings_seconds = int(os.getenv("RANKINGS_POLL_SECONDS", "900"))
    nw_seconds = int(os.getenv("NW_POLL_SECONDS", "240"))

    ensure_auth_tables()
    ensure_admin_tables()
    ensure_recon_tables()
    start_rankings_poller(poll_seconds=rankings_seconds, world_id=world_id)
    start_nw_poller(poll_seconds=nw_seconds)


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
