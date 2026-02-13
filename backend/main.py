import os
import re
import gzip
import json
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import jwt
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Request
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
    return FileResponse(
        str(p),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


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


JWT_COOKIE_NAME = "rh_session"


def _jwt_secret() -> str:
    secret = os.getenv("JWT_SECRET", "").strip()
    if not secret:
        raise HTTPException(status_code=500, detail="JWT_SECRET is not set")
    return secret


def _decode_session_jwt(token: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, _jwt_secret(), algorithms=["HS256"])
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid session")


def _admin_user_ids() -> set[str]:
    raw = os.getenv("DEV_USER_IDS", "").strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _enforce_alliance_scoping() -> bool:
    return (os.getenv("ENFORCE_ALLIANCE_SCOPING", "false").strip().lower() in {"1", "true", "yes", "on"})


def _get_scope_from_request(request: Request) -> Optional[Dict[str, Any]]:
    if not _enforce_alliance_scoping():
        return None

    token = request.cookies.get(JWT_COOKIE_NAME, "")
    if not token:
        raise HTTPException(status_code=401, detail="Login required")
    claims = _decode_session_jwt(token)
    uid = str(claims.get("sub") or "").strip()
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid session")

    if uid in _admin_user_ids():
        return None

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id, a.name
                FROM public.alliance_memberships m
                JOIN public.alliances a ON a.id = m.alliance_id
                WHERE m.discord_user_id = %s AND m.status = 'active'
                ORDER BY a.name
                """,
                (uid,),
            )
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(status_code=403, detail="No active alliance memberships")

            cur.execute(
                """
                SELECT a.name
                FROM public.user_active_alliance ua
                JOIN public.alliances a ON a.id = ua.alliance_id
                WHERE ua.discord_user_id = %s
                """,
                (uid,),
            )
            active_row = cur.fetchone() or {}

        names = [str(r.get("name") or "").strip() for r in rows if str(r.get("name") or "").strip()]
        active_name = str(active_row.get("name") or "").strip()
        if active_name and active_name in names:
            names = [active_name]
        return {"discord_user_id": uid, "alliance_names": names}
    finally:
        conn.close()


def _require_user_id(request: Request) -> str:
    token = request.cookies.get(JWT_COOKIE_NAME, "")
    if not token:
        raise HTTPException(status_code=401, detail="Login required")
    claims = _decode_session_jwt(token)
    uid = str(claims.get("sub") or "").strip()
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid session")
    return uid


class RawReportBody(BaseModel):
    raw_text: str = Field(..., min_length=1, max_length=250000)


class KnownHitBody(BaseModel):
    target: str = Field(default="", max_length=120)
    rawRatio: float = Field(..., gt=0)
    calibratedRatio: Optional[float] = None
    predictedOutcome: Optional[str] = Field(default="", max_length=60)
    actualOutcome: str = Field(..., min_length=1, max_length=60)
    atkPower: Optional[float] = None
    defDP: Optional[float] = None
    landTaken: Optional[float] = None
    note: Optional[str] = Field(default="", max_length=500)


class UserResearchBody(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    affects: str = Field(default="all", max_length=40)
    ratePerLevel: float = Field(default=0, ge=0, le=1)
    level: int = Field(default=0, ge=0, le=10000)
    apUp: bool = False
    dpUp: bool = False
    speedUp: bool = False
    casualtyReduction: bool = False
    notes: Optional[str] = Field(default="", max_length=1000)


DEFAULT_ALLIANCES: List[tuple[str, str]] = [
    ("nwo-1", "[NWO-1] NWO-1"),
    ("a-taem", "[A_TAEM] THE A-TEAM"),
    ("mk", "[MK] Mom's Knights"),
    ("myrmr", "[MYRMR] MYRM Reborn"),
    ("kga", "[KGA] Kingdom Game Addicts"),
    ("nnwo", "[NNWO] The Iron Veil"),
    ("tc", "[TC] The Continental"),
    ("kotf", "[KOTF] Knights of the Fire"),
    ("mg", "[MG] Maiden Gully"),
    ("vlhla", "[VLHLA] Valhalla"),
    ("kotf2", "[kOTF2] Knights of the Flame"),
    ("cru", "[CRU] Crusaders"),
    ("given", "[Given] The Unforgiven"),
    ("tgl", "[TGL] The Grand Library"),
    ("wr", "[WR] Whiskyrides"),
    ("wtfc", "[WTFC] Home For The Bewildered"),
    ("tdk", "[TDK] THE DARK KNIGHTS"),
    ("lmj", "[LMJ] LeRoyMfnJenkins"),
    ("omo", "[OMO] Odd Men Out"),
    ("hsh", "[HSH] 1|o01|O00Ol"),
    ("rlx", "[RLX] Break"),
    ("horosha", "[HOROSHA] Spy Killer"),
    ("301", "[301] 301"),
    ("kayam", "[KAYAM] Bhayamgak"),
    ("ihd", "[Ihd] I hate dave"),
    ("kotc", "[KotC] Knights of the Cross"),
    ("oss", "[OSS] The Ossuary"),
    ("h", "[H] BUSHIDO"),
    ("zero", "[Zero] Nobody"),
    ("og", "[OG] Oldies but Goldies"),
    ("moon", "[Moon] Halve Maen"),
    ("kgsmn", "[KGSMN] The Kingsman"),
    ("dh", "[DH] Dawg House"),
    ("608", "[6o8] Six of Eight"),
    ("27-4", "[27/4] Warriors -24/7"),
    ("tct", "[TCT] Continental Tea Room"),
    ("uka", "[UKA] United Kingdom's of Alluvia"),
    ("valor", "[Valor] The Midnight Aristocracy"),
    ("res", "[Res] Resistance"),
    ("spqr", "[SPQR] Roman Empire"),
    ("tk", "[TK] The Knights"),
    ("pjb", "[PJB] Phuck Joe Biden"),
    ("twrp", "[TWRP] The Winter Rose Pact"),
    ("rome", "[ROME] Rulers of Middle Earth"),
    ("ferda", "[FERDA] FER DA BOYS"),
    ("tcs", "[TCS] The Continental Saloon"),
    ("kog", "[KOG] The Kingdom Of Granthall"),
    ("bigns", "[Bigns] Biggin's"),
    ("earth", "[Earth] Earth Royalty"),
    ("dark", "[DARK] DARKNESS"),
    ("bob", "[B.o.B] Band of Brothers"),
    ("war", "[War] Black knights"),
    ("imi", "[IMI] Ignatius Martis Invictus"),
    ("cars", "[CARS] Cars"),
    ("dad", "[DAD] DAD"),
    ("a-a", "[A-A] Achaemenids"),
    ("fire", "[Fire] Atlantis"),
    ("netc", "[NETC] Noble Exotics Trading Co."),
    ("lnc", "[LNC] Lioncry"),
    ("koa", "[KOA] Kingz of All"),
]


def seed_default_alliances():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            for slug, name in DEFAULT_ALLIANCES:
                cur.execute(
                    """
                    INSERT INTO public.alliances (slug, name, created_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (slug) DO UPDATE
                    SET name = EXCLUDED.name
                    """,
                    (slug, name),
                )
        conn.commit()
    finally:
        conn.close()


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
                CREATE TABLE IF NOT EXISTS public.alliances (
                    id BIGSERIAL PRIMARY KEY,
                    slug TEXT UNIQUE NOT NULL,
                    name TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.app_users (
                    discord_user_id TEXT PRIMARY KEY,
                    discord_username TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.alliance_memberships (
                    id BIGSERIAL PRIMARY KEY,
                    alliance_id BIGINT NOT NULL REFERENCES public.alliances(id) ON DELETE CASCADE,
                    discord_user_id TEXT NOT NULL REFERENCES public.app_users(discord_user_id) ON DELETE CASCADE,
                    role TEXT NOT NULL DEFAULT 'member',
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (alliance_id, discord_user_id)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.user_active_alliance (
                    discord_user_id TEXT PRIMARY KEY REFERENCES public.app_users(discord_user_id) ON DELETE CASCADE,
                    alliance_id BIGINT NOT NULL REFERENCES public.alliances(id) ON DELETE CASCADE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.calc_known_hits (
                    id BIGSERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    created_by_discord_user_id TEXT,
                    alliance_scope TEXT,
                    target TEXT NOT NULL DEFAULT '',
                    target_norm TEXT NOT NULL DEFAULT '',
                    raw_ratio DOUBLE PRECISION NOT NULL,
                    calibrated_ratio DOUBLE PRECISION,
                    predicted_outcome TEXT,
                    actual_outcome TEXT NOT NULL,
                    atk_power DOUBLE PRECISION,
                    def_dp DOUBLE PRECISION,
                    land_taken DOUBLE PRECISION,
                    note TEXT
                );
                """
            )
            cur.execute(
                """
                ALTER TABLE public.calc_known_hits
                ADD COLUMN IF NOT EXISTS source_attack_report_id BIGINT;
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS calc_known_hits_target_idx
                ON public.calc_known_hits (target_norm, created_at DESC);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS calc_known_hits_alliance_idx
                ON public.calc_known_hits (alliance_scope, created_at DESC);
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS calc_known_hits_source_attack_unique_idx
                ON public.calc_known_hits (source_attack_report_id)
                WHERE source_attack_report_id IS NOT NULL;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.user_attack_research (
                    id BIGSERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    discord_user_id TEXT NOT NULL REFERENCES public.app_users(discord_user_id) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    affects TEXT NOT NULL DEFAULT 'all',
                    rate_per_level DOUBLE PRECISION NOT NULL DEFAULT 0,
                    level INT NOT NULL DEFAULT 0,
                    ap_up BOOLEAN NOT NULL DEFAULT false,
                    dp_up BOOLEAN NOT NULL DEFAULT false,
                    speed_up BOOLEAN NOT NULL DEFAULT false,
                    casualty_reduction BOOLEAN NOT NULL DEFAULT false,
                    notes TEXT,
                    UNIQUE(discord_user_id, name)
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS user_attack_research_user_idx
                ON public.user_attack_research (discord_user_id, updated_at DESC);
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


def _parse_research_levels(text: str) -> Dict[str, int]:
    chunk = _section(
        text,
        "The following technology information was also discovered:",
        [
            "The following recent market transactions were also discovered:",
            "Our spies also found the following information about the",
            "The following information was found regarding troop movements",
        ],
    )
    if not chunk:
        return {}

    out: Dict[str, int] = {}
    for raw_line in chunk.splitlines():
        line = raw_line.strip().lstrip("-*• ").strip()
        if not line:
            continue

        # Common forms:
        #   Horse Breeding Lv 10
        #   Horse Breeding level 10
        #   Horse Breeding: 10
        m = re.match(r"^(.+?)\s+(?:lv\.?|lvl\.?|level)\s*([0-9]{1,3})\s*$", line, flags=re.I)
        if not m:
            m = re.match(r"^([^:]{2,80}?)\s*:\s*([0-9]{1,3})\s*$", line, flags=re.I)
        if not m:
            continue

        name = str(m.group(1) or "").strip()
        try:
            lvl = int(m.group(2))
        except Exception:
            continue
        if not name or lvl <= 0:
            continue

        prev = out.get(name, 0)
        if lvl > prev:
            out[name] = lvl

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

    research_levels = _parse_research_levels(text)

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
        "research_levels": research_levels,
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
    p1b = re.compile(
        r"(?i)about\s+the\s+(small|medium|large)\s+(?:town|city)\s+(.+?)\s+\(level\s+(\d+)\s+settlement\)"
    )
    p1c = re.compile(
        r"(?i)about\s+the\s+(small|medium|large)\s+(?:town|city)\s+(.+?)\s*:"
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

    for m in p1b.finditer(text):
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

    for m in p1c.finditer(text):
        tier = m.group(1).strip().lower()
        name = m.group(2).strip()
        if not name:
            continue
        found.append(
            {
                "settlement_name": name,
                "settlement_level": None,
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
        key = (r["settlement_name"].lower(), r["settlement_level"] if r["settlement_level"] is not None else -1, r["settlement_tier"] or "")
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


AUTO_ATTACK_WEIGHTS = {
    "footmen": 1.0,
    "pikemen": 2.0,
    "elites": 10.0,
    "archers": 1.0,
    "crossbowmen": 3.0,
    "lightCav": 5.0,
    "heavyCav": 7.0,
    "knights": 15.0,
}

AUTO_DEFENSE_WEIGHTS = {
    "peasants": 0.1,
    "footmen": 1.0,
    "pikemen": 2.0,
    "elites": 10.0,
    "archers": 4.0,
    "crossbowmen": 2.0,
    "lightCav": 4.0,
    "heavyCav": 5.0,
    "knights": 10.0,
}


def _auto_norm_unit(name: str) -> Optional[str]:
    n = str(name or "").strip().lower()
    if not n:
        return None
    if "foot" in n:
        return "footmen"
    if "pike" in n:
        return "pikemen"
    if "elite" in n:
        return "elites"
    if "crossbow" in n:
        return "crossbowmen"
    if "archer" in n:
        return "archers"
    if "light" in n and "cav" in n:
        return "lightCav"
    if "heavy" in n and "cav" in n:
        return "heavyCav"
    if "knight" in n:
        return "knights"
    if "peasant" in n:
        return "peasants"
    if "cav" in n:
        return "lightCav"
    return None


def _auto_zero_units() -> Dict[str, int]:
    return {
        "peasants": 0,
        "footmen": 0,
        "pikemen": 0,
        "elites": 0,
        "archers": 0,
        "crossbowmen": 0,
        "lightCav": 0,
        "heavyCav": 0,
        "knights": 0,
    }


def _auto_grouped(units: Dict[str, int]) -> Dict[str, int]:
    return {
        "infantry": int(units.get("footmen", 0) + units.get("pikemen", 0) + units.get("elites", 0)),
        "archers": int(units.get("archers", 0) + units.get("crossbowmen", 0)),
        "cavalry": int(units.get("lightCav", 0) + units.get("heavyCav", 0) + units.get("knights", 0)),
        "pike": int(units.get("pikemen", 0)),
    }


def _auto_counter_reduction(counter_count: int, target_count: int, need_ratio: float) -> float:
    c = float(counter_count or 0)
    t = float(target_count or 0)
    if c <= 0 or t <= 0 or need_ratio <= 0:
        return 0.0
    needed = t * need_ratio
    if needed <= 0:
        return 0.0
    coverage = c / needed
    pct = 0.25 * coverage
    return max(0.0, min(0.40, pct))


def _auto_attack_units_from_casualties(casualties: Dict[str, Dict[str, int]]) -> Dict[str, int]:
    out = _auto_zero_units()
    for unit_name, item in (casualties or {}).items():
        key = _auto_norm_unit(unit_name)
        if not key:
            continue
        sent = _num(str((item or {}).get("sent") or "0")) or 0
        out[key] = int(out.get(key, 0) + max(0, int(sent)))
    return out


def _auto_defender_units_from_spy(parsed_spy: Dict[str, Any]) -> Dict[str, int]:
    out = _auto_zero_units()
    for unit_name, count in ((parsed_spy or {}).get("troops") or {}).items():
        key = _auto_norm_unit(str(unit_name))
        if not key:
            continue
        out[key] = int(out.get(key, 0) + max(0, int(count or 0)))
    return out


def _auto_compute_attack_power(attacker_units: Dict[str, int], defender_units: Dict[str, int]) -> float:
    a = attacker_units
    d = defender_units
    ag = _auto_grouped(a)
    dg = _auto_grouped(d)

    infantry_ap = (
        a.get("footmen", 0) * AUTO_ATTACK_WEIGHTS["footmen"]
        + a.get("pikemen", 0) * AUTO_ATTACK_WEIGHTS["pikemen"]
        + a.get("elites", 0) * AUTO_ATTACK_WEIGHTS["elites"]
    )
    archer_ap = (
        a.get("archers", 0) * AUTO_ATTACK_WEIGHTS["archers"]
        + a.get("crossbowmen", 0) * AUTO_ATTACK_WEIGHTS["crossbowmen"]
    )
    cav_ap = (
        a.get("lightCav", 0) * AUTO_ATTACK_WEIGHTS["lightCav"]
        + a.get("heavyCav", 0) * AUTO_ATTACK_WEIGHTS["heavyCav"]
        + a.get("knights", 0) * AUTO_ATTACK_WEIGHTS["knights"]
    )

    def_pike_vs_atk_cav = _auto_counter_reduction(dg["pike"], ag["cavalry"], 0.25)
    def_cav_vs_atk_arch = _auto_counter_reduction(dg["cavalry"], ag["archers"], 1.0)
    def_arch_vs_atk_inf = _auto_counter_reduction(dg["archers"], ag["infantry"], 1.0)

    infantry_ap *= (1.0 - def_arch_vs_atk_inf)
    archer_ap *= (1.0 - def_cav_vs_atk_arch)
    cav_ap *= (1.0 - def_pike_vs_atk_cav)
    return max(0.0, infantry_ap + archer_ap + cav_ap)


def _auto_compute_troop_dp(defender_units: Dict[str, int], attacker_units: Dict[str, int]) -> float:
    d = defender_units
    a = attacker_units
    ag = _auto_grouped(a)
    dg = _auto_grouped(d)

    atk_pike_vs_def_cav = _auto_counter_reduction(ag["pike"], dg["cavalry"], 0.25)
    atk_cav_vs_def_arch = _auto_counter_reduction(ag["cavalry"], dg["archers"], 1.0)
    atk_arch_vs_def_inf = _auto_counter_reduction(ag["archers"], dg["infantry"], 1.0)

    infantry_dp = (
        d.get("footmen", 0) * AUTO_DEFENSE_WEIGHTS["footmen"]
        + d.get("pikemen", 0) * AUTO_DEFENSE_WEIGHTS["pikemen"]
        + d.get("elites", 0) * AUTO_DEFENSE_WEIGHTS["elites"]
    )
    archer_dp = (
        d.get("archers", 0) * AUTO_DEFENSE_WEIGHTS["archers"]
        + d.get("crossbowmen", 0) * AUTO_DEFENSE_WEIGHTS["crossbowmen"]
    )
    cav_dp = (
        d.get("lightCav", 0) * AUTO_DEFENSE_WEIGHTS["lightCav"]
        + d.get("heavyCav", 0) * AUTO_DEFENSE_WEIGHTS["heavyCav"]
        + d.get("knights", 0) * AUTO_DEFENSE_WEIGHTS["knights"]
    )
    peasant_dp = d.get("peasants", 0) * AUTO_DEFENSE_WEIGHTS["peasants"]

    infantry_dp *= (1.0 - atk_arch_vs_def_inf)
    archer_dp *= (1.0 - atk_cav_vs_def_arch)
    cav_dp *= (1.0 - atk_pike_vs_def_cav)
    return max(0.0, infantry_dp + archer_dp + cav_dp + peasant_dp)


def _auto_extract_land_taken(gains: Dict[str, int]) -> Optional[int]:
    for k, v in (gains or {}).items():
        lk = str(k or "").lower()
        if "land" in lk or "acre" in lk:
            try:
                return int(v)
            except Exception:
                return None
    return None


def _auto_insert_known_hit_for_attack(cur, attack_report_id: int, attack_created_at: datetime, parsed_attack: Dict[str, Any]) -> bool:
    target = str((parsed_attack or {}).get("target") or "").strip()
    if not target:
        return False

    cur.execute(
        """
        SELECT id, created_at, kingdom, alliance, defense_power, castles, raw, raw_gz
        FROM public.spy_reports
        WHERE kingdom = %s
          AND created_at <= %s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (target, attack_created_at),
    )
    spy = cur.fetchone()
    if not spy:
        return False

    spy_raw = _load_raw_text(spy)
    if not spy_raw:
        return False
    spy_parsed = parse_spy_report(spy_raw)
    attacker_units = _auto_attack_units_from_casualties((parsed_attack or {}).get("casualties") or {})
    defender_units = _auto_defender_units_from_spy(spy_parsed)
    attack_power = _auto_compute_attack_power(attacker_units, defender_units)

    castles = int(spy_parsed.get("castles") or 0)
    castle_mult = 1.0 + (pow(max(0, castles), 0.5) / 100.0 if castles > 0 else 0.0)
    base_dp_from_spy = float(spy_parsed.get("defender_dp") or 0)
    if base_dp_from_spy > 0:
        defender_dp = base_dp_from_spy * castle_mult
    else:
        defender_dp = _auto_compute_troop_dp(defender_units, attacker_units) * castle_mult
    if defender_dp <= 0:
        return False

    raw_ratio = attack_power / max(1.0, defender_dp)
    land_taken = _auto_extract_land_taken((parsed_attack or {}).get("gains") or {})
    actual_outcome = str((parsed_attack or {}).get("attack_result") or "").strip() or "UNKNOWN"

    cur.execute(
        """
        INSERT INTO public.calc_known_hits (
            created_by_discord_user_id, alliance_scope, target, target_norm, raw_ratio, calibrated_ratio,
            predicted_outcome, actual_outcome, atk_power, def_dp, land_taken, note, source_attack_report_id, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (source_attack_report_id) DO NOTHING
        """,
        (
            None,
            None,
            target,
            _target_norm(target),
            float(raw_ratio),
            None,
            "",
            actual_outcome,
            float(attack_power),
            float(defender_dp),
            (int(land_taken) if land_taken is not None else None),
            f"auto: attack_report_id={int(attack_report_id)} linked_spy_id={int(spy.get('id') or 0)}",
            int(attack_report_id),
        ),
    )
    return cur.rowcount > 0


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
def list_kingdoms(request: Request, search: str = "", limit: int = 500):
    s = search.strip()
    scope = _get_scope_from_request(request)
    scoped_alliances = (scope or {}).get("alliance_names") or []

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if s and scoped_alliances:
                like = f"%{s}%"
                cur.execute(
                    """
                    SELECT
                        kingdom,
                        COALESCE(alliance, '') AS alliance,
                        COUNT(*)::int AS report_count,
                        MAX(created_at) AS latest_report_at
                    FROM public.spy_reports
                    WHERE (kingdom ILIKE %s OR COALESCE(alliance,'') ILIKE %s)
                      AND COALESCE(alliance,'') = ANY(%s)
                    GROUP BY kingdom, COALESCE(alliance,'')
                    ORDER BY latest_report_at DESC
                    LIMIT %s
                    """,
                    (like, like, scoped_alliances, limit),
                )
            elif s:
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
            elif scoped_alliances:
                cur.execute(
                    """
                    SELECT
                        kingdom,
                        COALESCE(alliance, '') AS alliance,
                        COUNT(*)::int AS report_count,
                        MAX(created_at) AS latest_report_at
                    FROM public.spy_reports
                    WHERE COALESCE(alliance,'') = ANY(%s)
                    GROUP BY kingdom, COALESCE(alliance,'')
                    ORDER BY latest_report_at DESC
                    LIMIT %s
                    """,
                    (scoped_alliances, limit),
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
def list_spy_reports(request: Request, kingdom: str, limit: int = 50):
    scope = _get_scope_from_request(request)
    scoped_alliances = (scope or {}).get("alliance_names") or []
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if scoped_alliances:
                cur.execute(
                    """
                    SELECT id, created_at, kingdom, alliance, defense_power, castles, raw, raw_gz
                    FROM public.spy_reports
                    WHERE kingdom = %s
                      AND COALESCE(alliance,'') = ANY(%s)
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    (kingdom, scoped_alliances, limit),
                )
            else:
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
                    "research_keys": sorted(list((parsed.get("research_levels") or {}).keys()))[:100],
                }
            )

        return {"ok": True, "kingdom": kingdom, "reports": reports}
    finally:
        conn.close()


def _target_norm(v: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(v or "").strip().lower())


def _known_hit_to_api(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(row.get("id")),
        "ts": (row.get("created_at").isoformat() if row.get("created_at") else None),
        "target": row.get("target") or "",
        "targetNorm": row.get("target_norm") or "",
        "rawRatio": float(row.get("raw_ratio") or 0),
        "calibratedRatio": float(row.get("calibrated_ratio") or 0),
        "predictedOutcome": row.get("predicted_outcome") or "",
        "actualOutcome": row.get("actual_outcome") or "",
        "atkPower": float(row.get("atk_power") or 0),
        "defDP": float(row.get("def_dp") or 0),
        "landTaken": row.get("land_taken"),
        "note": row.get("note") or None,
        "allianceScope": row.get("alliance_scope") or None,
        "createdBy": row.get("created_by_discord_user_id") or None,
        "sourceAttackReportId": row.get("source_attack_report_id"),
    }


@app.get("/api/calc/known-hits")
def list_known_hits(request: Request, limit: int = 1000, target: Optional[str] = None):
    conn = _connect()
    try:
        where = ""
        args: List[Any] = []
        if target and target.strip():
            where = " WHERE target_norm = %s"
            args.append(_target_norm(target))
        args.append(max(1, min(int(limit or 1000), 5000)))

        q = f"""
            SELECT id, created_at, target, target_norm, raw_ratio, calibrated_ratio, predicted_outcome,
                   actual_outcome, atk_power, def_dp, land_taken, note, alliance_scope, created_by_discord_user_id,
                   source_attack_report_id
            FROM public.calc_known_hits
            {where}
            ORDER BY created_at DESC, id DESC
            LIMIT %s
        """
        with conn.cursor() as cur:
            cur.execute(q, tuple(args))
            rows = cur.fetchall()
        return {"ok": True, "hits": [_known_hit_to_api(r) for r in rows]}
    finally:
        conn.close()


@app.post("/api/calc/known-hits")
def create_known_hit(request: Request, body: KnownHitBody):
    uid = _require_user_id(request)
    target = str(body.target or "").strip()

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.calc_known_hits (
                    created_by_discord_user_id, alliance_scope, target, target_norm, raw_ratio, calibrated_ratio,
                    predicted_outcome, actual_outcome, atk_power, def_dp, land_taken, note
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at, target, target_norm, raw_ratio, calibrated_ratio, predicted_outcome,
                          actual_outcome, atk_power, def_dp, land_taken, note, alliance_scope, created_by_discord_user_id,
                          source_attack_report_id
                """,
                (
                    uid,
                    None,
                    target,
                    _target_norm(target),
                    float(body.rawRatio),
                    (float(body.calibratedRatio) if body.calibratedRatio is not None else None),
                    str(body.predictedOutcome or "").strip(),
                    str(body.actualOutcome or "").strip(),
                    (float(body.atkPower) if body.atkPower is not None else None),
                    (float(body.defDP) if body.defDP is not None else None),
                    (float(body.landTaken) if body.landTaken is not None else None),
                    (str(body.note or "").strip() or None),
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return {"ok": True, "hit": _known_hit_to_api(row or {})}
    finally:
        conn.close()


@app.put("/api/calc/known-hits/{hit_id}")
def update_known_hit(request: Request, hit_id: int, body: KnownHitBody):
    _require_user_id(request)
    target = str(body.target or "").strip()

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE public.calc_known_hits
                SET updated_at = now(),
                    target = %s,
                    target_norm = %s,
                    raw_ratio = %s,
                    calibrated_ratio = %s,
                    predicted_outcome = %s,
                    actual_outcome = %s,
                    atk_power = %s,
                    def_dp = %s,
                    land_taken = %s,
                    note = %s
                WHERE id = %s
                RETURNING id, created_at, target, target_norm, raw_ratio, calibrated_ratio, predicted_outcome,
                          actual_outcome, atk_power, def_dp, land_taken, note, alliance_scope, created_by_discord_user_id,
                          source_attack_report_id
                """,
                (
                    target,
                    _target_norm(target),
                    float(body.rawRatio),
                    (float(body.calibratedRatio) if body.calibratedRatio is not None else None),
                    str(body.predictedOutcome or "").strip(),
                    str(body.actualOutcome or "").strip(),
                    (float(body.atkPower) if body.atkPower is not None else None),
                    (float(body.defDP) if body.defDP is not None else None),
                    (float(body.landTaken) if body.landTaken is not None else None),
                    (str(body.note or "").strip() or None),
                    int(hit_id),
                ),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Known hit not found")
        conn.commit()
        return {"ok": True, "hit": _known_hit_to_api(row)}
    finally:
        conn.close()


@app.delete("/api/calc/known-hits/{hit_id}")
def delete_known_hit(request: Request, hit_id: int):
    _require_user_id(request)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM public.calc_known_hits WHERE id = %s",
                (int(hit_id),),
            )
            deleted = cur.rowcount
        conn.commit()
        return {"ok": True, "deleted": int(deleted or 0)}
    finally:
        conn.close()


@app.delete("/api/calc/known-hits")
def clear_known_hits(request: Request):
    _require_user_id(request)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.calc_known_hits")
            deleted = cur.rowcount
        conn.commit()
        return {"ok": True, "deleted": int(deleted or 0)}
    finally:
        conn.close()


# -------------------------
# API: Raw report
# -------------------------
@app.get("/api/spy-reports/{report_id}/raw", response_class=PlainTextResponse)
def get_spy_report_raw(request: Request, report_id: int):
    scope = _get_scope_from_request(request)
    scoped_alliances = (scope or {}).get("alliance_names") or []
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if scoped_alliances:
                cur.execute(
                    """
                    SELECT raw, raw_gz
                    FROM public.spy_reports
                    WHERE id = %s
                      AND COALESCE(alliance,'') = ANY(%s)
                    """,
                    (report_id, scoped_alliances),
                )
            else:
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
def get_spy_report(request: Request, report_id: int):
    scope = _get_scope_from_request(request)
    scoped_alliances = (scope or {}).get("alliance_names") or []
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if scoped_alliances:
                cur.execute(
                    """
                    SELECT id, created_at, kingdom, alliance, defense_power, castles, raw, raw_gz
                    FROM public.spy_reports
                    WHERE id = %s
                      AND COALESCE(alliance,'') = ANY(%s)
                    """,
                    (report_id, scoped_alliances),
                )
            else:
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


def _sync_settlement_observations_from_spy_reports(from_id: int, limit: int) -> Dict[str, int]:
    start_id = max(0, int(from_id))
    lim = max(1, min(int(limit), 1_000_000))

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
                        event_detail=None,
                    )
                    if inserted:
                        inserted_events += 1

        conn.commit()
        return {
            "scanned": scanned,
            "reports_with_settlements": reports_with_settlements,
            "inserted_events": inserted_events,
            "last_id": last_id,
        }
    finally:
        conn.close()


def _sync_settlement_observations_from_attack_reports(from_id: int, limit: int) -> Dict[str, int]:
    start_id = max(0, int(from_id))
    lim = max(1, min(int(limit), 1_000_000))

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, target_kingdom, raw_text, attack_result
                FROM public.attack_reports
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
                kingdom = str(r.get("target_kingdom") or "").strip()
                if not kingdom:
                    continue

                raw_text = str(r.get("raw_text") or "").strip()
                if not raw_text:
                    continue

                parsed = parse_attack_report(raw_text)
                mentions = parsed.get("settlement_mentions") or []
                if not mentions:
                    continue

                reports_with_settlements += 1
                for s in mentions:
                    inserted = _insert_settlement_observation(
                        cur,
                        source_type="attack",
                        source_report_id=rid,
                        kingdom=kingdom,
                        settlement_name=str(s.get("settlement_name") or "").strip(),
                        settlement_level=s.get("settlement_level"),
                        settlement_tier=s.get("settlement_tier"),
                        event_type=str(parsed.get("settlement_event_type") or "seen"),
                        event_detail=parsed.get("settlement_event_detail"),
                    )
                    if inserted:
                        inserted_events += 1

        conn.commit()
        return {
            "scanned": scanned,
            "reports_with_settlements": reports_with_settlements,
            "inserted_events": inserted_events,
            "last_id": last_id,
        }
    finally:
        conn.close()


def _research_row_to_api(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row.get("id") or 0),
        "name": row.get("name") or "",
        "affects": row.get("affects") or "all",
        "ratePerLevel": float(row.get("rate_per_level") or 0),
        "level": int(row.get("level") or 0),
        "apUp": bool(row.get("ap_up")),
        "dpUp": bool(row.get("dp_up")),
        "speedUp": bool(row.get("speed_up")),
        "casualtyReduction": bool(row.get("casualty_reduction")),
        "notes": row.get("notes") or "",
        "updatedAt": (row.get("updated_at").isoformat() if row.get("updated_at") else None),
    }


@app.get("/api/profile/research")
def list_my_research(request: Request):
    uid = _require_user_id(request)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, affects, rate_per_level, level, ap_up, dp_up, speed_up, casualty_reduction, notes, updated_at
                FROM public.user_attack_research
                WHERE discord_user_id = %s
                ORDER BY updated_at DESC, id DESC
                """,
                (uid,),
            )
            rows = cur.fetchall()
        return {"ok": True, "items": [_research_row_to_api(r) for r in rows]}
    finally:
        conn.close()


@app.post("/api/profile/research")
def upsert_my_research(request: Request, body: UserResearchBody):
    uid = _require_user_id(request)
    name = str(body.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.user_attack_research
                  (discord_user_id, name, affects, rate_per_level, level, ap_up, dp_up, speed_up, casualty_reduction, notes, created_at, updated_at)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (discord_user_id, name)
                DO UPDATE SET
                  affects = EXCLUDED.affects,
                  rate_per_level = EXCLUDED.rate_per_level,
                  level = EXCLUDED.level,
                  ap_up = EXCLUDED.ap_up,
                  dp_up = EXCLUDED.dp_up,
                  speed_up = EXCLUDED.speed_up,
                  casualty_reduction = EXCLUDED.casualty_reduction,
                  notes = EXCLUDED.notes,
                  updated_at = now()
                RETURNING id, name, affects, rate_per_level, level, ap_up, dp_up, speed_up, casualty_reduction, notes, updated_at
                """,
                (
                    uid,
                    name,
                    str(body.affects or "all").strip().lower() or "all",
                    float(body.ratePerLevel or 0),
                    int(body.level or 0),
                    bool(body.apUp),
                    bool(body.dpUp),
                    bool(body.speedUp),
                    bool(body.casualtyReduction),
                    (str(body.notes or "").strip() or None),
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return {"ok": True, "item": _research_row_to_api(row or {})}
    finally:
        conn.close()


@app.delete("/api/profile/research/{item_id}")
def delete_my_research(request: Request, item_id: int):
    uid = _require_user_id(request)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM public.user_attack_research WHERE id = %s AND discord_user_id = %s",
                (int(item_id), uid),
            )
            deleted = cur.rowcount
        conn.commit()
        return {"ok": True, "deleted": int(deleted or 0)}
    finally:
        conn.close()


def _sync_auto_known_hits_from_attack_reports(from_id: int, limit: int) -> Dict[str, int]:
    start_id = max(0, int(from_id))
    lim = max(1, min(int(limit), 1_000_000))
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, created_at, raw_text
                FROM public.attack_reports
                WHERE id > %s
                ORDER BY id ASC
                LIMIT %s
                """,
                (start_id, lim),
            )
            rows = cur.fetchall()

            scanned = 0
            inserted_known_hits = 0
            last_id = start_id
            for r in rows:
                scanned += 1
                rid = int(r.get("id") or 0)
                last_id = max(last_id, rid)
                raw = str(r.get("raw_text") or "").strip()
                if not raw:
                    continue
                parsed = parse_attack_report(raw)
                try:
                    ins = _auto_insert_known_hit_for_attack(
                        cur,
                        rid,
                        r.get("created_at") or datetime.utcnow(),
                        parsed,
                    )
                except Exception:
                    ins = False
                if ins:
                    inserted_known_hits += 1

        conn.commit()
        return {
            "scanned": scanned,
            "inserted_known_hits": inserted_known_hits,
            "last_id": last_id,
        }
    finally:
        conn.close()


def _initial_settlement_observer_last_id() -> int:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(source_report_id), 0) AS max_id
                FROM public.settlement_observations
                WHERE source_type = 'spy' AND source_report_id IS NOT NULL
                """
            )
            row = cur.fetchone() or {}
            max_obs = int(row.get("max_id") or 0)
            if max_obs > 0:
                return max_obs

            # No prior observation state: start from current head of spy_reports.
            cur.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM public.spy_reports")
            row2 = cur.fetchone() or {}
            return int(row2.get("max_id") or 0)
    finally:
        conn.close()


_settlement_observer_started = False


def start_settlement_observer():
    global _settlement_observer_started
    if _settlement_observer_started:
        return
    _settlement_observer_started = True

    poll_seconds = max(3, int(os.getenv("SETTLEMENT_OBS_POLL_SECONDS", "15")))
    batch_size = max(100, min(int(os.getenv("SETTLEMENT_OBS_BATCH_SIZE", "2000")), 100_000))
    state = {"last_id": _initial_settlement_observer_last_id()}

    def _loop():
        while True:
            try:
                r = _sync_settlement_observations_from_spy_reports(state["last_id"], batch_size)
                state["last_id"] = max(state["last_id"], int(r.get("last_id") or state["last_id"]))
            except Exception as e:
                print(f"[settlement-observer] error: {repr(e)}")
            time.sleep(poll_seconds)

    t = threading.Thread(target=_loop, daemon=True, name="settlement-observer")
    t.start()


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

                auto_known_hit_inserted = False
                if stored:
                    auto_known_hit_inserted = _auto_insert_known_hit_for_attack(
                        cur,
                        int(stored["id"]),
                        stored["created_at"],
                        parsed,
                    )

                conn.commit()
                return {
                    "ok": True,
                    "report_type": "attack",
                    "stored": stored,
                    "parsed": parsed,
                    "settlement_events": events,
                    "auto_known_hit_inserted": bool(auto_known_hit_inserted),
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
def tracked_settlements(request: Request, kingdom: str = "", limit: int = 500):
    s = kingdom.strip()
    lim = max(1, min(int(limit), 1000))
    scope = _get_scope_from_request(request)
    scoped_alliances = (scope or {}).get("alliance_names") or []

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if s and scoped_alliances:
                cur.execute(
                    """
                    SELECT
                        o.kingdom,
                        o.settlement_name,
                        MAX(o.settlement_level) AS latest_level,
                        MAX(o.created_at) AS last_seen_at,
                        COUNT(*)::int AS sightings,
                        COUNT(*) FILTER (WHERE o.event_type = 'take_attempt_failed')::int AS failed_take_attempts,
                        COUNT(*) FILTER (WHERE o.event_type = 'captured')::int AS captures
                    FROM public.settlement_observations o
                    WHERE o.kingdom ILIKE %s
                      AND EXISTS (
                        SELECT 1
                        FROM public.spy_reports s
                        WHERE s.kingdom = o.kingdom
                          AND COALESCE(s.alliance,'') = ANY(%s)
                      )
                    GROUP BY o.kingdom, o.settlement_name
                    ORDER BY last_seen_at DESC
                    LIMIT %s
                    """,
                    (f"%{s}%", scoped_alliances, lim),
                )
            elif s:
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
            elif scoped_alliances:
                cur.execute(
                    """
                    SELECT
                        o.kingdom,
                        o.settlement_name,
                        MAX(o.settlement_level) AS latest_level,
                        MAX(o.created_at) AS last_seen_at,
                        COUNT(*)::int AS sightings,
                        COUNT(*) FILTER (WHERE o.event_type = 'take_attempt_failed')::int AS failed_take_attempts,
                        COUNT(*) FILTER (WHERE o.event_type = 'captured')::int AS captures
                    FROM public.settlement_observations o
                    WHERE EXISTS (
                        SELECT 1
                        FROM public.spy_reports s
                        WHERE s.kingdom = o.kingdom
                          AND COALESCE(s.alliance,'') = ANY(%s)
                    )
                    GROUP BY o.kingdom, o.settlement_name
                    ORDER BY last_seen_at DESC
                    LIMIT %s
                    """,
                    (scoped_alliances, lim),
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
    include_attack_reports: bool = True,
):
    expected = (os.getenv("SETTLEMENT_BACKFILL_TOKEN", "") or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="SETTLEMENT_BACKFILL_TOKEN is not set")
    if token != expected:
        raise HTTPException(status_code=403, detail="Invalid backfill token")

    lim = max(1, min(int(limit), 1_000_000))
    start_id = max(0, int(from_id))

    r = _sync_settlement_observations_from_spy_reports(start_id, lim)
    scanned = int(r.get("scanned") or 0)
    attack = {"scanned": 0, "reports_with_settlements": 0, "inserted_events": 0, "last_id": 0}
    if include_attack_reports:
        attack = _sync_settlement_observations_from_attack_reports(start_id, lim)

    return {
        "ok": True,
        "scanned_spy_reports": scanned,
        "reports_with_settlements": int(r.get("reports_with_settlements") or 0),
        "inserted_settlement_events": int(r.get("inserted_events") or 0),
        "next_from_id": int(r.get("last_id") or start_id),
        "done": scanned < lim,
        "scanned_attack_reports": int(attack.get("scanned") or 0),
        "attack_reports_with_settlements": int(attack.get("reports_with_settlements") or 0),
        "inserted_attack_settlement_events": int(attack.get("inserted_events") or 0),
        "next_attack_from_id": int(attack.get("last_id") or 0),
    }


@app.post("/api/calc/known-hits/backfill-auto")
def backfill_auto_known_hits(
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
    r = _sync_auto_known_hits_from_attack_reports(start_id, lim)
    scanned = int(r.get("scanned") or 0)
    return {
        "ok": True,
        "scanned_attack_reports": scanned,
        "inserted_known_hits": int(r.get("inserted_known_hits") or 0),
        "next_from_id": int(r.get("last_id") or start_id),
        "done": scanned < lim,
    }


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
    seed_default_alliances()
    start_rankings_poller(poll_seconds=rankings_seconds, world_id=world_id)
    start_nw_poller(poll_seconds=nw_seconds)
    start_settlement_observer()


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
