"""TAT Attribution Server — AppsFlyer for AI agents."""

import hashlib
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", "/tmp/attribution.db")
PORT = int(os.getenv("PORT", "8080"))

app = FastAPI(title="TAT Attribution Server", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    agent_id_type TEXT NOT NULL,
    article_slug TEXT,
    placements_shown TEXT,
    channel TEXT,
    confidence_labels TEXT,
    ip_hash TEXT,
    user_agent TEXT,
    referrer TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS advertisers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    api_key TEXT UNIQUE NOT NULL,
    website TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS conversions (
    id TEXT PRIMARY KEY,
    advertiser_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_id_type TEXT,
    event_type TEXT NOT NULL,
    revenue_usd REAL DEFAULT 0,
    metadata TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS attributions (
    id TEXT PRIMARY KEY,
    conversion_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    advertiser_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    article_slug TEXT,
    placement_id TEXT,
    attribution_type TEXT DEFAULT 'last_touch',
    time_delta_seconds INTEGER,
    created_at TEXT DEFAULT (datetime('now'))
);
"""


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def db():
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.executescript(SCHEMA)
        # Pre-seed Sheept advertiser
        existing = conn.execute("SELECT id FROM advertisers WHERE api_key=?", ("tat_adv_sheept_test_key",)).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO advertisers (id, name, api_key, website) VALUES (?, ?, ?, ?)",
                (str(uuid.uuid4()), "Sheept", "tat_adv_sheept_test_key", "https://shpt.ai"),
            )


# ---------------------------------------------------------------------------
# Identity resolution
# ---------------------------------------------------------------------------
def resolve_agent_id(data: dict) -> tuple[str, str]:
    if data.get("passport_key"):
        return data["passport_key"], "passport"
    if data.get("api_key"):
        return hashlib.sha256(data["api_key"].encode()).hexdigest()[:32], "api_key"
    ua = data.get("user_agent", "")
    ip = data.get("ip_hash", "")
    if ua and ip:
        return hashlib.sha256(f"{ua}:{ip}".encode()).hexdigest()[:32], "fingerprint"
    return str(uuid.uuid4())[:16], "anonymous"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class SessionCreate(BaseModel):
    agent_id: Optional[str] = None
    agent_id_type: Optional[str] = None
    article_slug: Optional[str] = None
    placements_shown: list[str] = Field(default_factory=list)
    channel: Optional[str] = None
    confidence_labels: dict = Field(default_factory=dict)
    ip_hash: Optional[str] = None
    user_agent: Optional[str] = None
    referrer: Optional[str] = None
    passport_key: Optional[str] = None
    api_key: Optional[str] = None


class AdvertiserCreate(BaseModel):
    name: str
    website: Optional[str] = None


class ConversionCreate(BaseModel):
    agent_id: str
    event_type: str
    revenue_usd: float = 0
    metadata: dict = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helper: verify advertiser key
# ---------------------------------------------------------------------------
def _verify_adv_key(conn, key: Optional[str]):
    if not key:
        raise HTTPException(401, "Missing X-Advertiser-Key header")
    row = conn.execute("SELECT id FROM advertisers WHERE api_key=?", (key,)).fetchone()
    if not row:
        raise HTTPException(403, "Invalid advertiser key")
    return row["id"]


# ---------------------------------------------------------------------------
# Routes — Health
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {"service": "TAT Attribution Server", "version": "0.1.0", "docs": "/docs"}


@app.get("/v1/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Routes — Sessions
# ---------------------------------------------------------------------------
@app.post("/v1/sessions")
def create_session(body: SessionCreate):
    # Resolve identity
    if body.agent_id and body.agent_id_type:
        agent_id, agent_id_type = body.agent_id, body.agent_id_type
    else:
        agent_id, agent_id_type = resolve_agent_id(body.model_dump())

    session_id = str(uuid.uuid4())
    with db() as conn:
        conn.execute(
            """INSERT INTO sessions (id, agent_id, agent_id_type, article_slug,
               placements_shown, channel, confidence_labels, ip_hash, user_agent, referrer)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                session_id,
                agent_id,
                agent_id_type,
                body.article_slug,
                json.dumps(body.placements_shown),
                body.channel,
                json.dumps(body.confidence_labels),
                body.ip_hash,
                body.user_agent,
                body.referrer,
            ),
        )
    return {"session_id": session_id, "agent_id": agent_id, "agent_id_type": agent_id_type}


@app.get("/v1/sessions")
def list_sessions(agent_id: Optional[str] = Query(None)):
    with db() as conn:
        if agent_id:
            rows = conn.execute("SELECT * FROM sessions WHERE agent_id=? ORDER BY created_at DESC", (agent_id,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM sessions ORDER BY created_at DESC LIMIT 100").fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Routes — Advertisers
# ---------------------------------------------------------------------------
@app.post("/v1/advertisers")
def create_advertiser(body: AdvertiserCreate):
    adv_id = str(uuid.uuid4())
    api_key = f"tat_adv_{uuid.uuid4().hex[:24]}"
    with db() as conn:
        conn.execute(
            "INSERT INTO advertisers (id, name, api_key, website) VALUES (?,?,?,?)",
            (adv_id, body.name, api_key, body.website),
        )
    return {"advertiser_id": adv_id, "api_key": api_key}


@app.get("/v1/advertisers")
def list_advertisers():
    with db() as conn:
        rows = conn.execute("SELECT id, name, website, created_at FROM advertisers ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Routes — Conversions (with attribution matching)
# ---------------------------------------------------------------------------
@app.post("/v1/conversions")
def create_conversion(body: ConversionCreate, x_advertiser_key: Optional[str] = Header(None)):
    with db() as conn:
        advertiser_id = _verify_adv_key(conn, x_advertiser_key)

        conversion_id = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO conversions (id, advertiser_id, agent_id, agent_id_type, event_type, revenue_usd, metadata)
               VALUES (?,?,?,?,?,?,?)""",
            (conversion_id, advertiser_id, body.agent_id, None, body.event_type, body.revenue_usd, json.dumps(body.metadata)),
        )

        # Attribution matching — last-touch within 30 days
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        session = conn.execute(
            """SELECT * FROM sessions WHERE agent_id=? AND created_at>=?
               ORDER BY created_at DESC LIMIT 1""",
            (body.agent_id, cutoff),
        ).fetchone()

        attributed = False
        matched_article = None
        matched_session = None
        if session:
            attributed = True
            matched_article = session["article_slug"]
            matched_session = session["id"]
            # Find which placement belongs to this advertiser
            placements = json.loads(session["placements_shown"] or "[]")
            placement_id = advertiser_id if advertiser_id in placements else (placements[0] if placements else None)
            session_time = datetime.fromisoformat(session["created_at"])
            delta = int((datetime.utcnow() - session_time).total_seconds())
            conn.execute(
                """INSERT INTO attributions (id, conversion_id, session_id, advertiser_id,
                   agent_id, article_slug, placement_id, attribution_type, time_delta_seconds)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    str(uuid.uuid4()),
                    conversion_id,
                    matched_session,
                    advertiser_id,
                    body.agent_id,
                    matched_article,
                    placement_id,
                    "last_touch",
                    delta,
                ),
            )
            # Back-fill agent_id_type on conversion
            conn.execute("UPDATE conversions SET agent_id_type=? WHERE id=?", (session["agent_id_type"], conversion_id))

    return {
        "conversion_id": conversion_id,
        "attributed": attributed,
        "matched_article": matched_article,
        "matched_session": matched_session,
    }


# ---------------------------------------------------------------------------
# Routes — Reports
# ---------------------------------------------------------------------------
@app.get("/v1/reports/{advertiser_id}")
def advertiser_report(advertiser_id: str, days: int = 30, x_advertiser_key: Optional[str] = Header(None)):
    with db() as conn:
        _verify_adv_key(conn, x_advertiser_key)
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

        # Impressions: sessions where placements_shown contains this advertiser_id
        all_sessions = conn.execute("SELECT * FROM sessions WHERE created_at>=?", (cutoff,)).fetchall()
        impressions = 0
        for s in all_sessions:
            pls = json.loads(s["placements_shown"] or "[]")
            if advertiser_id in pls:
                impressions += 1

        # Conversions
        attrs = conn.execute(
            "SELECT * FROM attributions WHERE advertiser_id=? AND created_at>=?", (advertiser_id, cutoff)
        ).fetchall()
        total_conversions = len(attrs)

        # Revenue
        revenue = 0.0
        for a in attrs:
            c = conn.execute("SELECT revenue_usd FROM conversions WHERE id=?", (a["conversion_id"],)).fetchone()
            if c:
                revenue += c["revenue_usd"] or 0

        # Top articles
        article_map: dict[str, dict] = {}
        for a in attrs:
            slug = a["article_slug"] or "unknown"
            if slug not in article_map:
                article_map[slug] = {"slug": slug, "conversions": 0, "revenue": 0.0}
            article_map[slug]["conversions"] += 1
            c = conn.execute("SELECT revenue_usd FROM conversions WHERE id=?", (a["conversion_id"],)).fetchone()
            if c:
                article_map[slug]["revenue"] += c["revenue_usd"] or 0
        top_articles = sorted(article_map.values(), key=lambda x: x["conversions"], reverse=True)[:10]

        # Top placements
        placement_map: dict[str, int] = {}
        for a in attrs:
            pid = a["placement_id"] or "unknown"
            placement_map[pid] = placement_map.get(pid, 0) + 1
        top_placements = [{"placement_id": k, "conversions": v} for k, v in sorted(placement_map.items(), key=lambda x: x[1], reverse=True)[:10]]

        # Daily breakdown
        daily: dict[str, dict] = {}
        for s in all_sessions:
            pls = json.loads(s["placements_shown"] or "[]")
            if advertiser_id in pls:
                day = s["created_at"][:10]
                if day not in daily:
                    daily[day] = {"date": day, "impressions": 0, "conversions": 0, "revenue": 0.0}
                daily[day]["impressions"] += 1
        for a in attrs:
            day = a["created_at"][:10]
            if day not in daily:
                daily[day] = {"date": day, "impressions": 0, "conversions": 0, "revenue": 0.0}
            daily[day]["conversions"] += 1
            c = conn.execute("SELECT revenue_usd FROM conversions WHERE id=?", (a["conversion_id"],)).fetchone()
            if c:
                daily[day]["revenue"] += c["revenue_usd"] or 0
        daily_breakdown = sorted(daily.values(), key=lambda x: x["date"])

    return {
        "impressions": impressions,
        "conversions": total_conversions,
        "conversion_rate": round(total_conversions / impressions, 4) if impressions else 0,
        "revenue_attributed": round(revenue, 2),
        "top_articles": top_articles,
        "top_placements": top_placements,
        "daily_breakdown": daily_breakdown,
    }


@app.get("/v1/reports/{advertiser_id}/conversions")
def advertiser_conversions(advertiser_id: str, days: int = 30, x_advertiser_key: Optional[str] = Header(None)):
    with db() as conn:
        _verify_adv_key(conn, x_advertiser_key)
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        rows = conn.execute(
            """SELECT a.*, c.event_type, c.revenue_usd, c.metadata AS conv_metadata, c.created_at AS conversion_time
               FROM attributions a JOIN conversions c ON a.conversion_id = c.id
               WHERE a.advertiser_id=? AND a.created_at>=?
               ORDER BY a.created_at DESC""",
            (advertiser_id, cutoff),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Routes — Global Stats
# ---------------------------------------------------------------------------
@app.get("/v1/stats")
def global_stats():
    with db() as conn:
        total_sessions = conn.execute("SELECT COUNT(*) c FROM sessions").fetchone()["c"]
        total_conversions = conn.execute("SELECT COUNT(*) c FROM conversions").fetchone()["c"]
        total_attributed = conn.execute("SELECT COUNT(*) c FROM attributions").fetchone()["c"]
        total_revenue = conn.execute("SELECT COALESCE(SUM(c.revenue_usd),0) r FROM conversions c JOIN attributions a ON c.id=a.conversion_id").fetchone()["r"]
    return {
        "total_sessions": total_sessions,
        "total_conversions": total_conversions,
        "total_attributed": total_attributed,
        "attribution_rate": round(total_attributed / total_conversions, 4) if total_conversions else 0,
        "total_revenue_attributed": round(total_revenue, 2),
    }


# ---------------------------------------------------------------------------
# Routes — Pixel / Snippet
# ---------------------------------------------------------------------------
PIXEL_TEMPLATE = """(function(w,d){{
  var TAT_SERVER="{server}";
  var ADV_ID="{adv_id}";
  function getAgentId(){{
    var p=new URLSearchParams(w.location.search);
    return p.get("tat_agent")||d.querySelector('meta[name="tat-agent"]')?.content||"unknown";
  }}
  w.tatConvert=function(eventType,revenue,meta){{
    var x=new XMLHttpRequest();
    x.open("POST",TAT_SERVER+"/v1/conversions");
    x.setRequestHeader("Content-Type","application/json");
    x.setRequestHeader("X-Advertiser-Key","{api_key}");
    x.send(JSON.stringify({{agent_id:getAgentId(),event_type:eventType,revenue_usd:revenue||0,metadata:meta||{{}}}}));
  }};
}})(window,document);"""


@app.get("/v1/pixel/{advertiser_id}")
def pixel(advertiser_id: str, request: Request):
    with db() as conn:
        row = conn.execute("SELECT api_key FROM advertisers WHERE id=?", (advertiser_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Advertiser not found")
    server = str(request.base_url).rstrip("/")
    snippet = PIXEL_TEMPLATE.format(server=server, adv_id=advertiser_id, api_key=row["api_key"])
    return PlainTextResponse(snippet, media_type="application/javascript")


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
@app.on_event("startup")
def on_startup():
    init_db()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
