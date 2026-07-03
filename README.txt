Recon Hub - Live NW Movement Pipeline

Overview
- Polls KingdomGame rankings continuously.
- Stores current NW snapshot and historical timeseries.
- Serves live NW APIs for charting and top movers.

Environment Variables
- DATABASE_URL or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD
- KG_WORLD_ID: default `1`
- KG_CONTINENT_ID: default `-1`
- RANKINGS_POLL_SECONDS: default `60`
- NW_POLL_SECONDS: default `60`
- ENABLE_RANKINGS_POLLER: default `true`
- ENABLE_NW_POLLER: default `true`

Login-first KG auth (preferred)
- KG_LOGIN_EMAIL: KG account email
- KG_LOGIN_PASSWORD: KG account password
- KG_LOGIN_ACCOUNT_ID: KG accountId for rankings requests
- KG_LOGIN_KINGDOM_ID: KG kingdomId for rankings requests
- KG_USER_LOGIN_URL: default `https://kingdomgame.net/WebService/User.asmx/Login`
- KG_TOKEN_TTL_SECONDS: default `1500`

Static token fallback (used only if login auth is unavailable)
- KG_POLLER_TOKEN
- KG_POLLER_ACCOUNT_ID
- KG_POLLER_KINGDOM_ID
- or KG_POLLER_CREDENTIALS_JSON as a JSON array of objects:
	[{"account_id":123,"kingdom_id":456,"token":"..."}]

Rankings request controls
- KG_RANKINGS_URL: default `https://kingdomgame.net/WebService/Kingdoms.asmx/GetKingdomRankings`
- KG_REQUEST_TIMEOUT_SECONDS: default `30`
- KG_USER_AGENT (optional)
- KG_ACCEPT_LANGUAGE (optional)

API Endpoints
- GET `/api/nw/live?limit=100`
	Returns current top list with rank, networth, delta, updatedAt.
- GET `/api/nw/history?kingdomId=123&range=24h`
	Returns timeseries points for charting.
- GET `/api/nw/movers?window=15m&minDelta=1000`
	Returns biggest gainers and losers in window.
- GET `/api/nw/health`
	Returns diagnostics: login status, rankings status, auth mode, rows pulled,
	last poll, last error, and per-attempt HTTP status/body preview.

Run
1. Install backend dependencies:
	 cd backend && pip install -r requirements.txt
2. Install frontend dependencies:
	 cd frontend && npm install
3. Build frontend (optional for local FastAPI static serving):
	 cd frontend && npm run build
4. Start backend:
	 cd backend && uvicorn main:app --host 0.0.0.0 --port 8000

Tests
- Backend NW pipeline tests:
	cd backend && python -m unittest test_nw_pipeline.py
