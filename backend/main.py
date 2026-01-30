from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

# ---- FIX: define BASE_DIR so Render doesn't crash ----
BASE_DIR = Path(__file__).resolve().parent
CALC_LOG_DB = BASE_DIR / "calc_log.db"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/calc")
def calc_redirect():
    return RedirectResponse(url="/kg-calc.html", status_code=302)

# NOTE:
# This file is a PATCH SAMPLE to demonstrate the BASE_DIR fix.
# Copy ONLY the BASE_DIR definition and CALC_LOG_DB line into your existing main.py
# if you prefer not to overwrite the full file.
