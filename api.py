"""
GEI PISA Pipeline - FastAPI
Exposes 5 endpoints for the Forage dashboard.
Queries Snowflake analytics views and returns JSON in the expected format.
Persistent connection + caching to keep response times under 1 second.
"""

import os
import time
import logging

import snowflake.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("gei.api")

# ── Snowflake config ───────────────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "PISA")
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

CACHE_TTL = 2  # seconds between Snowflake queries

# ── Persistent Snowflake connection ───────────────────────────────────────────
_sf_conn = None

def get_snowflake_connection():
    global _sf_conn
    if _sf_conn is None:
        log.info("Opening Snowflake connection...")
        _sf_conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            role=SNOWFLAKE_ROLE,
        )
        log.info("Snowflake connected")
    return _sf_conn

# ── Cache ──────────────────────────────────────────────────────────────────────
_cache: dict = {}

def cached_query(key: str, sql: str) -> list[dict]:
    now = time.time()
    if key in _cache and (now - _cache[key]["ts"]) < CACHE_TTL:
        return _cache[key]["data"]
    result = query(sql)
    _cache[key] = {"data": result, "ts": now}
    return result

def query(sql: str) -> list[dict]:
    global _sf_conn
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [col[0].lower() for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Query failed: {e}")
        _sf_conn = None  # force reconnect on next request
        raise HTTPException(status_code=500, detail=str(e))

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="GEI PISA API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}

# ── Chart A: Number of submissions ────────────────────────────────────────────
# Expected: { "count": 134000 }
@app.get("/submissions/count")
def submissions_count():
    rows = cached_query(
        "submissions_count",
        "SELECT total_submissions FROM PISA.ANALYTICS.SUBMISSIONS_COUNT"
    )
    total = sum(row["total_submissions"] for row in rows)
    return {"count": total}

# ── Chart B: Submissions over time ────────────────────────────────────────────
# Expected:
# { "datasets": [{ "id": "Submissions", "data": [{ "x": "12:00", "y": 82 }] }] }
@app.get("/submissions/over-time")
def submissions_over_time():
    rows = cached_query(
        "submissions_over_time",
        """
        SELECT hour, submissions
        FROM PISA.ANALYTICS.SUBMISSIONS_OVER_TIME
        ORDER BY hour
        """
    )
    data = [
        {
            "x": str(row["hour"])[:16][11:16],
            "y": row["submissions"]
        }
        for row in rows
    ]
    return {
        "datasets": [
            {
                "id": "Submissions",
                "data": data
            }
        ]
    }

# ── Chart C: Learning hours per week ──────────────────────────────────────────
# Expected:
# { "datasets": [{ "country": "GBR", "hours": 1640 }] }
@app.get("/learning-hours")
def learning_hours():
    rows = cached_query(
        "learning_hours",
        """
        SELECT country, avg_learning_hours
        FROM PISA.ANALYTICS.LEARNING_HOURS
        WHERE avg_learning_hours IS NOT NULL
        """
    )
    return {
        "datasets": [
            {
                "country": row["country"],
                "hours": int(row["avg_learning_hours"])
            }
            for row in rows
        ]
    }

# ── Chart D: ESCS score ───────────────────────────────────────────────────────
# Expected:
# { "datasets": [{ "id": "GBR", "value": 0.45 }] }
@app.get("/escs")
def escs():
    rows = cached_query(
        "escs",
        """
        SELECT country, avg_escs_score
        FROM PISA.ANALYTICS.ESCS
        WHERE avg_escs_score IS NOT NULL
        """
    )
    return {
        "datasets": [
            {
                "id": row["country"],
                "value": round(float(row["avg_escs_score"]), 4)
            }
            for row in rows
        ]
    }

# ── Chart E: Early education and belonging ────────────────────────────────────
# Expected:
# { "datasets": [{ "id": "GBR", "data": [{ "x": 6, "y": 1.1, "submissions": 412 }] }] }
@app.get("/early-ed-belonging")
def early_ed_belonging():
    rows = cached_query(
        "early_ed_belonging",
        """
        SELECT country, avg_years_pre_school, avg_belong, total_submissions
        FROM PISA.ANALYTICS.EARLY_ED
        WHERE avg_years_pre_school IS NOT NULL
          AND avg_belong IS NOT NULL
        """
    )
    return {
        "datasets": [
            {
                "id": row["country"],
                "data": [
                    {
                        "x": int(row["avg_years_pre_school"]),
                        "y": round(float(row["avg_belong"]), 4),
                        "submissions": row["total_submissions"]
                    }
                ]
            }
            for row in rows
        ]
    }