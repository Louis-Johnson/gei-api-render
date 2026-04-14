"""
GEI PISA Pipeline - FastAPI
Exposes 5 endpoints for the Forage dashboard.
Queries Snowflake analytics views and returns JSON in the expected format.
"""

import os
import logging
from contextlib import asynccontextmanager

import snowflake.connector
from fastapi import FastAPI, HTTPException
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


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )


def query(sql: str) -> list[dict]:
    """Run a SQL query and return results as a list of dicts."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [col[0].lower() for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="GEI PISA API")


# ── Chart A: Number of submissions ────────────────────────────────────────────
# Expected: { "count": 134000 }
@app.get("/submissions/count")
def submissions_count():
    rows = query("SELECT total_submissions FROM PISA.ANALYTICS.SUBMISSIONS_COUNT")
    total = sum(row["total_submissions"] for row in rows)
    return {"count": total}


# ── Chart B: Submissions over time ────────────────────────────────────────────
# Expected:
# { "datasets": [{ "id": "Submissions", "data": [{ "x": "12:00", "y": 82 }, ...] }] }
@app.get("/submissions/over-time")
def submissions_over_time():
    rows = query("""
        SELECT hour, submissions
        FROM PISA.ANALYTICS.SUBMISSIONS_OVER_TIME
        ORDER BY hour
    """)
    data = [
        {
            "x": row["hour"].strftime("%H:%M"),
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
# { "datasets": [{ "country": "GBR", "hours": 1640 }, ...] }
@app.get("/learning-hours")
def learning_hours():
    rows = query("""
        SELECT country, avg_learning_hours
        FROM PISA.ANALYTICS.LEARNING_HOURS
        WHERE avg_learning_hours IS NOT NULL
    """)
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
# { "datasets": [{ "id": "GBR", "value": 0.45 }, ...] }
@app.get("/escs")
def escs():
    rows = query("""
        SELECT country, avg_escs_score
        FROM PISA.ANALYTICS.ESCS
        WHERE avg_escs_score IS NOT NULL
    """)
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
    rows = query("""
        SELECT country, avg_years_pre_school, avg_belong, total_submissions
        FROM PISA.ANALYTICS.EARLY_ED
        WHERE avg_years_pre_school IS NOT NULL
          AND avg_belong IS NOT NULL
    """)
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


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}