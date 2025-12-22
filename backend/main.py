# backend/main.py
import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
from pydantic import BaseModel
from pathlib import Path
import os
import time
# from sqlalchemy import create_engine

# Import our database and utility functions
from backend import database
from backend import pandas_utils

# Define paths
BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR.parent / "frontend"
CSV_DB_PATH = database.CSV_DB_PATH

# Full df (of last n days) to avoid generating multiple times
saved_df = pd.DataFrame()
df_created_time = time.time()

# Get HTML directory
try:
    HTML_DIR = os.environ["HTML_DIR"]
except Exception as e:
    print(f"Error getting variable from the environment: {e}.")
    exit(1)

# --- Pydantic Models for Request Body ---
class StatusUpdate(BaseModel):
    status: str

# --- Startup Event ---
# @app.on_event("startup")
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    This function runs when the server starts.
    It initializes the database and syncs it with the CSV.
    """
    database.init_db()
    database.sync_db_with_csv()
    yield

# --- FastAPI App Initialization ---
app = FastAPI(lifespan=lifespan)

# --- API Endpoints ---
@app.get("/api/jobs")
def get_jobs(request: Request):
    """
    API endpoint to get jobs.
    Allows filtering by status and a search query 'q'.
    """
    global saved_df
    global df_created_time

    if saved_df.empty or request.query_params.get("refcache") == "true" or (time.time() - df_created_time > 1 * 60 * 60): # 1 hour
        print("Creating new df by fetching data from DB")
        if not CSV_DB_PATH.exists():
            raise HTTPException(status_code=404, detail=f"{CSV_DB_PATH.name} not found")
        database.sync_db_with_csv()
        # df = pd.read_csv(CSV_DB_PATH)
        saved_df = database.get_df_with_mod_time_remove_deleted(CSV_DB_PATH)
        saved_df = database.get_sorted_df_of_last_n_days(saved_df)
        saved_df = saved_df.fillna('N/A')
        # Get statuses from our Postgres DB and merge them into the dataframe
        statuses = database.get_job_statuses()
        if statuses:
            saved_df['status'] = saved_df['Filename'].map(statuses).fillna('new')
        else:
            raise HTTPException(status_code=404, detail=f"Unable to get statuses from database. Check connection to database.")
        df_created_time = time.time()
    else:
        print("Reusing previously saved df")
    df = saved_df

    df = pandas_utils.apply_filters_from_params(df, request)

    q = request.query_params.get("q")
    if q:
        # Simple search across a few key columns
        search_mask = (
            df['Job title'].str.contains(q, case=False, na=False) |
            df['Company name'].str.contains(q, case=False, na=False) |
            df['Required technical skills'].str.contains(q, case=False, na=False)
        )
        df = df[search_mask]

    # Sort the df by `last_mod_time` column (previously converted to pd.Timestamp), latest entry first
    df = df.sort_values(by=['dt_last_mod_time'], ascending=False, ignore_index=True)

    # Convert DataFrame to a list of dictionaries for JSON response
    return df.to_dict('records')

@app.put("/api/jobs/{filename}/status")
def update_status(filename: str, status_update: StatusUpdate):
    """API endpoint to update a job's status."""
    global saved_df
    database.update_job_status(filename, status_update.status)
    saved_df = pd.DataFrame()
    return {"message": f"Status of {filename} updated to {status_update.status}"}

# --- Static File Serving ---
# This serves the saved HTML job descriptions
app.mount("/jobs", StaticFiles(directory=HTML_DIR), name="jobs")

# This serves the main frontend (index.html, script.js, etc.)
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")

# A fallback to ensure index.html is served for any path not caught above
@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str):
    return FileResponse(FRONTEND_DIR / "index.html")
