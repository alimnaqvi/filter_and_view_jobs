# backend/main.py
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
from pydantic import BaseModel
from pathlib import Path
import os
from sqlalchemy import create_engine

# Import our database functions
from backend import database

# Define paths
BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR.parent / "frontend"
CSV_DB_PATH = database.CSV_DB_PATH

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
def get_jobs(status: str = None, q: str = None):
    """
    API endpoint to get jobs.
    Allows filtering by status and a search query 'q'.
    """
    print("Request received on /api/jobs")

    if not CSV_DB_PATH.exists():
        raise HTTPException(status_code=404, detail=f"{CSV_DB_PATH.name} not found")

    # df = pd.read_csv(CSV_DB_PATH)
    df = database.get_df_with_mod_time_remove_deleted(CSV_DB_PATH)

    df = database.get_sorted_df_of_last_n_days(df)
    
    # Get statuses from our Postgres DB and merge them into the dataframe
    statuses = database.get_job_statuses()
    df['status'] = df['Filename'].map(statuses).fillna('new')

    # Apply filters
    if status and status != "all":
        df = df[df['status'] == status]

    # Remove filenames that have been deleted and sort by last modification date
    # df['file_exists'] = df['Filename'].apply(lambda x: Path(HTML_DIR).joinpath(x).exists())
    # df = df[df['file_exists'] == True]
    
    # TODO: Add more filters, especially for last_mod_time

    if q:
        # Simple search across a few key columns
        search_mask = (
            df['Job title'].str.contains(q, case=False, na=False) |
            df['Company name'].str.contains(q, case=False, na=False) |
            df['Required technical skills'].str.contains(q, case=False, na=False)
        )
        df = df[search_mask]
    
    df = df.fillna('N/A')

    # Convert DataFrame to a list of dictionaries for JSON response
    return df.to_dict('records')

@app.put("/api/jobs/{filename}/status")
def update_status(filename: str, status_update: StatusUpdate):
    """API endpoint to update a job's status."""
    database.update_job_status(filename, status_update.status)
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

