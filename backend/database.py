import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import aiosqlite
import os
from datetime import datetime, timezone
# import sqlalchemy
# import time

load_dotenv()

# Define SQLite database path
SQLITE_DB_PATH = Path(
    os.environ.get(
        "SQLITE_DB_PATH",
        str(Path(__file__).resolve().parent / "local_db" / "filter_and_view_jobs.sqlite")
    )
)

DB_CONN = None

async def create_db_connection():
    global DB_CONN
    try:
        # Ensure the directory exists
        SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        DB_CONN = await aiosqlite.connect(SQLITE_DB_PATH)
        DB_CONN.row_factory = aiosqlite.Row
        print("Database connection created successfully.")
    except Exception as e:
        print(f"Error creating database connection: {e}")
        exit(1)

async def close_db_connection():
    if DB_CONN:
        await DB_CONN.close()
        print("Database connection closed.")

# Get CSV database path
try:
    CSV_DB_PATH = Path(os.environ["CSV_DB_PATH"])
except Exception as e:
    print(f"Error getting variable from the environment: {e}.")
    exit(1)

# Get HTML directory
try:
    HTML_DIR = Path(os.environ["HTML_DIR"])
except Exception as e:
    print(f"Error getting variable from the environment: {e}.")
    exit(1)

TABLE_NAME = "job_statuses"

async def init_db():
    """Initializes the database and table if they don't exist."""
    # Create a table to store data
    await DB_CONN.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        filename TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'new'
    )
    """)
    await DB_CONN.commit()
    print("Finished creating table (if it didn't exist).")

async def drop_column_from_db(column_name: str):
    """Drop (permanently delete!) the provided column from the table"""
    # Create a table to store data
    await DB_CONN.execute(f"""
    ALTER TABLE {TABLE_NAME}
    DROP COLUMN IF EXISTS {column_name};
    """)
    await DB_CONN.commit()
    print(f"Finished dropping column {column_name} (if it didn't exist).")

def get_last_mod_time(fname: str):
    last_mod_time = None
    filepath: Path = HTML_DIR / fname
    if filepath.exists():
        last_mod_time = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc).isoformat()

    return last_mod_time

async def sync_db_with_csv():
    """Ensures every job in the CSV has an entry in the status database."""
    if not CSV_DB_PATH.exists():
        print(f"Warning: {CSV_DB_PATH} not found. Cannot sync database.")
        return

    # Reading CSV is still synchronous as it's a file operation with pandas
    # Ideally should be done in a thread pool if file is huge, but usually fine for startup
    df = pd.read_csv(CSV_DB_PATH, dtype=str)
    filenames = df['Filename'].unique()

    # Find which filenames are not yet in the database
    print(f"Executing: SELECT filename FROM {TABLE_NAME}")
    async with DB_CONN.execute(f"SELECT filename FROM {TABLE_NAME}") as cursor:
        rows = await cursor.fetchall()
    print("Fetching all rows...")
    existing_files = {row['filename'] for row in rows}
    print("Filenames fetched from DB. Determining new files")

    new_files = set()
    for fname in filenames:
        if fname not in existing_files:
            new_files.add(fname)
    print(f"New files determined. There are {len(new_files)} new files")

    if new_files:
        # Insert new files with the default 'new' status
        print("Creating list of tuples to insert")
        insert_data = [(fname,) for fname in new_files]
        print("List of tuples created. Running executemany")
        await DB_CONN.executemany(f"INSERT INTO {TABLE_NAME} (filename) VALUES (?)", insert_data)
        await DB_CONN.commit()
        print(f"Added {len(new_files)} new jobs to the database.")

async def get_job_statuses() -> dict:
    """Fetches all job statuses from the DB as a dictionary."""
    print(f"Executing: SELECT filename, status FROM {TABLE_NAME}")
    async with DB_CONN.execute(f"SELECT filename, status FROM {TABLE_NAME}") as cursor:
        rows = await cursor.fetchall()
    statuses = {row['filename']: row['status'] for row in rows}
    return statuses

async def update_job_status(filename: str, status: str):
    """Updates the status of a specific job."""
    print(f"Executing: UPDATE {TABLE_NAME} SET status = ? WHERE filename = ?")
    await DB_CONN.execute(
        f"UPDATE {TABLE_NAME} SET status = ? WHERE filename = ?",
        (status, filename)
    )
    await DB_CONN.commit()
    print(f"Updated {filename} to status '{status}'")

def iso_date_to_days_since_last_mod(iso_date: str) -> int:
    delta_since_date = datetime.now(tz=timezone.utc) - datetime.fromisoformat(iso_date)
    return delta_since_date.total_seconds() / 60  / 60 / 24

def get_sorted_df_of_last_n_days(input_df: pd.DataFrame, days: float = 7):
    """
    Returns a new df after removing all rows that are more than  days ago from now.
    Sorts the resulting df by  column, latest entry first.
    """
    input_df['dt_last_mod_time'] = pd.to_datetime(input_df['last_mod_time'], errors='coerce')

    input_df.dropna(subset=['dt_last_mod_time'], inplace=True) # ensure no NaT in the entire column

    output_df: pd.DataFrame = input_df[pd.Timestamp(datetime.now(tz=timezone.utc)) - input_df['dt_last_mod_time'] <= pd.Timedelta(days=days)]

    # input_df['days_since_last_mod'] = input_df['last_mod_time'].apply(iso_date_to_days_since_last_mod)

    # output_df = input_df[input_df['days_since_last_mod'] <= 7]

    # TODO: This is not strictly needed since df will be sorted again at the end (after filtering)
    output_df = output_df.sort_values(by=['dt_last_mod_time'], ascending=False, ignore_index=True)

    return output_df

def get_df_with_mod_time_remove_deleted(input_csv=CSV_DB_PATH):
    df = pd.read_csv(input_csv, dtype=str)
    if not 'last_mod_time' in df.columns:
        df['last_mod_time'] = df['Filename'].apply(get_last_mod_time)
    # get_last_mod_time returns None for non-existent files
    # df['last_mod_time'] = pd.to_datetime(df['last_mod_time'], errors='coerce')
    # print(f"df len before dropping NaT: {len(df.index)}")
    df.dropna(subset=['last_mod_time'], inplace=True) # remove non-existing files
    # print(f"df len after dropping NaT: {len(df.index)}")
    return df
