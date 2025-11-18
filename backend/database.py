import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import execute_batch
import os

load_dotenv()

# Get database connection string
try:
    CONN_STRING = os.environ["DATABASE_URL"]
except Exception as e:
    print(f"Error getting variable from the environment: {e}.")
    exit(1)

# Create a connection pool.
# This is created once when the application starts.
# minconn=1: Start with one open connection.
# maxconn=5: Allow up to 5 connections in the pool.
try:
    DB_POOL = pool.SimpleConnectionPool(minconn=1, maxconn=5, dsn=CONN_STRING)
    print("Database connection pool created successfully.")
except Exception as e:
    print(f"Error creating database connection pool: {e}")
    exit(1)

# Get CSV database path
try:
    CSV_DB_PATH = Path(os.environ["CSV_DB_PATH"])
except Exception as e:
    print(f"Error getting variable from the environment: {e}.")
    exit(1)

TABLE_NAME = "job_statuses"

def init_db():
    """Initializes the database and table if they don't exist."""
    try:
        with DB_POOL.getconn() as conn:
            print("init_db: Connection with database established.")

            # Open a cursor to perform database operations
            with conn.cursor() as cursor:
                # Create a table to store data
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    filename TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'new'
                )
                """)
                print("Finished creating table (if it didn't exist).")

                # Commit the changes to the database
                conn.commit()

    except Exception as e:
        print(f"Error connecting to database: {e}.")

def sync_db_with_csv():
    """Ensures every job in the CSV has an entry in the status database."""
    if not CSV_DB_PATH.exists():
        print(f"Warning: {CSV_DB_PATH} not found. Cannot sync database.")
        return

    df = pd.read_csv(CSV_DB_PATH)
    filenames = df['Filename'].unique()

    try:
        with DB_POOL.getconn() as conn:
            print("sync_db_with_csv: Connection with database established.")

            # Open a cursor to perform database operations
            with conn.cursor() as cursor:
                # Find which filenames are not yet in the database
                cursor.execute(f"SELECT filename FROM {TABLE_NAME}")
                existing_files = {row[0] for row in cursor.fetchall()}
                print("Filenames fetched from DB. Determining new files")
                new_files = [fname for fname in filenames if fname not in existing_files]
                print(f"New files determined. There are {len(new_files)} new files")

                if new_files:
                    # Insert new files with the default 'new' status
                    print("Creating list of tuples to insert")
                    insert_data = [(fname,) for fname in new_files]
                    # insert_data = insert_data[17000:] # ! for testing
                    # print(f"After slicing: There are {len(insert_data)} new files") # ! for testing
                    print("List of tuples created. Running execute_batch")
                    execute_batch(cursor, f"INSERT INTO {TABLE_NAME} (filename) VALUES (%s)", insert_data)
                    conn.commit()
                    print(f"Synced {len(new_files)} new jobs to the database.")

    except Exception as e:
        print(f"Error connecting to database: {e}.")

def get_job_statuses() -> dict:
    """Fetches all job statuses from the DB as a dictionary."""
    try:
        with DB_POOL.getconn() as conn:
            print("get_job_statuses: Connection with database established.")

            # Open a cursor to perform database operations
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT filename, status FROM {TABLE_NAME}")
                statuses = {row[0]: row[1] for row in cursor.fetchall()}
                return statuses

    except Exception as e:
        print(f"Error connecting to database: {e}.")

def update_job_status(filename: str, status: str):
    """Updates the status of a specific job."""
    try:
        with DB_POOL.getconn() as conn:
            print("update_job_status: Connection with database established.")

            # Open a cursor to perform database operations
            with conn.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {TABLE_NAME} SET status = %s WHERE filename = %s", # ? should be %s 
                    (status, filename)
                )
                conn.commit()
                print(f"Updated {filename} to status '{status}'")

    except Exception as e:
        print(f"Error connecting to database: {e}.")
