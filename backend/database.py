import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import execute_batch
import os
from datetime import datetime, timezone
import sqlalchemy
# import time

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

# Get HTML directory
try:
    HTML_DIR = Path(os.environ["HTML_DIR"])
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
                    status TEXT NOT NULL DEFAULT 'new',
                    last_mod_time TIMESTAMP
                )
                """) # TODO: drop last_mod_time column
                print("Finished creating table (if it didn't exist).")

                # Commit the changes to the database
                conn.commit()

    except Exception as e:
        print(f"Error initializing database: {e}.")

def get_last_mod_time(fname: str):
    last_mod_time = None
    filepath: Path = HTML_DIR / fname
    if filepath.exists():
        last_mod_time = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc).isoformat()

    return last_mod_time

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

                new_files = set()
                # deleted_files = set()
                for fname in filenames:
                    # if not HTML_DIR.joinpath(fname).exists():
                    #     deleted_files.add(fname)
                    if fname not in existing_files:
                        new_files.add(fname)
                    # elif fname in existing_files and not HTML_DIR.joinpath(fname).exists():
                    #     # Filename is in existing_files but has been deleted from HTML dir
                    #     deleted_files.add(fname)
                print(f"New files determined. There are {len(new_files)} new files")

                if new_files:
                    # Insert new files with the default 'new' status
                    print("Creating list of tuples to insert")
                    insert_data = [(fname, get_last_mod_time(fname)) for fname in new_files]
                    # insert_data = insert_data[17000:] # ! for testing
                    # print(f"After slicing: There are {len(insert_data)} new files") # ! for testing
                    print("List of tuples created. Running execute_batch")
                    execute_batch(cursor, f"INSERT INTO {TABLE_NAME} (filename, last_mod_time) VALUES (%s, %s)", insert_data)
                    print(f"Added {len(new_files)} new jobs to the database.")
                    conn.commit()

                # if deleted_files:
                #     print("Creating list of tuples to remove deleted files")
                #     remove_data = [(fname,) for fname in deleted_files]
                #     print("List of tuples created. Running execute_batch")
                #     execute_batch(cursor, f"DELETE FROM {TABLE_NAME} WHERE filename = %s", remove_data)
                #     print(f"Deleted {len(deleted_files)} jobs from the database.")
                #     conn.commit()

                # conn.commit()

    except Exception as e:
        print(f"Error syncing DB with CSV: {e}.")

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
        print(f"Error getting job statuses: {e}.")

def update_job_status(filename: str, status: str):
    """Updates the status of a specific job."""
    try:
        with DB_POOL.getconn() as conn:
            print("update_job_status: Connection with database established.")

            # Open a cursor to perform database operations
            with conn.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {TABLE_NAME} SET status = %s WHERE filename = %s",
                    (status, filename)
                )
                conn.commit()
                print(f"Updated {filename} to status '{status}'")

    except Exception as e:
        print(f"Error updating job status: {e}.")

def iso_date_to_days_since_last_mod(iso_date: str) -> int:
    delta_since_date = datetime.now(tz=timezone.utc) - datetime.fromisoformat(iso_date)
    return delta_since_date.total_seconds() / 60  / 60 / 24

def get_sorted_df_of_last_n_days(input_df: pd.DataFrame, days: float = 7):
    """
    Returns a new df after removing all rows that are more than `days` days ago from now.
    Sorts the resulting df by `last_mod_time` column, latest entry first.
    """
    input_df['dt_last_mod_time'] = pd.to_datetime(input_df['last_mod_time'], errors='coerce')

    input_df.dropna(subset=['dt_last_mod_time'], inplace=True) # ensure no NaT in the entire column

    output_df: pd.DataFrame = input_df[pd.Timestamp(datetime.now(tz=timezone.utc)) - input_df['dt_last_mod_time'] <= pd.Timedelta(days=days)]

    # input_df['days_since_last_mod'] = input_df['last_mod_time'].apply(iso_date_to_days_since_last_mod)

    # output_df = input_df[input_df['days_since_last_mod'] <= 7]

    output_df = output_df.sort_values(by=['dt_last_mod_time'], ascending=False, ignore_index=True)

    return output_df

def get_df_with_mod_time_remove_deleted(input_csv=CSV_DB_PATH):
    df = pd.read_csv(input_csv)
    df['last_mod_time'] = df['Filename'].apply(get_last_mod_time)
    # get_last_mod_time returns None for non-existent files
    # df['last_mod_time'] = pd.to_datetime(df['last_mod_time'], errors='coerce')
    # print(f"df len before dropping NaT: {len(df.index)}")
    df.dropna(subset=['last_mod_time'], inplace=True) # remove non-existing files
    # print(f"df len after dropping NaT: {len(df.index)}")
    return df

# def get_sorted_db_as_df():
#     df = pd.DataFrame({})
#     try:
#         # A pool can also be created but default should be fine: https://docs.sqlalchemy.org/en/20/core/pooling.html
#         engine = sqlalchemy.create_engine(CONN_STRING)
#         print("get_sorted_db_as_df: SQLAlchemy engine created.")

#         query = f"SELECT * FROM {TABLE_NAME} ORDER BY last_mod_time DESC"
#         df = pd.read_sql_query(query, con=engine, parse_dates=['last_mod_time'])
#         print("df created from database:")
#         print(df.info())

#         # # Open a cursor to perform database operations
#         # with conn.cursor() as cursor:
#         #     cursor.execute(
#         #         f"UPDATE {TABLE_NAME} SET status = %s WHERE filename = %s",
#         #         (status, filename)
#         #     )
#         #     conn.commit()
#         #     print(f"Updated {filename} to status '{status}'")

#     except Exception as e:
#         print(f"Error getting sorted DB as DataFrame: {e}.")

#     return df
