import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from psycopg2 import pool, InterfaceError
from psycopg2.extras import execute_batch
import os
from datetime import datetime, timezone
# import sqlalchemy
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

def get_conn_and_exec_func(func):
    def wrapper(*args, **kwargs):
        conn = None
        try:
            for i in range(5): # Try 5 times max in case DB connection has been closed from the other side
                conn = DB_POOL.getconn()
                try:
                    with conn:
                        print("get_job_statuses: Connection with database established. Opening cursor.")

                        # Open a cursor to perform database operations
                        with conn.cursor() as cursor:
                            return func(conn, cursor, *args, **kwargs)
                except InterfaceError as e:
                    print(f"Got InterfaceError: {e} (times: {i + 1}).")
                    if conn:
                        print("Discarding the connection from pool and trying another one (max 5 attempts).")
                        DB_POOL.putconn(conn, close=True)
                        conn = None

        except Exception as e:
            print(f"Error getting job statuses: {e}.")
            if conn:
                print("Discarding the connection from pool.")
                DB_POOL.putconn(conn)
                conn = None
        finally:
            if conn:
                print("Putting connection back in pool.")
                DB_POOL.putconn(conn)
    return wrapper

@get_conn_and_exec_func
def init_db(conn, cursor):
    """Initializes the database and table if they don't exist."""
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

@get_conn_and_exec_func
def drop_column_from_db(conn, cursor, column_name: str):
    """Drop (permanently delete!) the provided column from the table"""
    # Create a table to store data
    cursor.execute(f"""
    ALTER TABLE {TABLE_NAME}
    DROP COLUMN IF EXISTS {column_name};
    """)
    print(f"Finished dropping column {column_name} (if it didn't exist).")

    # Commit the changes to the database
    conn.commit()

def get_last_mod_time(fname: str):
    last_mod_time = None
    filepath: Path = HTML_DIR / fname
    if filepath.exists():
        last_mod_time = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc).isoformat()

    return last_mod_time

@get_conn_and_exec_func
def sync_db_with_csv(conn, cursor):
    """Ensures every job in the CSV has an entry in the status database."""
    if not CSV_DB_PATH.exists():
        print(f"Warning: {CSV_DB_PATH} not found. Cannot sync database.")
        return

    df = pd.read_csv(CSV_DB_PATH)
    filenames = df['Filename'].unique()

    # Find which filenames are not yet in the database
    print(f"Cursor opened. Executing: SELECT filename FROM {TABLE_NAME}")
    cursor.execute(f"SELECT filename FROM {TABLE_NAME}")
    print("Fetching all rows...")
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
        insert_data = [(fname,) for fname in new_files]
        print("List of tuples created. Running execute_batch")
        execute_batch(cursor, f"INSERT INTO {TABLE_NAME} (filename) VALUES (%s)", insert_data)
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

@get_conn_and_exec_func
def get_job_statuses(conn, cursor) -> dict:
    """Fetches all job statuses from the DB as a dictionary."""
    print(f"Cursor opened. Executing: SELECT filename, status FROM {TABLE_NAME}")
    cursor.execute(f"SELECT filename, status FROM {TABLE_NAME}")
    statuses = {row[0]: row[1] for row in cursor.fetchall()}
    return statuses

@get_conn_and_exec_func
def update_job_status(conn, cursor, filename: str, status: str):
    """Updates the status of a specific job."""
    print(f"Cursor opened. Executing: UPDATE {TABLE_NAME} SET status = %s WHERE filename = %s")
    cursor.execute(
        f"UPDATE {TABLE_NAME} SET status = %s WHERE filename = %s",
        (status, filename)
    )
    conn.commit()
    print(f"Updated {filename} to status '{status}'")

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
    if not 'last_mod_time' in df.columns:
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
