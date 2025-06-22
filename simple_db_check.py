import os
import sys
import logging
from sqlalchemy import text

# --- Setup Project Path ---
# This allows the script to be run from the root directory and find other modules
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Attempt to import necessary application modules ---
try:
    import database  # This should give us access to the configured engine
    MODULE_IMPORT_SUCCESS = True
except ImportError as e:
    print(f"CRITICAL ERROR: Failed to import 'database.py'. Ensure this script is in the same directory as your application files. Details: {e}")
    MODULE_IMPORT_SUCCESS = False
    sys.exit(1)

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Main Diagnostic Function ---
def run_simple_db_check():
    """
    Connects to the database and runs simple checks using only SQLAlchemy and standard libraries.
    """
    print("===== Simple Neon Database Check =====")

    engine = database.get_db_engine()
    if engine is None:
        print("\n[ERROR] Could not create database engine. Is the DATABASE_URL environment variable set correctly?")
        return

    try:
        with engine.connect() as conn:
            print("\n[SUCCESS] Database connection successful.")

            # 1. Total count of occupations
            print("\n--- Checking Total Occupation Count ---")
            try:
                total_count_result = conn.execute(text("SELECT COUNT(*) FROM bls_job_data")).scalar_one_or_none()
                print(f"Total Occupations Loaded: {total_count_result or 0}")
            except Exception as e:
                print(f"[ERROR] Could not query total count. The table 'bls_job_data' might not exist. Details: {e}")
                # If the table doesn't exist, no other queries will work.
                return

            # 2. Specifically check for "13-1082"
            print("\n--- Checking for '13-1082: Project Management Specialists' ---")
            soc_to_find = "13-1082"
            exists_result = conn.execute(
                text("SELECT EXISTS(SELECT 1 FROM bls_job_data WHERE occupation_code = :soc)"),
                {"soc": soc_to_find}
            ).scalar_one()

            if exists_result:
                print(f"[FOUND] SOC Code '{soc_to_find}' exists in the database.")
            else:
                print(f"[MISSING] SOC Code '{soc_to_find}' was NOT found in the database.")

            # 3. Basic Statistics
            print("\n--- Basic Database Statistics ---")
            # Count distinct categories
            category_count = conn.execute(text("SELECT COUNT(DISTINCT job_category) FROM bls_job_data")).scalar_one_or_none()
            print(f"Number of Unique Job Categories: {category_count or 0}")

            # Get the most recent entry date
            latest_date = conn.execute(text("SELECT MAX(last_updated) FROM bls_job_data")).scalar_one_or_none()
            print(f"Most Recent Data Entry Date: {latest_date or 'N/A'}")
            
            # Get the oldest entry date
            oldest_date = conn.execute(text("SELECT MIN(last_updated) FROM bls_job_data")).scalar_one_or_none()
            print(f"Oldest Data Entry Date: {oldest_date or 'N/A'}")


    except Exception as e:
        print(f"\n[CRITICAL ERROR] An error occurred while connecting to the database or executing queries: {e}")
        logger.exception("Simple database check script failed.")

if __name__ == "__main__":
    if "DATABASE_URL" not in os.environ:
        print("[ERROR] The DATABASE_URL environment variable is not set.")
        print("Please set it before running this script, e.g., export DATABASE_URL='your_neon_db_connection_string'")
    elif not MODULE_IMPORT_SUCCESS:
        print("[ERROR] Could not run script due to module import failure.")
    else:
        run_simple_db_check()
