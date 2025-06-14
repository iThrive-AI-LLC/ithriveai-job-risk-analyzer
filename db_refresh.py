import os
import json
import datetime
import logging
from sqlalchemy import create_engine, text, inspect

# Attempt to import necessary custom modules
# These modules are expected to be in the same directory or Python path
try:
    import job_api_integration_database_only as job_api
    # Attempt to import specific functions and the engine from database.py
    # If database.py fails to initialize its engine, db_engine might be None
    from database import get_recent_searches, get_popular_searches, engine as db_engine
except ImportError as e:
    # Log the import error and define dummy functions to prevent crashes if db_refresh is imported by app_production.py
    # The main application (app_production.py) should ideally handle this more gracefully if db_refresh fails to load.
    logging.basicConfig(level=logging.ERROR) # Basic logging if main logger isn't set up
    logging.error(f"CRITICAL: Failed to import necessary modules for db_refresh: {e}. Defining dummy functions.")
    def update_job_data(job_title: str) -> bool:
        logging.error("db_refresh.update_job_data called but module not fully loaded due to import error.")
        return False
    def perform_database_queries() -> bool:
        logging.error("db_refresh.perform_database_queries called but module not fully loaded due to import error.")
        return False
    def check_and_update_refresh_timestamp() -> bool:
        logging.error("db_refresh.check_and_update_refresh_timestamp called but module not fully loaded due to import error.")
        return False
    # Set job_api and db_engine to None so later checks can see they are not available
    job_api = None
    db_engine = None


# Configure logging for this module
logger = logging.getLogger("db_refresh")
# Check if handlers are already configured to prevent duplicates if module is reloaded
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

REFRESH_TIMESTAMP_FILE = "last_refresh.json"

def update_job_data(job_title: str) -> bool:
    """
    Fetches and implicitly updates data for a specific job title to generate database activity.
    This relies on job_api.get_job_data (from job_api_integration_database_only.py)
    to handle fetching from BLS and saving to the database if the data is stale or missing.
    """
    logger.info(f"Attempting to update data for job title: '{job_title}'")
    if job_api is None or not hasattr(job_api, 'get_job_data'):
        logger.error("job_api_integration_database_only module (job_api) is not available. Cannot update job data.")
        return False
    try:
        job_data = job_api.get_job_data(job_title)
        if job_data and "error" not in job_data:
            logger.info(f"Successfully fetched/updated data for '{job_title}'. Source: {job_data.get('source', 'N/A')}")
            return True
        elif job_data and "error" in job_data:
            logger.warning(f"Failed to update data for '{job_title}': {job_data['error']}")
            return False
        else:
            logger.warning(f"No data or unexpected response returned for '{job_title}' during update attempt.")
            return False
    except Exception as e:
        logger.error(f"Error during update_job_data for '{job_title}': {e}", exc_info=True)
        return False

def perform_database_queries() -> bool:
    """
    Performs a few simple read queries on the database to generate activity.
    Uses functions from database.py if available, or direct queries.
    """
    logger.info("Performing general database queries for activity.")
    queries_performed_count = 0
    
    # Determine which engine to use
    current_engine = None
    if db_engine is not None:
        current_engine = db_engine
        logger.info("Using shared database engine from database.py.")
    else:
        db_url = os.environ.get("DATABASE_URL")
        if db_url:
            try:
                logger.warning("Shared db_engine not available, creating temporary engine for db_refresh.")
                # Ensure postgres URL is correctly formatted for SQLAlchemy
                if db_url.startswith("postgres://"):
                    db_url = db_url.replace("postgres://", "postgresql://", 1)
                current_engine = create_engine(db_url)
            except Exception as e_create:
                logger.error(f"Failed to create temporary engine using DATABASE_URL: {e_create}")
                return False
        else:
            logger.error("DATABASE_URL not set and shared db_engine not available. Cannot perform database queries.")
            return False

    if current_engine is None:
        logger.error("No database engine available to perform queries.")
        return False

    try:
        with current_engine.connect() as connection:
            # Query 1: Simple SELECT 1
            connection.execute(text("SELECT 1"))
            logger.info("Executed query: SELECT 1")
            queries_performed_count += 1

            # Check if job_searches table exists before attempting related queries
            inspector = inspect(current_engine)
            if 'job_searches' in inspector.get_table_names():
                # Query 2: Use get_recent_searches from database.py if available
                if 'get_recent_searches' in globals() and callable(get_recent_searches):
                    get_recent_searches(limit=1)
                    logger.info("Executed function: get_recent_searches(limit=1)")
                    queries_performed_count += 1
                else:
                    logger.warning("get_recent_searches function not available to db_refresh.")

                # Query 3: Use get_popular_searches from database.py if available
                if 'get_popular_searches' in globals() and callable(get_popular_searches):
                    get_popular_searches(limit=1)
                    logger.info("Executed function: get_popular_searches(limit=1)")
                    queries_performed_count += 1
                else:
                    logger.warning("get_popular_searches function not available to db_refresh.")
            else:
                logger.info("'job_searches' table not found in the database. Skipping related queries.")

        if queries_performed_count > 0:
            logger.info(f"Successfully performed {queries_performed_count} database activities.")
            return True
        else:
            logger.warning("No database queries were successfully performed (or functions were unavailable).")
            return False

    except Exception as e:
        logger.error(f"Error during perform_database_queries: {e}", exc_info=True)
        return False

def check_and_update_refresh_timestamp() -> bool:
    """
    Updates the refresh timestamp file (e.g., last_refresh.json) to the current time.
    This function is intended to be called by the main application (app_production.py)
    *after* other refresh activities (like update_job_data, perform_database_queries)
    have been completed.
    """
    logger.info(f"Attempting to update refresh timestamp in file: '{REFRESH_TIMESTAMP_FILE}'")
    try:
        with open(REFRESH_TIMESTAMP_FILE, "w") as f:
            json.dump({"date": datetime.datetime.now().isoformat()}, f)
        logger.info(f"Successfully updated refresh timestamp in '{REFRESH_TIMESTAMP_FILE}'.")
        return True
    except IOError as e:
        logger.error(f"IOError writing to '{REFRESH_TIMESTAMP_FILE}': {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while updating refresh timestamp in '{REFRESH_TIMESTAMP_FILE}': {e}", exc_info=True)
        return False

if __name__ == "__main__":
    # This block allows for direct testing of the db_refresh module.
    # Ensure DATABASE_URL is set in your environment if testing directly.
    # Also, ensure job_api_integration_database_only.py and database.py are accessible.

    # Setup basic logging for standalone testing
    if not logger.handlers: # Re-check in case it's run directly
        _test_handler = logging.StreamHandler()
        _test_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        _test_handler.setFormatter(_test_formatter)
        logger.addHandler(_test_handler)
        logger.setLevel(logging.INFO)

    logger.info("Running db_refresh module direct tests...")

    # Example: Set a DATABASE_URL if it's not already set for testing
    # if "DATABASE_URL" not in os.environ:
    # os.environ["DATABASE_URL"] = "your_test_database_url_here" # Replace with a test DB if needed
    # if "BLS_API_KEY" not in os.environ:
    # os.environ["BLS_API_KEY"] = "your_bls_api_key_here"


    sample_job_to_update = "Software Developer" # A common job title likely in BLS data
    logger.info(f"\n--- Test 1: update_job_data for '{sample_job_to_update}' ---")
    if update_job_data(sample_job_to_update):
        logger.info(f"Test 1 Result: Successfully updated/fetched data for '{sample_job_to_update}'.")
    else:
        logger.warning(f"Test 1 Result: Failed to update/fetch data for '{sample_job_to_update}'. Check logs and DB/API connection.")

    logger.info("\n--- Test 2: perform_database_queries ---")
    if perform_database_queries():
        logger.info("Test 2 Result: Successfully performed database queries.")
    else:
        logger.warning("Test 2 Result: Failed to perform database queries. Check logs and DB connection.")

    logger.info("\n--- Test 3: check_and_update_refresh_timestamp ---")
    if check_and_update_refresh_timestamp():
        logger.info("Test 3 Result: Successfully updated refresh timestamp file.")
        try:
            with open(REFRESH_TIMESTAMP_FILE, "r") as f_read:
                timestamp_data = json.load(f_read)
                logger.info(f"Contents of '{REFRESH_TIMESTAMP_FILE}': {timestamp_data}")
        except Exception as e_read:
            logger.error(f"Could not read back '{REFRESH_TIMESTAMP_FILE}': {e_read}")
    else:
        logger.warning("Test 3 Result: Failed to update refresh timestamp file.")

    logger.info("\ndb_refresh module direct tests complete.")
