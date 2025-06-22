import os
import sys
import json
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# --- Setup Project Path ---
# This allows the script to be run from the root directory and find other modules
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Attempt to import necessary application modules ---
try:
    # We need the table definition from bls_job_mapper to create it correctly
    from bls_job_mapper import bls_job_data_table, metadata
    MODULE_IMPORT_SUCCESS = True
except ImportError as e:
    print(f"CRITICAL ERROR: Failed to import from 'bls_job_mapper.py'. This script needs it to define the table schema. Details: {e}")
    MODULE_IMPORT_SUCCESS = False
    sys.exit(1)

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Colored Output Helpers ---
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_header(message):
    print(f"\n{Colors.HEADER}{Colors.BOLD}===== {message} ====={Colors.ENDC}")

def print_error(message):
    print(f"{Colors.FAIL}[ERROR] {message}{Colors.ENDC}")

def print_warning(message):
    print(f"{Colors.WARNING}[WARNING] {message}{Colors.ENDC}")

def print_info(message):
    print(f"{Colors.OKBLUE}[INFO] {message}{Colors.ENDC}")

def print_success(message):
    print(f"{Colors.OKGREEN}[SUCCESS] {message}{Colors.ENDC}")

# --- Core Logic ---
def get_database_url():
    """Retrieves the database URL from environment variables or prompts the user."""
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        print_info("Found DATABASE_URL in environment variables.")
        return db_url
    
    print_warning("DATABASE_URL environment variable not set.")
    print_info("Please paste your Neon database connection string below.")
    db_url = input("Enter DATABASE_URL: ").strip()
    return db_url

def reset_database():
    """Drops and recreates the bls_job_data table and deletes progress files."""
    print_header("Database Reset Utility")
    print_warning("This script will permanently delete all data from the 'bls_job_data' table and reset population progress.")
    
    confirmation = input(f"To confirm, please type 'RESET' and press Enter: ")
    if confirmation != "RESET":
        print_error("Confirmation failed. Aborting reset.")
        return

    db_url = get_database_url()
    if not db_url:
        print_error("No database URL provided. Aborting.")
        return

    try:
        if db_url.startswith('postgres://'):
            db_url = db_url.replace('postgres://', 'postgresql://', 1)
        
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print_info("Successfully connected to the database.")

            # Drop the table
            print_info("Dropping 'bls_job_data' table if it exists...")
            bls_job_data_table.drop(engine, checkfirst=True)
            print_success("'bls_job_data' table dropped.")

            # Re-create the table
            print_info("Re-creating 'bls_job_data' table...")
            metadata.create_all(engine, tables=[bls_job_data_table])
            print_success("'bls_job_data' table created with the correct schema.")

    except OperationalError as e:
        print_error(f"Could not connect to the database. Please check your connection string. Details: {e}")
        return
    except Exception as e:
        print_error(f"An unexpected error occurred during database operations: {e}")
        logger.exception("Database reset failed.")
        return

    # Delete progress files
    print_info("Deleting population progress files...")
    files_to_delete = ["population_progress.json", "simplified_admin_population_progress.json"]
    for f in files_to_delete:
        if os.path.exists(f):
            try:
                os.remove(f)
                print_success(f"Removed progress file: '{f}'")
            except OSError as e:
                print_error(f"Could not remove file '{f}': {e}")
        else:
            print_info(f"Progress file '{f}' not found, skipping.")
            
    print_header("Reset Complete")
    print_success("Your database has been reset. You can now run the population script to start fresh.")

if __name__ == "__main__":
    if not MODULE_IMPORT_SUCCESS:
        print_error("Could not start script due to module import failure.")
    else:
        reset_database()
