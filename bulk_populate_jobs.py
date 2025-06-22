import os
import sys
import json
import time
import logging
from sqlalchemy import text
from typing import Dict, List, Set, Tuple, Optional

# --- Setup Project Path ---
# This allows the script to be run from the root directory and find other modules
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Attempt to import necessary application modules ---
try:
    import database
    import bls_job_mapper
    from soc_codes import TARGET_SOC_CODES
    from bls_job_mapper import SOC_TO_CATEGORY_STATIC
    MODULE_IMPORT_SUCCESS = True
except ImportError as e:
    print(f"CRITICAL ERROR: Failed to import a required module (database, bls_job_mapper, or soc_codes). Ensure this script is in the root project directory. Details: {e}")
    MODULE_IMPORT_SUCCESS = False
    sys.exit(1)

# --- Configuration ---
PROGRESS_FILE = "population_progress.json"
LOG_FILE = "bulk_population.log"
API_DELAY_SECONDS = 1.2
MAX_RETRIES = 3

# --- Logger Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),
        logging.StreamHandler()
    ]
)
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

def print_stat(label, value, color=Colors.OKGREEN):
    print(f"{Colors.OKCYAN}{label:<35}{Colors.ENDC} {color}{value}{Colors.ENDC}")

def print_error(message):
    print(f"{Colors.FAIL}[ERROR] {message}{Colors.ENDC}")

def print_warning(message):
    print(f"{Colors.WARNING}[WARNING] {message}{Colors.ENDC}")

def print_info(message):
    print(f"{Colors.OKBLUE}[INFO] {message}{Colors.ENDC}")

def print_success(message):
    print(f"{Colors.OKGREEN}[SUCCESS] {message}{Colors.ENDC}")

# --- Helper for Database URL --------------------------------------------------
def get_database_url() -> Optional[str]:
    """
    Return DATABASE_URL.  If not present in environment, prompt the user.
    The value is **not** echoed to the console once entered, to avoid
    accidentally leaking credentials in screen-captures.
    """
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        print_info("Using DATABASE_URL from environment.")
        return db_url

    print_warning("DATABASE_URL environment variable not set.")
    print_info("Please paste your Neon database connection string below.")
    try:
        # Use input() for portability (PowerShell/Windows cmd friendly).
        db_url = input("Enter DATABASE_URL: ").strip()
    except (EOFError, KeyboardInterrupt):
        print_error("Input cancelled. Exiting.")
        return None

    if not db_url:
        print_error("No database URL provided. Exiting.")
        return None

    # Normalise postgres:// → postgresql:// so SQLAlchemy is happy.
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    # Persist for child modules in this process.
    os.environ["DATABASE_URL"] = db_url
    return db_url

# --- Progress Management ---
def load_progress() -> Dict:
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError):
            print_warning(f"Could not read progress file '{PROGRESS_FILE}'. Starting fresh.")
    return {"successfully_processed": [], "failed_socs": {}}

def save_progress(progress: Dict):
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f, indent=4)
    except IOError:
        print_error("Could not save progress to file.")

# --- Core Logic ---
def get_soc_lists_to_process(engine) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """Get the full list of SOCs and the list of missing SOCs."""
    print_info("Fetching master list of SOC codes and checking against database...")
    all_bls_socs = {soc[0]: soc[1] for soc in TARGET_SOC_CODES}
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT occupation_code FROM bls_job_data"))
            db_socs = {row[0] for row in result}
    except Exception as e:
        print_error(f"Could not query database for existing SOCs: {e}")
        return [], []
    
    missing_soc_codes = set(all_bls_socs.keys()) - db_socs
    missing_soc_tuples = sorted([(soc, all_bls_socs[soc]) for soc in missing_soc_codes])
    
    return sorted(TARGET_SOC_CODES), missing_soc_tuples

def process_single_soc(soc_code: str, title: str, engine) -> Tuple[bool, str]:
    """Processes a single SOC, including retries and error handling."""
    for attempt in range(MAX_RETRIES):
        try:
            success, message = bls_job_mapper.fetch_and_process_soc_data(soc_code, title, engine)
            if success:
                return True, message
            else:
                last_error_message = message
        except Exception as e:
            last_error_message = f"Unexpected exception: {str(e)}"
            logger.error(f"Attempt {attempt + 1} for SOC {soc_code} failed with exception.", exc_info=True)
        
        if attempt < MAX_RETRIES - 1:
            wait_time = 2 ** (attempt + 1)
            print_warning(f"  -> Attempt {attempt + 1} failed. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            
    return False, last_error_message

def run_population_run(soc_list: List[Tuple[str, str]], engine, progress: Dict, run_limit: Optional[int] = None):
    """Orchestrates a population run for a given list of SOC codes."""
    jobs_to_process = [job for job in soc_list if job[0] not in progress["successfully_processed"]]
    if run_limit:
        jobs_to_process = jobs_to_process[:run_limit]

    if not jobs_to_process:
        print_success("No new jobs to process in this list.")
        return

    print_header(f"Starting Population Run for {len(jobs_to_process)} Jobs")
    start_time = time.time()
    success_count = 0
    fail_count = 0

    for i, (soc_code, title) in enumerate(jobs_to_process):
        print_info(f"Processing [{i+1}/{len(jobs_to_process)}]: {soc_code} - {title}")
        
        success, message = process_single_soc(soc_code, title, engine)
        
        if success:
            print_success(f"  -> Successfully populated {soc_code}.")
            progress["successfully_processed"].append(soc_code)
            if soc_code in progress["failed_socs"]:
                del progress["failed_socs"][soc_code]
            success_count += 1
        else:
            print_error(f"  -> Failed to populate {soc_code}: {message}")
            progress["failed_socs"][soc_code] = message
            fail_count += 1
        
        save_progress(progress)
        time.sleep(API_DELAY_SECONDS)

    duration = time.time() - start_time
    print_header("Run Summary")
    print_success(f"Successfully populated: {success_count}")
    print_error(f"Failed to populate: {fail_count}")
    print_info(f"Total time taken: {duration:.2f} seconds.")

# --- Interactive Menu ---
def main():
    if not MODULE_IMPORT_SUCCESS:
        return
        
    engine = database.get_db_engine()
    if engine is None:
        print_error("Could not create database engine. Is DATABASE_URL set correctly?")
        return

    progress = load_progress()
    all_socs, missing_socs = get_soc_lists_to_process(engine)

    while True:
        print_header("Bulk Population and Verification Tool")
        print_stat("Total BLS Occupations", len(all_socs))
        print_stat("Occupations in DB", len(all_socs) - len(missing_socs))
        print_stat("Missing Occupations", len(missing_socs), Colors.WARNING if missing_socs else Colors.OKGREEN)
        print_stat("Failed in Last Run", len(progress.get("failed_socs", {})), Colors.FAIL if progress.get("failed_socs") else Colors.OKGREEN)

        print("\n--- Menu ---")
        print("1. Populate ALL Missing Occupations")
        print("2. Populate a Small Batch (20 Jobs)")
        print("3. Retry Previously Failed Occupations")
        print("4. Populate a Specific Job Category")
        print("5. Reset All Progress (use with caution)")
        print("6. Exit")
        
        choice = input("Enter your choice: ")

        if choice == '1':
            run_population_run(missing_socs, engine, progress)
        elif choice == '2':
            run_population_run(missing_socs, engine, progress, run_limit=20)
        elif choice == '3':
            failed_jobs = [(soc, title) for soc, title in all_socs if soc in progress["failed_socs"]]
            if failed_jobs:
                run_population_run(failed_jobs, engine, progress)
            else:
                print_success("No failed jobs to retry.")
        elif choice == '4':
            print("\nAvailable Categories:")
            categories = sorted(list(set(SOC_TO_CATEGORY_STATIC.values())))
            for i, cat in enumerate(categories):
                print(f"{i+1}. {cat}")
            try:
                cat_choice = int(input("Select a category number: ")) - 1
                if 0 <= cat_choice < len(categories):
                    selected_cat = categories[cat_choice]
                    cat_jobs = [job for job in missing_socs if bls_job_mapper.get_job_category(job[0]) == selected_cat]
                    run_population_run(cat_jobs, engine, progress)
                else:
                    print_error("Invalid category number.")
            except ValueError:
                print_error("Please enter a valid number.")
        elif choice == '5':
            confirm = input("Are you sure you want to reset all progress? This cannot be undone. (y/n): ").lower()
            if confirm == 'y':
                progress = {"successfully_processed": [], "failed_socs": {}}
                save_progress(progress)
                print_success("Progress has been reset.")
        elif choice == '6':
            print_info("Exiting.")
            break
        else:
            print_warning("Invalid choice. Please try again.")
        
        # Refresh lists after a run
        all_socs, missing_socs = get_soc_lists_to_process(engine)
        progress = load_progress()

if __name__ == "__main__":
    # Ensure a usable DATABASE_URL is available (prompt user if missing)
    if get_database_url() is None:  # User declined / no URL provided
        print_error("Exiting: no DATABASE_URL provided.")
        sys.exit(1)

    # At this point DATABASE_URL is guaranteed to be set in the environment,
    # and the `database` module can create its engine successfully.
    main()
