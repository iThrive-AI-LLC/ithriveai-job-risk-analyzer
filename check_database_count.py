import os
import sys
import logging
import time
from typing import Dict, Set

import pandas as pd
from sqlalchemy import text

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
    MODULE_IMPORT_SUCCESS = True
except ImportError as e:
    print(f"CRITICAL ERROR: Failed to import a required module (database, bls_job_mapper, or soc_codes). Ensure this script is in the root project directory. Details: {e}")
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

def print_stat(label, value, color=Colors.OKGREEN):
    print(f"{Colors.OKCYAN}{label:<40}{Colors.ENDC} {color}{value}{Colors.ENDC}")

def print_error(message):
    print(f"{Colors.FAIL}[ERROR] {message}{Colors.ENDC}")

def print_warning(message):
    print(f"{Colors.WARNING}[WARNING] {message}{Colors.ENDC}")

def print_info(message):
    print(f"{Colors.OKBLUE}[INFO] {message}{Colors.ENDC}")

# Missing helper for success messages
def print_success(message):
    print(f"{Colors.OKGREEN}[SUCCESS] {message}{Colors.ENDC}")

# --- Core Functions ---

# NOTE: In this standalone script we avoid Streamlit caching for simplicity.
def get_bls_available_soc_codes() -> Dict[str, str]:
    """
    Returns the master list of all available SOC codes from the BLS.
    This uses the comprehensive list generated in `soc_codes.py`.
    """
    print_info("Loading master list of available BLS SOC codes...")
    if not TARGET_SOC_CODES:
        print_error("`TARGET_SOC_CODES` from soc_codes.py is empty. Cannot perform comparison.")
        return {}
    # Convert list of tuples to a dictionary for easy lookup
    return {soc[0]: soc[1] for soc in TARGET_SOC_CODES}

def get_neon_soc_codes(engine) -> Set[str]:
    """Queries the Neon database to get all currently loaded SOC codes."""
    print_info("Querying Neon database for currently loaded SOC codes...")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT occupation_code FROM bls_job_data"))
            return {row[0] for row in result}
    except Exception as e:
        print_error(f"Failed to query Neon database for SOC codes: {e}")
        return set()

def compare_bls_vs_neon(bls_soc_map: Dict[str, str], neon_soc_set: set):
    """Compares the two sets of SOC codes and prints a summary."""
    print_header("BLS vs. Neon Database Comparison")
    
    bls_count = len(bls_soc_map)
    neon_count = len(neon_soc_set)
    missing_codes = set(bls_soc_map.keys()) - neon_soc_set
    missing_count = len(missing_codes)
    
    print_stat("Total SOCs available from BLS:", bls_count)
    print_stat("Total SOCs loaded in Neon DB:", neon_count)
    
    if missing_count > 0:
        print_stat("Missing SOCs to be populated:", missing_count, Colors.WARNING)
    else:
        print_stat("Missing SOCs to be populated:", 0, Colors.OKGREEN)

    if bls_count > 0:
        completion_pct = (neon_count / bls_count) * 100
        print_stat("Database Population Progress:", f"{completion_pct:.2f}%")
        
        # Visual progress bar
        bar_length = 50
        filled_length = int(bar_length * completion_pct / 100)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        print(f"Progress: |{Colors.OKGREEN}{bar}{Colors.ENDC}|")
    
    return missing_codes

def find_missing_soc_codes(missing_codes: set, bls_soc_map: Dict[str, str]):
    """Displays the list of missing SOC codes and offers to export them."""
    print_header("List of Missing SOC Codes")
    if not missing_codes:
        print_success("No missing SOC codes found. Your database is up to date!")
        return

    sorted_missing = sorted(list(missing_codes))
    
    for soc in sorted_missing:
        title = bls_soc_map.get(soc, "Unknown Title")
        print(f"  - {soc}: {title}")

    print_info(f"\nFound {len(sorted_missing)} missing occupations.")
    
    export = input("Do you want to export this list to a file? (y/n): ").lower()
    if export == 'y':
        filename = "missing_soc_codes.csv"
        df = pd.DataFrame([{"soc_code": soc, "title": bls_soc_map.get(soc)} for soc in sorted_missing])
        df.to_csv(filename, index=False)
        print_success(f"Missing SOC codes exported to '{filename}'")

def search_specific_soc(bls_soc_map: Dict[str, str], neon_soc_set: set):
    """Searches for a specific SOC code."""
    print_header("Search for a Specific SOC Code")
    soc_to_find = input("Enter the SOC code to search for (e.g., 13-1082): ").strip()
    if not soc_to_find:
        print_warning("No SOC code entered.")
        return

    title = bls_soc_map.get(soc_to_find)
    
    if title:
        print_success(f"SOC code {soc_to_find} exists in the BLS master list. Title: '{title}'")
    else:
        print_warning(f"SOC code {soc_to_find} was NOT found in the BLS master list.")

    if soc_to_find in neon_soc_set:
        print_success(f"SOC code {soc_to_find} is already loaded in your Neon database.")
    else:
        print_warning(f"SOC code {soc_to_find} is MISSING from your Neon database.")

def bulk_populate_missing(missing_codes: set, bls_soc_map: Dict[str, str], engine):
    """Populates the database with missing SOC codes."""
    print_header("Bulk Populate Missing Occupations")
    if not missing_codes:
        print_success("No missing codes to populate.")
        return

    print_warning(f"This will attempt to populate {len(missing_codes)} missing occupations from the BLS API.")
    confirm = input("This may take a long time and consume API credits. Are you sure you want to proceed? (y/n): ").lower()
    
    if confirm != 'y':
        print_info("Bulk population cancelled.")
        return

    start_time = time.time()
    populated_count = 0
    failed_count = 0
    
    for i, soc_code in enumerate(sorted(list(missing_codes))):
        title = bls_soc_map.get(soc_code, f"Title for {soc_code}")
        print_info(f"[{i+1}/{len(missing_codes)}] Processing: {soc_code} - {title}")
        
        try:
            success, message = bls_job_mapper.fetch_and_process_soc_data(soc_code, title, engine)
            if success:
                print_success(f"  -> Successfully populated {soc_code}.")
                populated_count += 1
            else:
                print_error(f"  -> Failed to populate {soc_code}: {message}")
                failed_count += 1
        except Exception as e:
            print_error(f"  -> An unexpected exception occurred for {soc_code}: {e}")
            failed_count += 1
        
        time.sleep(1.2) # Be respectful to the BLS API

    end_time = time.time()
    duration = end_time - start_time
    
    print_header("Bulk Population Summary")
    print_success(f"Successfully populated {populated_count} occupations.")
    if failed_count > 0:
        print_error(f"Failed to populate {failed_count} occupations. Check logs for details.")
    print_info(f"Total time taken: {duration:.2f} seconds.")

def main():
    """Main function to run the interactive menu."""
    if not MODULE_IMPORT_SUCCESS:
        return

    engine = database.get_db_engine()
    if engine is None:
        print_error("Could not create database engine. Is DATABASE_URL environment variable set correctly?")
        return

    # Pre-load data for the session
    bls_soc_map = get_bls_available_soc_codes()
    neon_soc_set = get_neon_soc_codes(engine)
    missing_codes = set(bls_soc_map.keys()) - neon_soc_set

    while True:
        print_header("BLS vs. NEON Database Audit Tool")
        print("1. Run Full Comparison Summary")
        print("2. List Missing Occupations & Export")
        print("3. Search for a Specific SOC Code")
        print("4. Check for '13-1082 Project Management Specialists'")
        print("5. Bulk-Populate ALL Missing Occupations")
        print("6. Exit")
        
        choice = input("Enter your choice (1-6): ")
        
        if choice == '1':
            compare_bls_vs_neon(bls_soc_map, neon_soc_set)
        elif choice == '2':
            find_missing_soc_codes(missing_codes, bls_soc_map)
        elif choice == '3':
            search_specific_soc(bls_soc_map, neon_soc_set)
        elif choice == '4':
            print_info("--- Checking for '13-1082 Project Management Specialists' ---")
            search_specific_soc({"13-1082": "Project Management Specialists"}, neon_soc_set)
        elif choice == '5':
            bulk_populate_missing(missing_codes, bls_soc_map, engine)
            # Refresh data after population
            neon_soc_set = get_neon_soc_codes(engine)
            missing_codes = set(bls_soc_map.keys()) - neon_soc_set
        elif choice == '6':
            print_info("Exiting audit tool.")
            break
        else:
            print_warning("Invalid choice. Please enter a number between 1 and 6.")

if __name__ == "__main__":
    if "DATABASE_URL" not in os.environ:
        print_error("The DATABASE_URL environment variable is not set.")
        print_info("Please set it before running this script, e.g., export DATABASE_URL='your_neon_db_connection_string'")
    else:
        main()
