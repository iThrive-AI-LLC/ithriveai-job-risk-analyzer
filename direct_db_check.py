import os
import sys
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

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

def print_success(message):
    print(f"{Colors.OKGREEN}[SUCCESS] {message}{Colors.ENDC}")

# --- Core Functions ---

def get_database_url():
    """
    Retrieves the database URL from environment variables or prompts the user if not found.
    """
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        print_info("Found DATABASE_URL in environment variables.")
        return db_url
    
    print_warning("DATABASE_URL environment variable not set.")
    print_info("Please paste your Neon database connection string below.")
    db_url = input("Enter DATABASE_URL: ")
    return db_url.strip()

def run_direct_db_check():
    """
    Main function to connect to the DB and run all statistical checks.
    """
    db_url = get_database_url()
    if not db_url:
        print_error("No database URL provided. Exiting.")
        return

    print_info("Attempting to connect to the database...")
    
    try:
        # Ensure URL is in the correct format for SQLAlchemy
        if db_url.startswith('postgres://'):
            db_url = db_url.replace('postgres://', 'postgresql://', 1)

        engine = create_engine(db_url, connect_args={"sslmode": "require"}, echo=False)
        
        with engine.connect() as conn:
            print_success("Database connection successful!")

            # 1. Check if table exists and get total count
            print_header("Total Occupation Count")
            try:
                total_count = conn.execute(text("SELECT COUNT(*) FROM bls_job_data")).scalar_one()
                print_stat("Total Occupations in 'bls_job_data':", total_count)
            except ProgrammingError as e:
                if 'relation "bls_job_data" does not exist' in str(e):
                    print_error("The table 'bls_job_data' does not exist in your database.")
                    print_info("You may need to run your application's admin/population tool to create and populate the table.")
                    return # Stop further checks if the table is missing
                else:
                    raise # Re-raise other programming errors

            # 2. Check for specific SOC code
            print_header("Specific SOC Code Check")
            soc_to_find = "13-1082"
            exists_result = conn.execute(
                text("SELECT EXISTS(SELECT 1 FROM bls_job_data WHERE occupation_code = :soc)"),
                {"soc": soc_to_find}
            ).scalar_one()

            if exists_result:
                print_success(f"SOC Code '{soc_to_find}' (Project Management Specialists) is loaded in the database.")
            else:
                print_warning(f"SOC Code '{soc_to_find}' (Project Management Specialists) is MISSING from the database.")

            # 3. Basic statistics
            print_header("Database Statistics")
            category_count = conn.execute(text("SELECT COUNT(DISTINCT job_category) FROM bls_job_data")).scalar_one()
            print_stat("Number of Unique Job Categories:", category_count)

            latest_date = conn.execute(text("SELECT MAX(last_updated) FROM bls_job_data")).scalar_one_or_none()
            print_stat("Most Recent Data Entry:", latest_date or "N/A")
            
            oldest_date = conn.execute(text("SELECT MIN(last_updated) FROM bls_job_data")).scalar_one_or_none()
            print_stat("Oldest Data Entry:", oldest_date or "N/A")

    except OperationalError as e:
        print_error("Could not connect to the database. Please check your connection string and network.")
        print_info(f"Details: {e}")
    except Exception as e:
        print_error(f"An unexpected error occurred: {e}")
        logging.exception("An unexpected error occurred during the database check.")

if __name__ == "__main__":
    run_direct_db_check()
