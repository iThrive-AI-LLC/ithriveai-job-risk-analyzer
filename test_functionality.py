import os
import sys
import time
import logging
from typing import List, Dict, Any, Optional

# --- Configuration for colored output ---
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(message: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}===== {message} ====={Colors.ENDC}")

def print_success(message: str):
    print(f"{Colors.OKGREEN}[SUCCESS] {message}{Colors.ENDC}")

def print_failure(message: str):
    print(f"{Colors.FAIL}[FAILURE] {message}{Colors.ENDC}")

def print_warning(message: str):
    print(f"{Colors.WARNING}[WARNING] {message}{Colors.ENDC}")

def print_info(message: str):
    print(f"{Colors.OKBLUE}[INFO] {message}{Colors.ENDC}")

# --- Logger Setup ---
# Configure basic logging for the test script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress overly verbose logs from imported modules if necessary, or set their level
logging.getLogger("bls_connector").setLevel(logging.WARNING)
logging.getLogger("bls_job_mapper").setLevel(logging.WARNING)
logging.getLogger("database").setLevel(logging.WARNING) # Assuming database.py also uses logging

# --- Attempt to import necessary application modules ---
# This structure assumes the test script is in the root directory of the project,
# and the modules are directly importable.
try:
    import database
    import bls_connector
    import job_api_integration_database_only as job_api # Main interface for data
    import bls_job_mapper # To check its direct functions if needed
except ImportError as e:
    print_failure(f"Critical Import Error: {e}. Please ensure all required modules are in the Python path.")
    print_warning("Some tests may not run or may behave unexpectedly.")
    sys.exit(1)

# --- Environment Variable Check ---
DATABASE_URL = os.environ.get("DATABASE_URL")
BLS_API_KEY = os.environ.get("BLS_API_KEY")

def check_env_vars():
    print_header("Environment Variable Check")
    all_vars_ok = True
    if not DATABASE_URL:
        print_failure("DATABASE_URL is not set. Please set this environment variable.")
        all_vars_ok = False
    else:
        print_success(f"DATABASE_URL found: {DATABASE_URL[:20]}...") # Print partial URL for security

    if not BLS_API_KEY:
        print_failure("BLS_API_KEY is not set. Please set this environment variable.")
        all_vars_ok = False
    else:
        print_success(f"BLS_API_KEY found: {'*' * (len(BLS_API_KEY) - 4) + BLS_API_KEY[-4:]}") # Mask API key

    if not all_vars_ok:
        print_warning("One or more environment variables are missing. Tests may fail or be skipped.")
    return all_vars_ok

# --- Test Functions ---

def test_database_connectivity():
    print_header("Test 1: Database Connectivity to Neon")
    if not DATABASE_URL:
        print_warning("Skipping database connectivity test: DATABASE_URL not set.")
        return False
    try:
        # Use the check_database_health function if available and robust
        if hasattr(database, 'check_database_health'):
            health_status = database.check_database_health()
            if health_status.get("status") == "healthy":
                print_success(f"Database connection successful. (Message: {health_status.get('message')}, Response Time: {health_status.get('response_time_ms')}ms)")
                return True
            else:
                print_failure(f"Database health check failed. Status: {health_status.get('status')}, Message: {health_status.get('message')}")
                return False
        else:
            # Fallback to a direct engine test if check_database_health is not found
            print_info("database.check_database_health not found, attempting direct connection test.")
            engine = database.create_db_engine(DATABASE_URL, max_retries=1) # Use the create_db_engine from database.py
            if engine:
                with engine.connect() as conn:
                    conn.execute(database.text("SELECT 1"))
                print_success("Direct database connection successful using database.create_db_engine.")
                return True
            else:
                print_failure("Failed to create database engine using database.create_db_engine.")
                return False
    except Exception as e:
        print_failure(f"Database connectivity test failed with exception: {e}")
        logger.exception("Exception during database connectivity test:")
        return False

def test_bls_api_connectivity():
    print_header("Test 2: BLS API Key Validity & Connectivity")
    if not BLS_API_KEY:
        print_warning("Skipping BLS API connectivity test: BLS_API_KEY not set.")
        return False
    try:
        is_connected = bls_connector.check_api_connectivity()
        if is_connected:
            print_success("BLS API key is valid and connection successful.")
            return True
        else:
            print_failure("BLS API connectivity check failed. Key might be invalid or API unreachable.")
            return False
    except Exception as e:
        print_failure(f"BLS API connectivity test failed with exception: {e}")
        logger.exception("Exception during BLS API connectivity test:")
        return False

def test_individual_job_searches():
    print_header("Test 3: Individual Job Title Searches (Real BLS Data Only)")
    if not (DATABASE_URL and BLS_API_KEY):
        print_warning("Skipping individual job searches: DB_URL or API_KEY missing.")
        return False

    test_jobs = [
        {"title": "Software Developer", "expect_success": True},
        {"title": "Registered Nurse", "expect_success": True},
        {"title": "Financial Analyst", "expect_success": True},
        {"title": "DefinitelyNotARealJobTitleXYZ123", "expect_success": False}, # Expected to fail gracefully
        {"title": "Cashier", "expect_success": True}
    ]
    all_passed = True

    for job_info in test_jobs:
        title = job_info["title"]
        expect_success = job_info["expect_success"]
        print_info(f"Testing job title: '{title}' (Expected to {'succeed' if expect_success else 'fail gracefully'})")
        try:
            data = job_api.get_job_data(title)
            if "error" not in data and data.get("occupation_code") != "00-0000":
                if expect_success:
                    print_success(f"Successfully fetched data for '{data.get('job_title', title)}' (SOC: {data.get('occupation_code')})")
                    print(f"  Source: {data.get('source')}")
                    print(f"  Category: {data.get('job_category')}")
                    print(f"  5-Year Risk: {data.get('year_5_risk')}% ({data.get('risk_category')})")
                    print(f"  Median Wage: ${data.get('median_wage', data.get('bls_data', {}).get('median_wage', 'N/A')):,.0f}")
                    if not data.get('source') or "error" in data.get('source', '').lower():
                         print_warning(f"Data source for '{title}' indicates an issue: {data.get('source')}")
                         all_passed = False
                else:
                    print_failure(f"Expected graceful failure for '{title}', but got success: SOC {data.get('occupation_code')}")
                    all_passed = False
            elif "error" in data:
                if expect_success:
                    print_failure(f"Failed to fetch data for '{title}': {data['error']}")
                    all_passed = False
                else:
                    print_success(f"Gracefully handled non-existent/problematic job title '{title}': {data['error']}")
            else: # No error, but occupation_code might be "00-0000" or data incomplete
                if expect_success:
                    print_warning(f"Data for '{title}' might be incomplete or generic (SOC: {data.get('occupation_code')}). Review details.")
                    print(f"  Source: {data.get('source')}, Category: {data.get('job_category')}")
                    # Potentially mark as partial success or failure based on strictness
                else: # Expected failure, but got a generic success
                    print_success(f"Gracefully handled '{title}' by returning generic data (SOC: {data.get('occupation_code')})")
        except Exception as e:
            print_failure(f"Test for '{title}' failed with an unexpected exception: {e}")
            logger.exception(f"Exception during individual job search test for '{title}':")
            all_passed = False
        print("-" * 30)
        time.sleep(1) # Be nice to the API if it's being hit directly by bls_job_mapper
    return all_passed

def test_job_comparison():
    print_header("Test 4: Job Comparison Functionality (Up to 5 Jobs)")
    if not (DATABASE_URL and BLS_API_KEY):
        print_warning("Skipping job comparison: DB_URL or API_KEY missing.")
        return False

    job_sets_to_compare = [
        ["Software Developer", "Web Developer", "Data Scientist"], # 3 jobs
        ["Registered Nurse", "Physician Assistant", "Medical Assistant", "Pharmacist", "Dental Hygienist"], # 5 jobs
        ["Truck Driver", "DefinitelyNotARealJobTitleXYZ123", "Cashier"] # Mix valid and invalid
    ]
    all_passed = True

    for i, job_list in enumerate(job_sets_to_compare):
        print_info(f"Testing comparison for job set {i+1}: {job_list}")
        try:
            comparison_data = job_api.get_jobs_comparison_data(job_list)
            if not comparison_data:
                print_failure(f"Comparison for set {i+1} returned no data.")
                all_passed = False
                continue

            successful_jobs = 0
            for job_title, data in comparison_data.items():
                if "error" not in data:
                    print_success(f"  Data for '{data.get('job_title', job_title)}' (SOC: {data.get('occupation_code')}): 5Y Risk {data.get('year_5_risk')}%")
                    successful_jobs +=1
                else:
                    if job_title == "DefinitelyNotARealJobTitleXYZ123": # Expected error
                         print_success(f"  Gracefully handled invalid title '{job_title}' in comparison: {data['error']}")
                    else:
                        print_warning(f"  Error for '{job_title}' in comparison: {data['error']}")
            
            if successful_jobs == 0 and any(jt != "DefinitelyNotARealJobTitleXYZ123" for jt in job_list) :
                print_failure(f"No valid job data retrieved for comparison set {i+1}.")
                all_passed = False
            elif successful_jobs < sum(1 for jt in job_list if jt != "DefinitelyNotARealJobTitleXYZ123"):
                 print_warning(f"Partial success for comparison set {i+1}. Some jobs had errors.")
                 # Decide if this counts as a failure for `all_passed` based on strictness
            else:
                print_success(f"Comparison set {i+1} processed.")

        except Exception as e:
            print_failure(f"Comparison test for set {i+1} failed with an unexpected exception: {e}")
            logger.exception(f"Exception during job comparison test for set {job_list}:")
            all_passed = False
        print("-" * 30)
        time.sleep(2) # Be nice to API
    return all_passed

def test_error_handling_invalid_input():
    print_header("Test 5: Error Handling for Invalid Inputs")
    if not (DATABASE_URL and BLS_API_KEY): # Basic checks still need these to try API calls
        print_warning("Skipping error handling test: DB_URL or API_KEY missing.")
        return False # Cannot meaningfully test API/DB error paths

    invalid_job_title = "    " # Empty string / only whitespace
    print_info(f"Testing with invalid job title: '{invalid_job_title}'")
    all_passed = True
    try:
        data = job_api.get_job_data(invalid_job_title)
        if "error" in data:
            print_success(f"Correctly handled invalid job title. Error: {data['error']}")
        else:
            print_failure(f"Invalid job title not handled correctly. Data: {data}")
            all_passed = False
    except Exception as e:
        print_failure(f"Test for invalid job title failed with an unexpected exception: {e}")
        logger.exception("Exception during error handling test:")
        all_passed = False
    
    # Test with a very long, likely problematic string
    long_job_title = "a" * 500
    print_info(f"Testing with very long job title (500 chars)")
    try:
        data = job_api.get_job_data(long_job_title)
        if "error" in data:
            print_success(f"Correctly handled long job title. Error: {data['error']}")
        else:
            print_failure(f"Long job title not handled correctly. Data: {data}")
            all_passed = False
    except Exception as e:
        print_failure(f"Test for long job title failed with an unexpected exception: {e}")
        logger.exception("Exception during long title error handling test:")
        all_passed = False

    return all_passed

# --- Main Test Execution ---
if __name__ == "__main__":
    print_header("Starting AI Job Risk Analyzer Functionality Tests")
    
    if not check_env_vars():
        print_failure("Cannot proceed with tests due to missing environment variables.")
        sys.exit(1)

    results = {}
    results["db_connectivity"] = test_database_connectivity()
    results["bls_api_connectivity"] = test_bls_api_connectivity()
    
    # Only proceed with data-dependent tests if basic connectivity is OK
    if results["db_connectivity"] and results["bls_api_connectivity"]:
        results["individual_searches"] = test_individual_job_searches()
        results["job_comparison"] = test_job_comparison()
        results["error_handling"] = test_error_handling_invalid_input()
    else:
        print_warning("Skipping data-dependent tests due to connectivity issues.")
        results["individual_searches"] = "SKIPPED"
        results["job_comparison"] = "SKIPPED"
        results["error_handling"] = "SKIPPED"

    print_header("Test Summary")
    final_status_ok = True
    for test_name, status in results.items():
        if status is True:
            print_success(f"{test_name.replace('_', ' ').title()}: PASSED")
        elif status == "SKIPPED":
            print_warning(f"{test_name.replace('_', ' ').title()}: SKIPPED")
        else: # False
            print_failure(f"{test_name.replace('_', ' ').title()}: FAILED")
            final_status_ok = False
            
    if final_status_ok and all(s != "SKIPPED" for s in results.values()):
        print_success(f"{Colors.BOLD}All tests passed successfully!{Colors.ENDC}")
    elif any(s == "SKIPPED" for s in results.values()) and final_status_ok :
         print_warning(f"{Colors.BOLD}Some tests were skipped. Review logs. Other tests passed.{Colors.ENDC}")
    else:
        print_failure(f"{Colors.BOLD}One or more tests failed. Please review the logs.{Colors.ENDC}")

    print_info("Make sure your Neon database is active and BLS API key is correct if you see failures.")
    print_info("Test script execution finished.")
