import streamlit as st
import os
import time
import logging
from typing import List, Dict, Any

# Configure basic logging for the test page
logger = logging.getLogger("StreamlitTestPage")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Attempt to import necessary application modules
# These should be in the root of your repository or accessible via Python path
try:
    import database
    import bls_connector
    import job_api_integration_database_only as job_api
    MODULE_IMPORT_SUCCESS = True
    logger.info("Successfully imported application modules for test_page.py")
except ImportError as e:
    MODULE_IMPORT_SUCCESS = False
    critical_error_message = f"CRITICAL IMPORT ERROR: Failed to import one or more application modules: {e}. The test page cannot function without them. Ensure database.py, bls_connector.py, and job_api_integration_database_only.py are in the root of your repository."
    logger.critical(critical_error_message)
    # We'll display this error in the Streamlit app itself.

# --- Helper Functions for Displaying Test Results ---
def display_test_header(title: str):
    st.subheader(title)

def display_result(success: bool, message: str, details: Any = None):
    if success:
        st.success(f"✅ PASSED: {message}")
    else:
        st.error(f"❌ FAILED: {message}")
    if details:
        if isinstance(details, (dict, list)):
            st.json(details)
        else:
            st.text_area("Details", str(details), height=100, disabled=True)

# --- Test Functions ---

def run_database_connectivity_test():
    display_test_header("1. Database Connectivity Test")
    if not MODULE_IMPORT_SUCCESS or not hasattr(database, 'check_database_health'):
        display_result(False, "Database module or check_database_health function not available due to import issues.")
        return

    with st.spinner("Testing database connection..."):
        time.sleep(1) # Simulate some work
        try:
            # Check if DATABASE_URL is accessible (either via os.environ or st.secrets within database.py)
            db_url = os.environ.get("DATABASE_URL") or st.secrets.get("database", {}).get("DATABASE_URL")
            if not db_url:
                display_result(False, "DATABASE_URL secret is not set in Streamlit Cloud.")
                return

            health_status = database.check_database_health()
            if health_status.get("status") == "healthy":
                display_result(True, f"Successfully connected to the database. (Message: {health_status.get('message')})", health_status)
            else:
                display_result(False, f"Database health check indicated an issue. (Message: {health_status.get('message')})", health_status)
        except Exception as e:
            logger.error(f"Exception during database connectivity test: {e}", exc_info=True)
            display_result(False, f"An exception occurred: {e}", str(e))

def run_bls_api_connectivity_test():
    display_test_header("2. BLS API Key & Connectivity Test")
    if not MODULE_IMPORT_SUCCESS or not hasattr(bls_connector, 'check_api_connectivity'):
        display_result(False, "BLS Connector module or check_api_connectivity function not available due to import issues.")
        return

    with st.spinner("Testing BLS API connection..."):
        time.sleep(1)
        try:
            # Check if BLS_API_KEY is accessible
            api_key = os.environ.get("BLS_API_KEY") or st.secrets.get("api_keys", {}).get("BLS_API_KEY")
            if not api_key:
                display_result(False, "BLS_API_KEY secret is not set in Streamlit Cloud.")
                return

            is_connected = bls_connector.check_api_connectivity()
            if is_connected:
                display_result(True, "BLS API key appears valid and connection was successful.")
            else:
                display_result(False, "BLS API connectivity check failed. The API key might be invalid, expired, or the BLS API service might be temporarily unavailable.")
        except Exception as e:
            logger.error(f"Exception during BLS API connectivity test: {e}", exc_info=True)
            display_result(False, f"An exception occurred: {e}", str(e))

def run_single_job_search_test():
    display_test_header("3. Basic Single Job Search Test")
    if not MODULE_IMPORT_SUCCESS or not hasattr(job_api, 'get_job_data'):
        display_result(False, "Job API Integration module or get_job_data function not available due to import issues.")
        return

    sample_job_title = "Software Developer" # A common job title expected to be in BLS data
    with st.spinner(f"Performing a search for '{sample_job_title}'..."):
        time.sleep(1)
        try:
            data = job_api.get_job_data(sample_job_title)
            if data and "error" not in data and data.get("occupation_code") and data.get("occupation_code") != "00-0000":
                result_summary = {
                    "Job Title (Standardized)": data.get("job_title"),
                    "Occupation Code": data.get("occupation_code"),
                    "Job Category": data.get("job_category"),
                    "Source": data.get("source"),
                    "5-Year Risk (%)": data.get("year_5_risk"),
                    "Risk Category": data.get("risk_category"),
                    "Median Wage": data.get("median_wage") or data.get("bls_data", {}).get("median_wage"),
                    "Analysis Snippet": data.get("analysis", "")[:150] + "..." if data.get("analysis") else "N/A"
                }
                display_result(True, f"Successfully fetched and processed data for '{sample_job_title}'.", result_summary)
            elif data and "error" in data:
                display_result(False, f"API/DB call for '{sample_job_title}' returned an error.", data["error"])
            else:
                display_result(False, f"Received no specific error, but data for '{sample_job_title}' is incomplete, generic (e.g., SOC 00-0000), or missing.", data)
        except Exception as e:
            logger.error(f"Exception during single job search test for '{sample_job_title}': {e}", exc_info=True)
            display_result(False, f"An exception occurred: {e}", str(e))

def run_job_comparison_test():
    display_test_header("4. Basic Job Comparison Test")
    if not MODULE_IMPORT_SUCCESS or not hasattr(job_api, 'get_jobs_comparison_data'):
        display_result(False, "Job API Integration module or get_jobs_comparison_data function not available due to import issues.")
        return

    sample_job_list = ["Registered Nurse", "Accountant", "Graphic Designer"]
    with st.spinner(f"Performing comparison for jobs: {', '.join(sample_job_list)}..."):
        time.sleep(1)
        try:
            comparison_data = job_api.get_jobs_comparison_data(sample_job_list)
            if not comparison_data:
                display_result(False, "Job comparison returned no data at all.")
                return

            results_summary = []
            all_successful = True
            for job_title, data in comparison_data.items():
                if data and "error" not in data and data.get("occupation_code") and data.get("occupation_code") != "00-0000":
                    results_summary.append({
                        "Searched Title": job_title,
                        "Standardized Title": data.get("job_title"),
                        "SOC": data.get("occupation_code"),
                        "5Y Risk (%)": data.get("year_5_risk"),
                        "Status": "Success"
                    })
                else:
                    all_successful = False
                    results_summary.append({
                        "Searched Title": job_title,
                        "Status": "Failed/Incomplete",
                        "Error": data.get("error", "Data incomplete or generic")
                    })
            
            if all_successful and results_summary:
                display_result(True, "Successfully fetched and processed data for all jobs in comparison.", results_summary)
            elif results_summary: # Partial success
                display_result(False, "Job comparison processed, but some jobs had issues or incomplete data.", results_summary)
            else: # Should not happen if comparison_data was not empty, but as a safeguard
                display_result(False, "Job comparison processed, but no valid results were obtained.", comparison_data)

        except Exception as e:
            logger.error(f"Exception during job comparison test for {sample_job_list}: {e}", exc_info=True)
            display_result(False, f"An exception occurred: {e}", str(e))

# --- Streamlit Page UI ---
st.set_page_config(page_title="App Health & Test Page", layout="wide")
st.title("⚙️ Application Health & Functionality Test Page")
st.markdown("""
This page helps verify that the core components of the AI Job Risk Analyzer are functioning correctly
on the production server. It checks database connectivity, BLS API access, and basic data processing.
""")

if not MODULE_IMPORT_SUCCESS:
    st.error(critical_error_message)
    st.warning("Most tests will not run. Please check the application logs and ensure all Python files are correctly uploaded to your GitHub repository.")
else:
    st.sidebar.header("Run Tests")
    if st.sidebar.button("Run All Tests", type="primary", use_container_width=True):
        with st.expander("1. Database Connectivity Test Results", expanded=True):
            run_database_connectivity_test()
        with st.expander("2. BLS API Key & Connectivity Test Results", expanded=True):
            run_bls_api_connectivity_test()
        with st.expander("3. Basic Single Job Search Test Results", expanded=True):
            run_single_job_search_test()
        with st.expander("4. Basic Job Comparison Test Results", expanded=True):
            run_job_comparison_test()
        st.sidebar.success("All tests initiated!")

    st.sidebar.markdown("---")
    if st.sidebar.button("Test Database Only", use_container_width=True):
        with st.expander("1. Database Connectivity Test Results", expanded=True):
            run_database_connectivity_test()
    if st.sidebar.button("Test BLS API Only", use_container_width=True):
        with st.expander("2. BLS API Key & Connectivity Test Results", expanded=True):
            run_bls_api_connectivity_test()
    if st.sidebar.button("Test Single Job Search", use_container_width=True):
        with st.expander("3. Basic Single Job Search Test Results", expanded=True):
            run_single_job_search_test()
    if st.sidebar.button("Test Job Comparison", use_container_width=True):
        with st.expander("4. Basic Job Comparison Test Results", expanded=True):
            run_job_comparison_test()

    st.markdown("---")
    st.header("Current Environment Status (from server perspective)")
    
    # Display secrets status (without revealing actual secrets)
    db_url_secret = "Not Set"
    try:
        if os.environ.get("DATABASE_URL") or st.secrets.get("database", {}).get("DATABASE_URL"):
            db_url_secret = "Set (found in env or secrets)"
    except Exception: # st.secrets might not be available if not fully initialized
        if os.environ.get("DATABASE_URL"):
             db_url_secret = "Set (found in env)"
    st.markdown(f"- **DATABASE_URL Secret**: `{db_url_secret}`")

    bls_api_key_secret = "Not Set"
    try:
        if os.environ.get("BLS_API_KEY") or st.secrets.get("api_keys", {}).get("BLS_API_KEY"):
            bls_api_key_secret = "Set (found in env or secrets)"
    except Exception:
        if os.environ.get("BLS_API_KEY"):
            bls_api_key_secret = "Set (found in env)"
    st.markdown(f"- **BLS_API_KEY Secret**: `{bls_api_key_secret}`")

    st.markdown(f"- **Python Version**: `{sys.version.split()[0]}`")
    st.markdown(f"- **Streamlit Version**: `{st.__version__}`")

    st.markdown("---")
    st.info("Click the buttons in the sidebar to run specific tests or all tests.")

# Instructions on how to use this page:
# 1. Save this code as `test_page.py` in the root of your GitHub repository.
# 2. Ensure all other application modules (database.py, bls_connector.py, etc.) are also in the root.
# 3. Deploy or redeploy your Streamlit application.
# 4. Access this page by navigating to `your-app-url/test_page`.
#    (Streamlit automatically creates routes for .py files in the root or a `pages/` directory).
#    If it's in the root, and your main app is `app.py`, then `your-app-url/Test_Page` might be the URL.
#    If you create a `pages/` subdirectory and put `test_page.py` there, it will appear in the sidebar navigation.
#    For simplicity, placing it in the root and accessing via `your-app-url/test_page` is fine.
