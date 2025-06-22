import streamlit as st
import pandas as pd
import os
import json
import time
import datetime
import logging
from sqlalchemy import create_engine, text, inspect

# Attempt to import necessary application modules
try:
    import database # For database.py utility functions like get_db_engine, check_database_health
    import bls_job_mapper # For get_complete_job_data and the list of known SOCs
    import bls_connector # For check_api_connectivity
    MODULE_IMPORT_SUCCESS = True
except ImportError as e:
    MODULE_IMPORT_SUCCESS = False
    # This error will be displayed prominently in the Streamlit app UI
    CRITICAL_ERROR_MESSAGE = f"CRITICAL IMPORT ERROR: Failed to import one or more essential application modules: {e}. The Admin Dashboard cannot function without them. Ensure database.py, bls_job_mapper.py, and bls_connector.py are in the root of your repository and all dependencies are installed."
    logging.basicConfig(level=logging.ERROR) # Basic logging if main logger isn't set up
    logging.critical(CRITICAL_ERROR_MESSAGE)


# --- Configuration ---
POPULATION_PROGRESS_FILE = "admin_population_progress.json"
LOG_FILE = "admin_population_log.txt"
# Get a base list of SOC codes and representative titles from bls_job_mapper
# We want unique SOC codes to avoid redundant processing.
ALL_KNOWN_SOC_CODES_WITH_TITLES = {}
if MODULE_IMPORT_SUCCESS and hasattr(bls_job_mapper, 'JOB_TITLE_TO_SOC'):
    for title, soc in bls_job_mapper.JOB_TITLE_TO_SOC.items():
        if soc not in ALL_KNOWN_SOC_CODES_WITH_TITLES: # Keep the first title encountered for a SOC
            ALL_KNOWN_SOC_CODES_WITH_TITLES[soc] = title
else:
    # Provide a minimal list if bls_job_mapper is not available, so the page can load
    ALL_KNOWN_SOC_CODES_WITH_TITLES = {
        "15-1252": "Software Developer",
        "29-1141": "Registered Nurse",
        "13-2011": "Accountant"
    }
    if MODULE_IMPORT_SUCCESS: # If only JOB_TITLE_TO_SOC was missing
        logging.warning("bls_job_mapper.JOB_TITLE_TO_SOC not found or empty. Using a minimal SOC list for population tool.")


# --- Logger Setup ---
logger = logging.getLogger("AdminDashboard")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    # File handler for persistent logs
    file_handler = logging.FileHandler(LOG_FILE, mode='a') # Append mode
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    # Stream handler for console output (if run directly or for Streamlit logs)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(file_formatter)
    logger.addHandler(stream_handler)

# --- Helper Functions ---
def get_db_engine_instance():
    """Safely gets a database engine instance using database.py utilities."""
    if not MODULE_IMPORT_SUCCESS or not hasattr(database, 'get_db_engine'):
        logger.error("database.get_db_engine function is not available.")
        st.error("Database module is not correctly loaded. Cannot get engine.")
        return None
    try:
        return database.get_db_engine()
    except Exception as e:
        logger.error(f"Failed to get database engine: {e}", exc_info=True)
        st.error(f"Failed to initialize database engine: {e}")
        return None

def load_population_progress():
    """Loads population progress from the JSON file."""
    if os.path.exists(POPULATION_PROGRESS_FILE):
        try:
            with open(POPULATION_PROGRESS_FILE, "r") as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Error loading progress file '{POPULATION_PROGRESS_FILE}': {e}")
    # Default progress if file doesn't exist or is corrupt
    return {
        "all_target_soc_codes": list(ALL_KNOWN_SOC_CODES_WITH_TITLES.keys()), # List of SOCs to process
        "current_index": 0,
        "processed_count": 0,
        "successfully_populated_socs": [],
        "failed_socs": {}, # Store SOC: error_message
        "last_run_timestamp": None,
        "total_to_process": len(ALL_KNOWN_SOC_CODES_WITH_TITLES)
    }

def save_population_progress(progress_data):
    """Saves population progress to the JSON file."""
    progress_data["last_run_timestamp"] = datetime.datetime.now().isoformat()
    try:
        with open(POPULATION_PROGRESS_FILE, "w") as f:
            json.dump(progress_data, f, indent=4)
    except IOError as e:
        logger.error(f"Error saving progress file '{POPULATION_PROGRESS_FILE}': {e}")
        st.warning(f"Could not save population progress: {e}")

def get_database_stats(engine):
    """Retrieves basic statistics from the bls_job_data table."""
    if engine is None:
        return {"error": "Database engine not available."}
    stats = {"total_records": 0, "last_entry_timestamp": None, "table_exists": False}
    try:
        with engine.connect() as conn:
            inspector = inspect(engine)
            if 'bls_job_data' in inspector.get_table_names():
                stats["table_exists"] = True
                result = conn.execute(text("SELECT COUNT(*) FROM bls_job_data")).scalar_one_or_none()
                stats["total_records"] = result if result is not None else 0
                
                # Try to get the latest update timestamp from the table if the column exists
                # Note: The 'last_updated_in_db' or 'last_api_fetch' column names are from bls_job_mapper.py
                # Check if columns exist before querying
                columns_in_table = [col['name'] for col in inspector.get_columns('bls_job_data')]
                timestamp_col_to_query = None
                if 'last_updated_in_db' in columns_in_table:
                    timestamp_col_to_query = 'last_updated_in_db'
                elif 'last_api_fetch' in columns_in_table:
                    timestamp_col_to_query = 'last_api_fetch'

                if timestamp_col_to_query:
                    ts_result = conn.execute(text(f"SELECT MAX({timestamp_col_to_query}) FROM bls_job_data")).scalar_one_or_none()
                    stats["last_entry_timestamp"] = ts_result if ts_result else "N/A"
                else:
                    stats["last_entry_timestamp"] = "Timestamp column not found"
            else:
                stats["error"] = "'bls_job_data' table does not exist."
    except Exception as e:
        logger.error(f"Error getting database stats: {e}", exc_info=True)
        stats["error"] = str(e)
    return stats

# --- Streamlit UI ---
st.title("⚙️ Admin Dashboard: BLS Data Management")

if not MODULE_IMPORT_SUCCESS:
    st.error(CRITICAL_ERROR_MESSAGE)
    st.stop()

# Initialize session state variables
if "population_running" not in st.session_state:
    st.session_state.population_running = False
if "current_batch_logs" not in st.session_state:
    st.session_state.current_batch_logs = []
if "population_progress" not in st.session_state:
    st.session_state.population_progress = load_population_progress()

# Ensure 'all_target_soc_codes' is up-to-date with current known SOCs if progress is reset or first time
if not st.session_state.population_progress.get("all_target_soc_codes") or \
   set(st.session_state.population_progress["all_target_soc_codes"]) != set(ALL_KNOWN_SOC_CODES_WITH_TITLES.keys()):
    logger.info("Updating target SOC codes list in progress file based on current ALL_KNOWN_SOC_CODES_WITH_TITLES.")
    st.session_state.population_progress["all_target_soc_codes"] = list(ALL_KNOWN_SOC_CODES_WITH_TITLES.keys())
    st.session_state.population_progress["total_to_process"] = len(ALL_KNOWN_SOC_CODES_WITH_TITLES)
    # Optionally reset current_index if the list changed significantly, or handle intelligently.
    # For simplicity, if the list changes, a reset might be safest unless complex diffing is done.


engine = get_db_engine_instance()

tab1, tab2, tab3 = st.tabs(["📊 Dashboard & Stats", "📥 Database Population Tool", "📜 View Logs"])

with tab1:
    st.header("Current Database Status")
    if engine:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader("Database Connectivity")
            if hasattr(database, 'check_database_health'):
                db_health = database.check_database_health(engine)
                if db_health == "OK":
                    st.success("Connected")
                elif db_health == "Not Configured":
                    st.warning("Connection not configured")
                else:
                    st.error("Connection Issue")
            else:
                st.warning("database.check_database_health not available.")
        
        with col2:
            st.subheader("BLS API Connectivity")
            if hasattr(bls_connector, 'check_api_connectivity'):
                api_key = os.environ.get('BLS_API_KEY') or st.secrets.get("api_keys", {}).get("BLS_API_KEY")
                if not api_key:
                    st.error("BLS_API_KEY not configured in secrets/env.")
                elif bls_connector.check_api_connectivity():
                    st.success("BLS API Accessible")
                else:
                    st.error("BLS API Not Accessible (check key or service status)")
            else:
                st.warning("bls_connector.check_api_connectivity not available.")

        with col3:
            st.subheader("Data Population Progress")
            progress_data = st.session_state.population_progress
            total_to_process = progress_data.get("total_to_process", len(ALL_KNOWN_SOC_CODES_WITH_TITLES))
            processed_count = progress_data.get("processed_count", 0)
            if total_to_process > 0:
                st.progress(processed_count / total_to_process if total_to_process > 0 else 0)
                st.write(f"Processed: {processed_count} of {total_to_process} target SOCs.")
                st.write(f"Successfully populated: {len(progress_data.get('successfully_populated_socs', []))}")
                st.write(f"Failures: {len(progress_data.get('failed_socs', {}))}")
            else:
                st.info("No target SOCs defined for population.")
            if progress_data.get("last_run_timestamp"):
                st.caption(f"Last population run: {datetime.datetime.fromisoformat(progress_data['last_run_timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")


        st.subheader("`bls_job_data` Table Statistics")
        db_stats = get_database_stats(engine)
        if "error" in db_stats:
            st.error(f"Could not retrieve database stats: {db_stats['error']}")
        elif not db_stats.get("table_exists", False):
            st.warning("The `bls_job_data` table does not seem to exist in the database.")
        else:
            st.metric("Total Occupations Cached", db_stats.get("total_records", 0))
            st.metric("Last DB Entry Timestamp", str(db_stats.get("last_entry_timestamp", "N/A")))
            
            # Display target SOCs vs Cached SOCs
            cached_socs_query = "SELECT DISTINCT occupation_code FROM bls_job_data"
            df_cached_socs = pd.read_sql(text(cached_socs_query), engine)
            cached_soc_set = set(df_cached_socs['occupation_code'])
            
            target_soc_set = set(ALL_KNOWN_SOC_CODES_WITH_TITLES.keys())
            
            missing_from_db = target_soc_set - cached_soc_set
            extra_in_db = cached_soc_set - target_soc_set # SOCs in DB but not in our hardcoded list
            
            st.info(f"Target SOCs in `ALL_KNOWN_SOC_CODES_WITH_TITLES`: {len(target_soc_set)}")
            st.info(f"Unique SOCs currently in `bls_job_data` table: {len(cached_soc_set)}")
            
            if missing_from_db:
                with st.expander(f"Target SOCs Missing from Database ({len(missing_from_db)}) Click to see list"):
                    st.write(sorted(list(missing_from_db)))
            else:
                st.success("All target SOCs from `ALL_KNOWN_SOC_CODES_WITH_TITLES` appear to be in the database.")

            if extra_in_db:
                 with st.expander(f"SOCs in Database NOT in `ALL_KNOWN_SOC_CODES_WITH_TITLES` ({len(extra_in_db)}) (May be normal if other sources add data)"):
                    st.write(sorted(list(extra_in_db)))

    else:
        st.error("Database engine is not initialized. Cannot display stats.")


with tab2:
    st.header("BLS Data Population Tool")
    st.markdown("""
    This tool incrementally fetches data for all known BLS Standard Occupational Classification (SOC) codes
    from the BLS API and stores it in your Neon database. This helps ensure your application uses
    comprehensive, real BLS data and improves response times by serving data from the cache.
    """)

    if not engine:
        st.error("Database engine not available. Population tool cannot run.")
    else:
        progress_data = st.session_state.population_progress
        total_target_socs = len(progress_data.get("all_target_soc_codes", []))
        current_idx = progress_data.get("current_index", 0)
        processed_count = progress_data.get("processed_count", 0)
        
        st.info(f"Current Progress: {processed_count} SOCs processed out of {total_target_socs} target SOCs. Next to process: Index {current_idx}.")

        batch_size = st.number_input("Batch Size (Number of SOCs per run)", min_value=1, max_value=50, value=5, step=1,
                                     help="How many SOC codes to process in one go. BLS API has limits.")
        api_delay = st.number_input("Delay Between API Calls (seconds)", min_value=0.5, max_value=10.0, value=1.5, step=0.1,
                                    help="Time to wait between fetching data for each SOC to respect API rate limits.")

        col_run, col_pause, col_reset = st.columns(3)
        with col_run:
            if st.button("▶️ Start/Resume Population", type="primary", disabled=st.session_state.population_running, use_container_width=True):
                st.session_state.population_running = True
                st.session_state.current_batch_logs = [] # Clear logs for new run
                logger.info("Population process started/resumed by admin.")
                st.rerun() # Rerun to reflect the change in population_running state
        with col_pause:
            if st.button("⏸️ Pause Population", disabled=not st.session_state.population_running, use_container_width=True):
                st.session_state.population_running = False
                logger.info("Population process paused by admin.")
                save_population_progress(st.session_state.population_progress) # Save current state
                st.rerun()
        with col_reset:
            if st.button("🔄 Reset Progress", use_container_width=True, help="Resets the population progress, allowing you to start over. Does not delete existing data from the database."):
                if st.session_state.population_running:
                    st.warning("Please pause the population process before resetting.")
                else:
                    st.session_state.population_progress = load_population_progress() # Load default/initial state
                    st.session_state.population_progress["current_index"] = 0
                    st.session_state.population_progress["processed_count"] = 0
                    st.session_state.population_progress["successfully_populated_socs"] = []
                    st.session_state.population_progress["failed_socs"] = {}
                    st.session_state.population_progress["all_target_soc_codes"] = list(ALL_KNOWN_SOC_CODES_WITH_TITLES.keys())
                    st.session_state.population_progress["total_to_process"] = len(ALL_KNOWN_SOC_CODES_WITH_TITLES)
                    save_population_progress(st.session_state.population_progress)
                    st.session_state.current_batch_logs = ["Progress has been reset."]
                    logger.info("Population progress reset by admin.")
                    st.rerun()
        
        # Log display area for the current batch
        log_placeholder = st.empty()
        if st.session_state.current_batch_logs:
            log_placeholder.text_area("Current Batch Log", "\n".join(st.session_state.current_batch_logs), height=200, key="batch_log_display", disabled=True)

        if st.session_state.population_running:
            st.warning("Population process is running. Page will auto-refresh. Do not close this browser tab if possible.")
            
            progress_data = st.session_state.population_progress # Ensure we have the latest
            target_soc_list = progress_data.get("all_target_soc_codes", [])
            current_idx = progress_data.get("current_index", 0)
            
            if current_idx >= len(target_soc_list):
                st.success("🎉 All target SOC codes have been processed!")
                logger.info("Population process completed all target SOCs.")
                st.session_state.population_running = False
                progress_data["last_run_timestamp"] = datetime.datetime.now().isoformat()
                save_population_progress(progress_data)
                st.rerun()
            else:
                soc_codes_to_process_this_batch = target_soc_list[current_idx : current_idx + batch_size]
                
                if not soc_codes_to_process_this_batch: # Should not happen if current_idx < len
                     st.info("No more SOCs in the current list to process for this batch.")
                     st.session_state.population_running = False # Stop if list is exhausted
                     save_population_progress(progress_data)
                     st.rerun()
                else:
                    st.session_state.current_batch_logs.append(f"--- Starting Batch at {datetime.datetime.now().strftime('%H:%M:%S')} ---")
                    log_placeholder.text_area("Current Batch Log", "\n".join(st.session_state.current_batch_logs), height=200, key="batch_log_update", disabled=True)

                    for i, soc_code in enumerate(soc_codes_to_process_this_batch):
                        if not st.session_state.population_running: # Check if paused during batch
                            st.session_state.current_batch_logs.append("Population paused during batch.")
                            logger.info("Population paused by admin during batch processing.")
                            break 
                        
                        representative_title = ALL_KNOWN_SOC_CODES_WITH_TITLES.get(soc_code, f"Unknown Title for SOC {soc_code}")
                        log_msg = f"Processing SOC: {soc_code} (Title: '{representative_title}'). Index: {current_idx + i}"
                        st.session_state.current_batch_logs.append(log_msg)
                        logger.info(log_msg)
                        log_placeholder.text_area("Current Batch Log", "\n".join(st.session_state.current_batch_logs), height=200, key=f"batch_log_soc_{i}", disabled=True)
                        
                        try:
                            # get_complete_job_data handles fetching from API if not in DB or stale, and saves to DB.
                            # It uses the job_title to find SOC, then fetches data for that SOC.
                            job_data = bls_job_mapper.get_complete_job_data(representative_title)
                            
                            if job_data and "error" not in job_data:
                                success_msg = f"SUCCESS: Data fetched/updated for SOC {soc_code} ('{job_data.get('job_title', representative_title)}'). Source: {job_data.get('source')}"
                                st.session_state.current_batch_logs.append(success_msg)
                                logger.info(success_msg)
                                if soc_code not in progress_data["successfully_populated_socs"]:
                                     progress_data["successfully_populated_socs"].append(soc_code)
                                if soc_code in progress_data["failed_socs"]:
                                    del progress_data["failed_socs"][soc_code] # Remove from failed if now successful
                            elif job_data and "error" in job_data:
                                error_msg = f"ERROR for SOC {soc_code}: {job_data['error']}"
                                st.session_state.current_batch_logs.append(error_msg)
                                logger.error(error_msg)
                                progress_data["failed_socs"][soc_code] = job_data['error']
                            else:
                                unknown_err_msg = f"UNKNOWN_STATE for SOC {soc_code}: No data and no specific error returned."
                                st.session_state.current_batch_logs.append(unknown_err_msg)
                                logger.error(unknown_err_msg)
                                progress_data["failed_socs"][soc_code] = "Unknown state or no data returned."

                        except Exception as e:
                            exc_error_msg = f"EXCEPTION for SOC {soc_code}: {e}"
                            st.session_state.current_batch_logs.append(exc_error_msg)
                            logger.error(exc_error_msg, exc_info=True)
                            progress_data["failed_socs"][soc_code] = str(e)
                        
                        progress_data["processed_count"] = progress_data.get("processed_count",0) + 1
                        progress_data["current_index"] = current_idx + i + 1
                        save_population_progress(progress_data) # Save after each SOC
                        
                        log_placeholder.text_area("Current Batch Log", "\n".join(st.session_state.current_batch_logs), height=200, key=f"batch_log_soc_done_{i}", disabled=True)
                        time.sleep(api_delay) # Respect API rate limits

                    st.session_state.current_batch_logs.append(f"--- Batch Finished at {datetime.datetime.now().strftime('%H:%M:%S')} ---")
                    if st.session_state.population_running: # If not paused
                        st.rerun() # Rerun to process next batch or show completion
                    else: # If paused, just update the log display
                        log_placeholder.text_area("Current Batch Log", "\n".join(st.session_state.current_batch_logs), height=200, key="batch_log_final_paused", disabled=True)


with tab3:
    st.header("Population Process Logs")
    st.markdown("Displays detailed logs from the database population process.")
    
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r") as f:
                log_content = f.read()
            st.text_area("Log Content", log_content, height=500, disabled=True, key="full_log_display")
            
            # Provide download button for the log file
            st.download_button(
                label="📥 Download Full Log File",
                data=log_content,
                file_name="admin_population_log.txt",
                mime="text/plain"
            )
        except Exception as e:
            st.error(f"Could not read log file: {e}")
    else:
        st.info("Log file does not exist yet. Run the population tool to generate logs.")

    st.markdown("---")
    st.subheader("Failed SOC Codes")
    failed_socs_data = st.session_state.population_progress.get("failed_socs", {})
    if failed_socs_data:
        st.warning(f"Found {len(failed_socs_data)} SOC codes that failed to populate or had errors.")
        failed_df = pd.DataFrame(list(failed_socs_data.items()), columns=['SOC Code', 'Error Message'])
        st.dataframe(failed_df, use_container_width=True)
        
        # Option to retry failed SOCs (simplified: resets them for next run)
        if st.button("Mark Failed SOCs for Retry"):
            if st.session_state.population_running:
                st.warning("Pause population before retrying failed SOCs.")
            else:
                # This logic is simplified: it doesn't specifically re-queue.
                # A more robust retry would involve a separate list or re-inserting them into the processing queue.
                # For now, resetting their 'failed' status and letting the main loop pick them up if progress is reset
                # or if the main list is re-iterated is one way.
                # A better way for targeted retry:
                # 1. Create a list of failed SOCs.
                # 2. When "Retry Failed" is clicked, set `all_target_soc_codes` to this list and reset index.
                # This is more complex to manage with the main `all_target_soc_codes`.
                # Simple approach: just clear failed_socs and let user reset main progress if they want to retry all.
                st.session_state.population_progress["failed_socs"] = {}
                save_population_progress(st.session_state.population_progress)
                st.success("Cleared failed SOCs list. If you reset main progress, they will be attempted again.")
                st.rerun()

    else:
        st.info("No SOC codes are currently marked as failed.")

# General Footer or Info
st.sidebar.markdown("---")
st.sidebar.info(
    "Admin Dashboard v1.0\n\n"
    "Ensure DATABASE_URL and BLS_API_KEY are correctly set in Streamlit secrets for production."
)
if not MODULE_IMPORT_SUCCESS:
    st.sidebar.error("One or more critical modules failed to import. Dashboard functionality will be severely limited.")

