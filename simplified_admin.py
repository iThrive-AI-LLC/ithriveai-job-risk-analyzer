import streamlit as st
import os
import json
import time
import datetime
import logging
from sqlalchemy.exc import SQLAlchemyError

# Attempt to import necessary application modules
try:
    import bls_job_mapper # For get_complete_job_data and the list of known SOCs
    # database.py might not be directly needed here if engine is passed,
    # but bls_job_mapper depends on it.
    MODULE_IMPORT_SUCCESS = True
except ImportError as e:
    MODULE_IMPORT_SUCCESS = False
    # This error will be logged, and the UI function will show a message.
    logging.basicConfig(level=logging.ERROR)
    logging.critical(f"Simplified Admin: CRITICAL IMPORT ERROR: {e}. Admin functions disabled.")

# --- Configuration ---
POPULATION_PROGRESS_FILE_SIMPLIFIED = "simplified_admin_population_progress.json"
LOG_FILE_SIMPLIFIED = "simplified_admin_population_log.txt"

# --- Logger Setup ---
logger = logging.getLogger("SimplifiedAdmin")
if not logger.handlers: # Ensure logger is configured to avoid duplicate handlers on Streamlit reruns
    logger.setLevel(logging.INFO)
    # File handler for persistent logs
    try:
        file_handler = logging.FileHandler(LOG_FILE_SIMPLIFIED, mode='a') # Append mode
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception as e_fh:
        logging.error(f"Simplified Admin: Could not set up file logging: {e_fh}") # Use root logger if module logger fails
    
    # Stream handler for console output (useful for Streamlit logs if running locally)
    stream_handler = logging.StreamHandler()
    stream_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_formatter)
    logger.addHandler(stream_handler)

# --- Helper Functions ---
def _get_all_target_socs_from_mapper():
    """Retrieves the target SOC codes and their representative titles from bls_job_mapper."""
    if MODULE_IMPORT_SUCCESS and hasattr(bls_job_mapper, 'JOB_TITLE_TO_SOC'):
        # Create a dictionary of SOC: Title, ensuring unique SOCs
        soc_map = {}
        for title, soc in bls_job_mapper.JOB_TITLE_TO_SOC.items():
            if soc not in soc_map: # Keep the first title encountered for a SOC
                soc_map[soc] = title
        if soc_map:
            return soc_map
    logger.warning("Simplified Admin: bls_job_mapper.JOB_TITLE_TO_SOC not found or empty. Using a minimal default SOC list.")
    return {
        "15-1252": "Software Developer (Default)",
        "29-1141": "Registered Nurse (Default)"
    }

def _load_population_progress_simplified():
    """Loads population progress from the JSON file."""
    target_soc_map = _get_all_target_socs_from_mapper()
    default_progress = {
        "target_soc_map": target_soc_map, # Store SOC: Title map
        "ordered_soc_keys": list(target_soc_map.keys()), # Maintain an order for processing
        "current_index": 0,
        "processed_this_session": 0, # Count for the current UI session/run
        "total_processed_ever": 0, # Overall count from file
        "successfully_populated_socs": [],
        "failed_socs": {}, # Store SOC: error_message
        "last_run_timestamp": None,
        "total_target_socs": len(target_soc_map)
    }
    if os.path.exists(POPULATION_PROGRESS_FILE_SIMPLIFIED):
        try:
            with open(POPULATION_PROGRESS_FILE_SIMPLIFIED, "r") as f:
                loaded_data = json.load(f)
                # Validate and merge, ensuring target_soc_map is updated if bls_job_mapper changed
                if loaded_data.get("target_soc_map") != target_soc_map:
                    logger.info("Simplified Admin: Target SOC map changed. Resetting progress index and counts for consistency.")
                    loaded_data["target_soc_map"] = target_soc_map
                    loaded_data["ordered_soc_keys"] = list(target_soc_map.keys())
                    loaded_data["total_target_socs"] = len(target_soc_map)
                    loaded_data["current_index"] = 0 # Reset index if map changes
                    loaded_data["total_processed_ever"] = 0 # Reset overall count
                    loaded_data["successfully_populated_socs"] = []
                    loaded_data["failed_socs"] = {}
                # Ensure all keys from default_progress are present
                for key, value in default_progress.items():
                    if key not in loaded_data:
                        loaded_data[key] = value
                return loaded_data
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Simplified Admin: Error loading progress file '{POPULATION_PROGRESS_FILE_SIMPLIFIED}': {e}. Using default.")
    return default_progress

def _save_population_progress_simplified(progress_data):
    """Saves population progress to the JSON file."""
    progress_data["last_run_timestamp"] = datetime.datetime.now().isoformat()
    try:
        with open(POPULATION_PROGRESS_FILE_SIMPLIFIED, "w") as f:
            json.dump(progress_data, f, indent=4)
        logger.info(f"Simplified Admin: Saved population progress to {POPULATION_PROGRESS_FILE_SIMPLIFIED}")
    except IOError as e:
        logger.error(f"Simplified Admin: Error saving progress file '{POPULATION_PROGRESS_FILE_SIMPLIFIED}': {e}")
        if st._is_running_with_streamlit: # Check if in Streamlit context before using st
             st.warning(f"Could not save population progress: {e}")


def _process_one_soc(soc_code: str, representative_title: str, engine, current_logs_list: list):
    """Processes a single SOC code: fetches data and updates logs."""
    log_msg_start = f"Processing SOC: {soc_code} (Rep. Title: '{representative_title}')"
    current_logs_list.append(log_msg_start)
    logger.info(log_msg_start)
    
    if not MODULE_IMPORT_SUCCESS or not hasattr(bls_job_mapper, 'get_complete_job_data'):
        error_msg = f"CRITICAL_ERROR for SOC {soc_code}: bls_job_mapper.get_complete_job_data is not available."
        current_logs_list.append(error_msg)
        logger.error(error_msg)
        return False, error_msg # Indicate failure

    try:
        # bls_job_mapper.get_complete_job_data is expected to handle DB saving internally if data is fetched from API
        job_data = bls_job_mapper.get_complete_job_data(representative_title) # Pass title, it resolves SOC
        
        if job_data and "error" not in job_data:
            # Verify that the returned SOC matches the one we intended to process, if possible
            returned_soc = job_data.get("occupation_code")
            if returned_soc != soc_code and returned_soc != "00-0000": # Allow 00-0000 as a generic non-match
                warn_msg = f"WARNING for SOC {soc_code}: bls_job_mapper returned data for SOC {returned_soc} when processing '{representative_title}'."
                current_logs_list.append(warn_msg)
                logger.warning(warn_msg)
            
            success_msg = f"SUCCESS: Data fetched/updated for SOC {soc_code} ('{job_data.get('job_title', representative_title)}'). Source: {job_data.get('source', 'N/A')}"
            current_logs_list.append(success_msg)
            logger.info(success_msg)
            return True, None # Indicate success
            
        elif job_data and "error" in job_data:
            error_msg = f"API/DB_ERROR for SOC {soc_code}: {job_data['error']}"
            current_logs_list.append(error_msg)
            logger.error(error_msg)
            return False, job_data['error'] # Indicate failure
            
        else: # Should not happen if job_data is always a dict
            unknown_err_msg = f"UNKNOWN_STATE for SOC {soc_code}: No data and no specific error returned from bls_job_mapper."
            current_logs_list.append(unknown_err_msg)
            logger.error(unknown_err_msg)
            return False, "Unknown state or no data returned."

    except Exception as e:
        exc_error_msg = f"EXCEPTION during processing of SOC {soc_code}: {e}"
        current_logs_list.append(exc_error_msg)
        logger.error(exc_error_msg, exc_info=True)
        return False, str(e) # Indicate failure

# --- Main UI Function to be called from app.py ---
def display_admin_controls(engine_instance):
    """
    Displays admin controls for database population within the main Streamlit app.
    Args:
        engine_instance: The SQLAlchemy engine instance from the main app.
    """
    if not MODULE_IMPORT_SUCCESS:
        st.error("Simplified Admin Panel: Essential modules (like bls_job_mapper) could not be imported. Database population functionality is disabled. Please check application logs.")
        return

    st.subheader("Simplified Admin: Database Population Tool")
    
    if engine_instance is None:
        st.error("Database engine is not available. Cannot run population tool.")
        logger.error("Simplified Admin: display_admin_controls called with no engine_instance.")
        return

    # Initialize session state variables if they don't exist
    if "s_admin_population_running" not in st.session_state:
        st.session_state.s_admin_population_running = False
    if "s_admin_current_batch_logs" not in st.session_state:
        st.session_state.s_admin_current_batch_logs = []
    if "s_admin_population_progress" not in st.session_state:
        st.session_state.s_admin_population_progress = _load_population_progress_simplified()

    progress_data = st.session_state.s_admin_population_progress
    total_target_socs = progress_data.get("total_target_socs", 0)
    current_idx = progress_data.get("current_index", 0)
    total_processed_ever = progress_data.get("total_processed_ever", 0)

    st.info(f"Overall Progress: {total_processed_ever} SOCs processed out of {total_target_socs} target SOCs. Next to process: Index {current_idx}.")
    
    # Progress bar for overall progress
    if total_target_socs > 0:
        st.progress(total_processed_ever / total_target_socs)
    else:
        st.info("No target SOCs defined for population (list might be empty or bls_job_mapper not loaded).")


    batch_size = st.number_input("Batch Size (SOCs per run)", min_value=1, max_value=20, value=3, step=1,
                                 key="s_admin_batch_size", help="Number of SOCs to process in one click of 'Start/Resume'. Small batches are recommended for web environments.")
    api_delay = st.number_input("Delay Between API Calls (seconds)", min_value=0.5, max_value=5.0, value=1.0, step=0.1,
                                key="s_admin_api_delay", help="Time to wait between fetching data for each SOC to respect API rate limits.")

    col_run, col_pause, col_reset = st.columns(3)
    with col_run:
        if st.button("▶️ Start/Resume Batch", type="primary", disabled=st.session_state.s_admin_population_running, use_container_width=True):
            st.session_state.s_admin_population_running = True
            st.session_state.s_admin_current_batch_logs = [f"--- New Batch Started at {datetime.datetime.now().strftime('%H:%M:%S')} ---"]
            logger.info("Simplified Admin: Population batch started by admin.")
            st.rerun() 
    with col_pause: # This button primarily serves to stop a continuous run if implemented, or as a visual cue
        if st.button("⏸️ Pause (Stop Auto-Run)", disabled=not st.session_state.s_admin_population_running, use_container_width=True):
            st.session_state.s_admin_population_running = False
            logger.info("Simplified Admin: Population process marked as paused by admin.")
            _save_population_progress_simplified(st.session_state.s_admin_population_progress) # Save current state
            st.rerun()
    with col_reset:
        if st.button("🔄 Reset All Progress", use_container_width=True, help="Resets all population progress. Does not delete DB data."):
            if st.session_state.s_admin_population_running:
                st.warning("Please pause the population process before resetting.")
            else:
                st.session_state.s_admin_population_progress = _load_population_progress_simplified() # Reloads defaults, including new SOC map
                # Explicitly reset counts and lists
                st.session_state.s_admin_population_progress["current_index"] = 0
                st.session_state.s_admin_population_progress["total_processed_ever"] = 0
                st.session_state.s_admin_population_progress["processed_this_session"] = 0
                st.session_state.s_admin_population_progress["successfully_populated_socs"] = []
                st.session_state.s_admin_population_progress["failed_socs"] = {}
                
                _save_population_progress_simplified(st.session_state.s_admin_population_progress)
                st.session_state.s_admin_current_batch_logs = ["Admin: Population progress has been reset."]
                logger.info("Simplified Admin: Population progress reset by admin.")
                st.rerun()
    
    log_placeholder = st.empty()
    if st.session_state.s_admin_current_batch_logs:
        log_placeholder.text_area("Current Batch Log", "\n".join(st.session_state.s_admin_current_batch_logs), height=200, key="s_admin_batch_log_display", disabled=True)

    if st.session_state.s_admin_population_running:
        # This block will execute if "Start/Resume Batch" was clicked
        progress_data = st.session_state.s_admin_population_progress
        ordered_soc_keys = progress_data.get("ordered_soc_keys", [])
        current_idx = progress_data.get("current_index", 0)
        
        if current_idx >= len(ordered_soc_keys):
            st.success("🎉 All target SOC codes have been processed!")
            logger.info("Simplified Admin: Population process completed all target SOCs.")
            st.session_state.s_admin_population_running = False # Stop running
            progress_data["last_run_timestamp"] = datetime.datetime.now().isoformat()
            _save_population_progress_simplified(progress_data)
            st.rerun() # Update UI to reflect completion
        else:
            soc_codes_for_this_run = ordered_soc_keys[current_idx : current_idx + batch_size]
            
            if not soc_codes_for_this_run:
                 st.info("No more SOCs in the current list to process for this batch run.")
                 st.session_state.s_admin_population_running = False
                 _save_population_progress_simplified(progress_data)
                 st.rerun()
            else:
                processed_in_this_batch = 0
                for i, soc_code_to_process in enumerate(soc_codes_for_this_run):
                    # Get representative title for this SOC
                    rep_title = progress_data["target_soc_map"].get(soc_code_to_process, f"Unknown for {soc_code_to_process}")
                    
                    is_success, error_detail = _process_one_soc(soc_code_to_process, rep_title, engine_instance, st.session_state.s_admin_current_batch_logs)
                    
                    progress_data["total_processed_ever"] = progress_data.get("total_processed_ever", 0) + 1
                    progress_data["current_index"] = current_idx + i + 1 # Update index after processing
                    
                    if is_success:
                        if soc_code_to_process not in progress_data["successfully_populated_socs"]:
                             progress_data["successfully_populated_socs"].append(soc_code_to_process)
                        if soc_code_to_process in progress_data["failed_socs"]:
                            del progress_data["failed_socs"][soc_code_to_process]
                    else:
                        progress_data["failed_socs"][soc_code_to_process] = error_detail or "Processing failed"
                    
                    processed_in_this_batch += 1
                    
                    # Update log display immediately
                    log_placeholder.text_area("Current Batch Log", "\n".join(st.session_state.s_admin_current_batch_logs), height=200, key=f"s_admin_log_update_{i}", disabled=True)
                    
                    if i < len(soc_codes_for_this_run) - 1: # If not the last item in this specific run's batch
                        time.sleep(api_delay) # Delay between SOC processing calls
                
                st.session_state.s_admin_current_batch_logs.append(f"--- Batch of {processed_in_this_batch} SOC(s) Finished at {datetime.datetime.now().strftime('%H:%M:%S')} ---")
                st.session_state.s_admin_population_running = False # Stop after one batch, user clicks to continue
                _save_population_progress_simplified(progress_data)
                st.rerun() # Rerun to update UI and allow next batch click

    st.markdown("---")
    st.subheader("Summary of Failed SOC Populations")
    failed_socs_data = st.session_state.s_admin_population_progress.get("failed_socs", {})
    if failed_socs_data:
        st.warning(f"Found {len(failed_socs_data)} SOC codes that previously failed or had errors.")
        # Display as a simple list for brevity in simplified admin
        for soc, err in failed_socs_data.items():
            st.text(f"- {soc}: {err}")
    else:
        st.info("No SOC codes are currently marked as having failed population.")

# Example of how to use this in app.py:
# import simplified_admin
#
# # In your main app.py, perhaps in a sidebar or an expander:
# st.sidebar.title("Admin Panel")
# with st.sidebar.expander("Database Population Controls"):
#     # Assuming `engine` is your SQLAlchemy engine instance from database.py
#     # engine = database.get_db_engine() # Or however you get it
#     if 'engine' in locals() or 'engine' in globals(): # Check if engine is defined
#          simplified_admin.display_admin_controls(engine)
#     else:
#          st.error("Database engine not initialized in main app. Admin panel cannot load.")
