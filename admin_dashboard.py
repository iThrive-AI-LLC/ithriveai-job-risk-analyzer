import streamlit as st
import pandas as pd
import os
import json
import time
import datetime
import logging
from sqlalchemy import create_engine, text, inspect

# --- Module Import and Configuration ---
try:
    import database
    import bls_job_mapper
    import bls_connector
    MODULE_IMPORT_SUCCESS = True
except ImportError as e:
    MODULE_IMPORT_SUCCESS = False
    CRITICAL_ERROR_MESSAGE = f"CRITICAL IMPORT ERROR: Failed to import essential modules: {e}. The Admin Dashboard cannot function. Ensure all required files are present and dependencies are installed."
    logging.basicConfig(level=logging.CRITICAL)
    logging.critical(CRITICAL_ERROR_MESSAGE)

POPULATION_PROGRESS_FILE = "admin_population_progress.json"
LOG_FILE = "admin_population_log.txt"

ALL_KNOWN_SOC_CODES_WITH_TITLES = {}
if MODULE_IMPORT_SUCCESS and hasattr(bls_job_mapper, 'JOB_TITLE_TO_SOC'):
    for title, soc in bls_job_mapper.JOB_TITLE_TO_SOC.items():
        if soc not in ALL_KNOWN_SOC_CODES_WITH_TITLES:
            ALL_KNOWN_SOC_CODES_WITH_TITLES[soc] = title
else:
    ALL_KNOWN_SOC_CODES_WITH_TITLES = {
        "15-1252": "Software Developer",
        "29-1141": "Registered Nurse",
        "13-2011": "Accountant"
    }

# --- Logger Setup ---
logger = logging.getLogger("AdminDashboard")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(LOG_FILE, mode='a')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

# --- Helper Functions (Non-Streamlit) ---
def load_population_progress():
    if os.path.exists(POPULATION_PROGRESS_FILE):
        try:
            with open(POPULATION_PROGRESS_FILE, "r") as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Error loading progress file: {e}")
    return {
        "all_target_soc_codes": list(ALL_KNOWN_SOC_CODES_WITH_TITLES.keys()),
        "current_index": 0, "processed_count": 0,
        "successfully_populated_socs": [], "failed_socs": {},
        "last_run_timestamp": None, "total_to_process": len(ALL_KNOWN_SOC_CODES_WITH_TITLES)
    }

def save_population_progress(progress_data):
    progress_data["last_run_timestamp"] = datetime.datetime.now().isoformat()
    try:
        with open(POPULATION_PROGRESS_FILE, "w") as f:
            json.dump(progress_data, f, indent=4)
    except IOError as e:
        logger.error(f"Error saving progress file: {e}")

def get_database_stats(engine):
    if engine is None:
        return {"error": "Database engine not available."}
    stats = {"total_records": 0, "last_entry_timestamp": None, "table_exists": False}
    try:
        with engine.connect() as conn:
            inspector = inspect(engine)
            if 'bls_job_data' in inspector.get_table_names():
                stats["table_exists"] = True
                stats["total_records"] = conn.execute(text("SELECT COUNT(*) FROM bls_job_data")).scalar_one_or_none() or 0
                columns = [col['name'] for col in inspector.get_columns('bls_job_data')]
                ts_col = 'last_updated_in_db' if 'last_updated_in_db' in columns else 'last_api_fetch' if 'last_api_fetch' in columns else None
                if ts_col:
                    stats["last_entry_timestamp"] = conn.execute(text(f"SELECT MAX({ts_col}) FROM bls_job_data")).scalar_one_or_none() or "N/A"
                else:
                    stats["last_entry_timestamp"] = "Timestamp column not found"
            else:
                stats["error"] = "'bls_job_data' table does not exist."
    except Exception as e:
        logger.error(f"Error getting DB stats: {e}", exc_info=True)
        stats["error"] = str(e)
    return stats

# --- Main Rendering Function ---
def render():
    """Public entry-point to render the Admin Dashboard UI."""
    st.title("⚙️ Admin Dashboard: BLS Data Management")

    if not MODULE_IMPORT_SUCCESS:
        st.error(CRITICAL_ERROR_MESSAGE)
        return

    # Initialize session state inside the render function
    if "population_running" not in st.session_state:
        st.session_state.population_running = False
    if "current_batch_logs" not in st.session_state:
        st.session_state.current_batch_logs = []
    if "population_progress" not in st.session_state:
        st.session_state.population_progress = load_population_progress()

    # Update target SOCs if necessary
    if set(st.session_state.population_progress.get("all_target_soc_codes", [])) != set(ALL_KNOWN_SOC_CODES_WITH_TITLES.keys()):
        logger.info("Updating target SOC codes list.")
        st.session_state.population_progress["all_target_soc_codes"] = list(ALL_KNOWN_SOC_CODES_WITH_TITLES.keys())
        st.session_state.population_progress["total_to_process"] = len(ALL_KNOWN_SOC_CODES_WITH_TITLES)

    engine = database.engine
    tabs = st.tabs(["📊 Dashboard & Stats", "📥 Database Population Tool", "📜 View Logs"])

    with tabs[0]:
        _render_dashboard_tab(engine)
    with tabs[1]:
        _render_population_tool_tab(engine)
    with tabs[2]:
        _render_logs_tab()

def _render_dashboard_tab(engine):
    st.header("Current Database Status")
    if not engine:
        st.error("Database engine not initialized.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("DB Connectivity")
        db_health = database.check_database_health(engine) if hasattr(database, 'check_database_health') else "Unknown"
        if db_health == "OK": st.success("Connected")
        elif db_health == "Not Configured": st.warning("Not Configured")
        else: st.error("Connection Issue")
    
    with col2:
        st.subheader("BLS API")
        api_key = os.environ.get('BLS_API_KEY') or st.secrets.get("api_keys", {}).get("BLS_API_KEY")
        if not api_key: st.error("BLS_API_KEY not set.")
        elif bls_connector.check_api_connectivity(): st.success("API Accessible")
        else: st.error("API Not Accessible")

    with col3:
        st.subheader("Population Progress")
        progress = st.session_state.population_progress
        total = progress.get("total_to_process", 1)
        processed = progress.get("processed_count", 0)
        st.progress(processed / total if total > 0 else 0)
        st.write(f"Processed: {processed} of {total}")
        st.write(f"Success: {len(progress.get('successfully_populated_socs', []))}")
        st.write(f"Failures: {len(progress.get('failed_socs', {}))}")
        if progress.get("last_run_timestamp"):
            st.caption(f"Last run: {datetime.datetime.fromisoformat(progress['last_run_timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")

    st.subheader("`bls_job_data` Table Statistics")
    stats = get_database_stats(engine)
    if "error" in stats:
        st.error(f"DB stats error: {stats['error']}")
    elif not stats.get("table_exists"):
        st.warning("`bls_job_data` table not found.")
    else:
        st.metric("Total Occupations Cached", stats.get("total_records", 0))
        st.metric("Last DB Entry", str(stats.get("last_entry_timestamp", "N/A")))
        
        with engine.connect() as conn:
            cached_socs = set(pd.read_sql(text("SELECT DISTINCT occupation_code FROM bls_job_data"), conn)['occupation_code'])
        target_socs = set(ALL_KNOWN_SOC_CODES_WITH_TITLES.keys())
        missing = target_socs - cached_socs
        
        if missing:
            with st.expander(f"Missing Target SOCs ({len(missing)})"):
                st.write(sorted(list(missing)))
        else:
            st.success("All target SOCs are in the database.")

def _render_population_tool_tab(engine):
    st.header("BLS Data Population Tool")
    st.markdown("Incrementally fetch and store BLS data in your database.")
    if not engine:
        st.error("Database engine not available.")
        return

    progress = st.session_state.population_progress
    st.info(f"Progress: {progress.get('processed_count', 0)} / {progress.get('total_to_process', 0)} SOCs. Next: Index {progress.get('current_index', 0)}.")

    batch_size = st.number_input("Batch Size", 1, 50, 5, 1)
    api_delay = st.number_input("API Delay (s)", 0.5, 10.0, 1.5, 0.1)

    c1, c2, c3 = st.columns(3)
    if c1.button("▶️ Start/Resume", type="primary", disabled=st.session_state.population_running, use_container_width=True):
        st.session_state.population_running = True
        st.session_state.current_batch_logs = []
        logger.info("Population started.")
        st.rerun()
    if c2.button("⏸️ Pause", disabled=not st.session_state.population_running, use_container_width=True):
        st.session_state.population_running = False
        logger.info("Population paused.")
        save_population_progress(st.session_state.population_progress)
        st.rerun()
    if c3.button("🔄 Reset", use_container_width=True):
        if st.session_state.population_running:
            st.warning("Pause before resetting.")
        else:
            st.session_state.population_progress = load_population_progress()
            save_population_progress(st.session_state.population_progress)
            st.session_state.current_batch_logs = ["Progress reset."]
            logger.info("Population progress reset.")
            st.rerun()

    log_placeholder = st.empty()
    if st.session_state.current_batch_logs:
        log_placeholder.text_area("Batch Log", "\n".join(st.session_state.current_batch_logs), height=200, disabled=True)

    if st.session_state.population_running:
        _run_population_batch(engine, batch_size, api_delay, log_placeholder)

def _run_population_batch(engine, batch_size, api_delay, log_placeholder):
    st.warning("Population running...")
    progress = st.session_state.population_progress
    targets = progress.get("all_target_soc_codes", [])
    current_idx = progress.get("current_index", 0)

    if current_idx >= len(targets):
        st.success("🎉 All SOCs processed!")
        st.session_state.population_running = False
        save_population_progress(progress)
        st.rerun()
        return

    batch = targets[current_idx : current_idx + batch_size]
    st.session_state.current_batch_logs.append(f"--- Starting Batch: {datetime.datetime.now():%H:%M:%S} ---")
    
    for i, soc_code in enumerate(batch):
        if not st.session_state.population_running:
            st.session_state.current_batch_logs.append("Paused during batch.")
            logger.info("Population paused during batch.")
            break
        
        title = ALL_KNOWN_SOC_CODES_WITH_TITLES.get(soc_code, f"Unknown Title for {soc_code}")
        log_msg = f"Processing SOC {soc_code} ({title})..."
        st.session_state.current_batch_logs.append(log_msg)
        logger.info(log_msg)
        log_placeholder.text_area("Batch Log", "\n".join(st.session_state.current_batch_logs), height=200, disabled=True)

        try:
            job_data = bls_job_mapper.get_complete_job_data(title)
            if "error" not in job_data:
                msg = f"SUCCESS: Data for SOC {soc_code}."
                progress["successfully_populated_socs"].append(soc_code)
                if soc_code in progress["failed_socs"]: del progress["failed_socs"][soc_code]
            else:
                msg = f"ERROR for SOC {soc_code}: {job_data['error']}"
                progress["failed_socs"][soc_code] = job_data['error']
            st.session_state.current_batch_logs.append(msg)
            logger.info(msg)
        except Exception as e:
            msg = f"EXCEPTION for SOC {soc_code}: {e}"
            st.session_state.current_batch_logs.append(msg)
            logger.error(msg, exc_info=True)
            progress["failed_socs"][soc_code] = str(e)
        
        progress["processed_count"] += 1
        progress["current_index"] += 1
        save_population_progress(progress)
        time.sleep(api_delay)
    
    st.session_state.current_batch_logs.append(f"--- Batch Finished: {datetime.datetime.now():%H:%M:%S} ---")
    if st.session_state.population_running:
        st.rerun()
    else:
        log_placeholder.text_area("Batch Log", "\n".join(st.session_state.current_batch_logs), height=200, disabled=True)

def _render_logs_tab():
    st.header("Population Process Logs")
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r") as f:
                log_content = f.read()
            st.text_area("Log Content", log_content, height=500, disabled=True)
            st.download_button("📥 Download Log File", log_content, "admin_log.txt", "text/plain")
        except Exception as e:
            st.error(f"Could not read log file: {e}")
    else:
        st.info("Log file not found.")

    st.subheader("Failed SOC Codes")
    failed_socs = st.session_state.population_progress.get("failed_socs", {})
    if failed_socs:
        st.warning(f"Found {len(failed_socs)} failed SOCs.")
        df = pd.DataFrame(list(failed_socs.items()), columns=['SOC Code', 'Error'])
        st.dataframe(df, use_container_width=True)
        if st.button("Clear Failed List"):
            st.session_state.population_progress["failed_socs"] = {}
            save_population_progress(st.session_state.population_progress)
            st.success("Cleared failed list. Reset progress to retry.")
            st.rerun()
    else:
        st.info("No failed SOCs.")

# --- Main Execution Guard ---
if __name__ == "__main__":
    st.sidebar.info("Admin Dashboard running as standalone script.")
    render()
