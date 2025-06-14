import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
import datetime
import os
import sys
import threading
import time
import requests # For keep-alive self-ping
import logging
import re
from sqlalchemy import create_engine, text

# --- Logger Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("AI_Job_Analyzer_App")

# --- Keep-Alive Functionality ---
def keep_alive():
    logger.info("Keep-alive thread started.")
    keep_alive_stats = {
        "last_success": None, "consecutive_failures": 0,
        "total_attempts": 0, "successful_attempts": 0
    }
    timestamp_file = "app_last_activity.txt"

    while True:
        try:
            keep_alive_stats["total_attempts"] += 1
            logger.info(f"Keep-alive attempt #{keep_alive_stats['total_attempts']}")
            success_this_cycle = False

            db_url_for_ping = os.environ.get('DATABASE_URL')
            if not db_url_for_ping:
                try:
                    db_url_for_ping = st.secrets.get("database", {}).get("DATABASE_URL")
                except Exception: pass 
            
            if db_url_for_ping:
                try:
                    if db_url_for_ping.startswith("postgres://"):
                        db_url_for_ping = db_url_for_ping.replace("postgres://", "postgresql://", 1)
                    
                    ping_engine_connect_args = {}
                    if 'postgresql' in db_url_for_ping:
                         ping_engine_connect_args = {"connect_timeout": 5, "sslmode": "require"}

                    ping_engine = create_engine(db_url_for_ping, connect_args=ping_engine_connect_args)
                    with ping_engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    logger.info("Keep-alive: Database ping successful.")
                    success_this_cycle = True
                    ping_engine.dispose()
                except Exception as e:
                    logger.warning(f"Keep-alive: Database ping failed: {str(e)}")
            else:
                logger.info("Keep-alive: DATABASE_URL not configured, skipping database ping strategy.")

            try:
                is_streamlit_cloud = "STREAMLIT_SHARING_MODE" in os.environ and os.environ["STREAMLIT_SHARING_MODE"] == "True"
                
                if not is_streamlit_cloud: 
                    streamlit_server_address = os.environ.get('STREAMLIT_SERVER_ADDRESS', 'localhost')
                    streamlit_server_port = os.environ.get('STREAMLIT_SERVER_PORT', '8501')
                    base_url = f"http://{streamlit_server_address}:{streamlit_server_port}"
                    health_url = f"{base_url}/?health_check=true" # Use the simple health_check
                    response = requests.get(health_url, timeout=10)
                    if response.status_code == 200:
                        logger.info(f"Keep-alive: Self HTTP request to {health_url} successful.")
                        success_this_cycle = True
                    else:
                        logger.warning(f"Keep-alive: Self HTTP request to {health_url} failed with status {response.status_code}.")
                else:
                    logger.info("Keep-alive: Skipping self HTTP request on Streamlit Cloud (UptimeRobot is primary).")
            except Exception as e:
                logger.warning(f"Keep-alive: Self HTTP request failed: {str(e)}")

            try:
                with open(timestamp_file, "w") as f:
                    f.write(datetime.datetime.now().isoformat())
                logger.info(f"Keep-alive: File system activity successful (wrote to {timestamp_file}).")
                success_this_cycle = True
            except Exception as e:
                logger.warning(f"Keep-alive: File system activity failed: {str(e)}")

            if success_this_cycle:
                keep_alive_stats["last_success"] = datetime.datetime.now().isoformat()
                keep_alive_stats["consecutive_failures"] = 0
                keep_alive_stats["successful_attempts"] += 1
            else:
                keep_alive_stats["consecutive_failures"] += 1
                logger.error("Keep-alive: All strategies failed in this cycle.")
            
            try:
                with open("app_keep_alive_stats.json", "w") as f:
                    json.dump(keep_alive_stats, f)
            except Exception as e:
                logger.warning(f"Keep-alive: Failed to write stats file: {str(e)}")
            
            time.sleep(240) 

        except Exception as e_outer:
            logger.error(f"Keep-alive: Outer loop error: {str(e_outer)}", exc_info=True)
            keep_alive_stats["consecutive_failures"] += 1
            sleep_time = min(300 * (1 + (keep_alive_stats["consecutive_failures"] * 0.1)), 900) 
            logger.info(f"Keep-alive: Sleeping for {sleep_time:.0f} seconds due to outer loop error before retry.")
            time.sleep(sleep_time)

if "keep_alive_started" not in st.session_state:
    st.session_state.keep_alive_started = True
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    logger.info("Keep-alive thread initiated.")

# --- Module Imports for Application Logic ---
try:
    import job_api_integration_database_only as job_api_integration # For main app
    import simple_comparison
    import career_navigator
    import bls_job_mapper # For admin tool and utility functions
    from bls_job_mapper import TARGET_SOC_CODES # Import the list for admin tool
    from job_title_autocomplete_v2 import job_title_autocomplete
    import database # For user search history, etc.
    logger.info("Core application modules imported successfully.")
except ImportError as e:
    logger.critical(f"Failed to import one or more core application modules: {e}. Application may not function.", exc_info=True)
    st.error(f"Application Error: A critical module failed to load ({e}). Please check the logs. The application might be unstable.")
    st.stop()

# --- Configuration and Global Variables ---
bls_api_key = os.environ.get('BLS_API_KEY')
if not bls_api_key:
    try:
        bls_api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
        if bls_api_key:
            os.environ['BLS_API_KEY'] = bls_api_key
            logger.info("BLS API key loaded from Streamlit secrets.")
    except Exception:
        logger.warning("BLS_API_KEY not found in environment variables or Streamlit secrets.")

engine = None
database_available = False
try:
    engine = bls_job_mapper.get_db_engine()
    database_available = True
    logger.info("Database engine obtained from bls_job_mapper module.")
except ValueError as ve:
    logger.error(f"Database configuration error: {ve}")
    # This error will be shown in the sidebar status
except Exception as e:
    logger.error(f"Failed to obtain database engine from bls_job_mapper: {e}", exc_info=True)
    # This error will be shown in the sidebar status

# --- Health Check Endpoint ---
query_params = st.query_params
if query_params.get("health_check") == "true":
    st.text("OK")
    if query_params.get("detailed") == "true":
        st.markdown("## Detailed Health Status")
        st.markdown(f"- **BLS API Key Configured**: {'Yes' if bls_api_key else 'No'}")
        db_status = "Not Connected"
        if engine:
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                db_status = "Connected"
            except Exception as e:
                db_status = f"Connection Error: {str(e)[:50]}..."
        st.markdown(f"- **Database Connection**: {db_status}")
        
        # Keep-alive stats
        try:
            with open("app_keep_alive_stats.json", "r") as f:
                ka_stats = json.load(f)
                st.markdown("### Keep-Alive Stats")
                st.json(ka_stats)
        except Exception:
            st.markdown("- **Keep-Alive Stats**: Not available")

    st.stop()

# --- Streamlit Page Configuration ---
st.set_page_config(
    page_title="Career AI Impact Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS ---
st.markdown("""
<style>
    .main { background-color: #FFFFFF; }
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { height: 60px; width: 250px; white-space: pre-wrap; background-color: #F0F8FF; border-radius: 4px 4px 0 0; gap: 10px; padding-top: 15px; padding-bottom: 15px; font-size: 18px; font-weight: 600; text-align: center; }
    .stTabs [aria-selected="true"] { background-color: #0084FF; color: white; }
    h1, h2, h3, h4, h5, h6 { color: #0084FF; }
    .job-risk-low { background-color: #d4edda; border-radius: 5px; padding: 10px; margin-bottom: 10px; }
    .job-risk-moderate { background-color: #fff3cd; border-radius: 5px; padding: 10px; margin-bottom: 10px; }
    .job-risk-high { background-color: #f8d7da; border-radius: 5px; padding: 10px; margin-bottom: 10px; }
    .job-risk-very-high { background-color: #f8d7da; border-color: #f5c6cb; border-radius: 5px; padding: 10px; margin-bottom: 10px; border-width: 2px; border-style: solid; }
    .sidebar .sidebar-content { background-color: #f8f9fa; }
    .st-eb { border-radius: 5px; }
    .status-indicator { display: flex; align-items: center; padding: 5px 10px; border-radius: 4px; margin-bottom: 10px; font-size: 14px; }
    .status-indicator.online { background-color: #d4edda; color: #155724; }
    .status-indicator.offline { background-color: #f8d7da; color: #721c24; }
    .status-indicator-dot { height: 10px; width: 10px; border-radius: 50%; margin-right: 8px; }
    .status-indicator-dot.online { background-color: #28a745; }
    .status-indicator-dot.offline { background-color: #dc3545; }
</style>
""", unsafe_allow_html=True)

# --- Sidebar Status Indicators ---
with st.sidebar:
    st.title("System Status")

    # BLS API Key Status
    if bls_api_key:
        st.markdown('<div class="status-indicator online"><span class="status-indicator-dot online"></span>BLS API: Configured</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="status-indicator offline"><span class="status-indicator-dot offline"></span>BLS API: NOT CONFIGURED</div>', unsafe_allow_html=True)

    # Database Status
    db_status_msg = "Database: Not Connected"
    db_status_class = "offline"
    if engine:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            db_status_msg = "Database: Connected"
            db_status_class = "online"
        except Exception as e:
            db_status_msg = f"Database: Connection Error"
            logger.error(f"Sidebar DB Check Error: {e}")
    st.markdown(f'<div class="status-indicator {db_status_class}"><span class="status-indicator-dot {db_status_class}"></span>{db_status_msg}</div>', unsafe_allow_html=True)

    # Data Refresh Status
    data_refresh_status = "Data refresh cycle status unknown." # Placeholder
    st.markdown(f'<div class="status-indicator online"><span class="status-indicator-dot online"></span>{data_refresh_status}</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown(f"App Version: 2.1.0 (Real Data Only)")
    st.markdown(f"Last App Load: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Keep-alive status display
    if "keep_alive_started" in st.session_state:
        try:
            with open("app_keep_alive_stats.json", "r") as f:
                ka_stats = json.load(f)
            last_ping_time_str = ka_stats.get("last_success")
            if last_ping_time_str:
                last_ping_dt = datetime.datetime.fromisoformat(last_ping_time_str)
                time_since_last_ping = (datetime.datetime.now() - last_ping_dt).total_seconds() / 60
                st.markdown(f"Keep-Alive: Active (last ping: {time_since_last_ping:.1f} min ago)")
            else:
                st.markdown("Keep-Alive: Attempting...")
        except Exception:
            st.markdown("Keep-Alive: Status unavailable")
    else:
        st.markdown("Keep-Alive: Initializing...")
    
    st.markdown("---")
    st.markdown("#### UptimeRobot Setup")
    st.markdown("""
    To keep this application alive with UptimeRobot:
    1. Create a new monitor in UptimeRobot
    2. Set Type to "HTTP(s)"
    3. Set URL to your app URL with `?health_check=true` (e.g., `your-app-url.streamlit.app/?health_check=true`)
    4. Set monitoring interval to 5 minutes
    5. Enable "Alert When Down"
    """)

    if not bls_api_key:
        st.error("BLS API Key is not configured. Please set the BLS_API_KEY in Streamlit secrets or environment variables. The application cannot function without it.")

# --- Main Application UI ---
st.image("https://img1.wsimg.com/isteam/ip/70686f32-22d2-489c-a383-6fcd793644be/blob-3712e2e.png/:/rs=h:197,cg:true,m/qt=q:95", width=150) # Smaller logo
st.markdown("<h1 style='text-align: center; color: #0084FF;'>Is your job at risk with AI innovation?</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #4CACE5; font-size: 24px; font-weight: 600;'>AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666666; font-weight: bold; font-size: 16px;'>Discover how AI might impact your career in the next 5 years and get personalized recommendations.</p>", unsafe_allow_html=True)
st.info("📊 This application uses authentic Bureau of Labor Statistics (BLS) data only. No synthetic or fictional data is used.")


# Initialize session state for admin controls
if 'admin_run_batch' not in st.session_state: st.session_state.admin_run_batch = False
if 'admin_batch_size' not in st.session_state: st.session_state.admin_batch_size = 5
if 'admin_api_delay' not in st.session_state: st.session_state.admin_api_delay = 2
if 'admin_processed_count' not in st.session_state: st.session_state.admin_processed_count = 0
if 'current_soc_index' not in st.session_state: st.session_state.current_soc_index = 0
if 'admin_batch_log' not in st.session_state: st.session_state.admin_batch_log = []
if 'admin_total_socs' not in st.session_state: st.session_state.admin_total_socs = len(TARGET_SOC_CODES)
if 'admin_failed_socs' not in st.session_state: st.session_state.admin_failed_socs = []
if 'admin_warning_socs' not in st.session_state: st.session_state.admin_warning_socs = []


# --- Admin Controls Expander (in Sidebar) ---
with st.sidebar.expander("⚙️ ADMIN CONTROLS - Click to Expand", expanded=False):
    st.markdown("This section is for administrators only and provides tools for database management.")
    
    if not bls_api_key:
        st.error("BLS API Key is not configured. Admin database population tool cannot function.")
    elif not engine:
        st.error("Database is not connected. Admin database population tool cannot function.")
    else:
        st.subheader("Simplified Admin: Database Population Tool")
        
        # Progress display
        progress_percent = (st.session_state.admin_processed_count / st.session_state.admin_total_socs) * 100 if st.session_state.admin_total_socs > 0 else 0
        st.progress(int(progress_percent))
        st.markdown(f"Overall Progress: {st.session_state.admin_processed_count} SOCs processed out of {st.session_state.admin_total_socs} target SOCs. Next to process: Index {st.session_state.current_soc_index}.")

        # Batch controls
        st.session_state.admin_batch_size = st.number_input("Batch Size (SOCs per run)", min_value=1, max_value=20, value=st.session_state.admin_batch_size, step=1)
        st.session_state.admin_api_delay = st.number_input("Delay Between API Calls (seconds)", min_value=1, max_value=10, value=st.session_state.admin_api_delay, step=1)

        col_admin1, col_admin2, col_admin3 = st.columns(3)
        with col_admin1:
            if st.button("▶️ Start/Resume Batch", key="admin_start_batch", disabled=st.session_state.admin_run_batch or st.session_state.current_soc_index >= st.session_state.admin_total_socs):
                st.session_state.admin_run_batch = True
                st.session_state.admin_batch_log.append(f"Batch started: {datetime.datetime.now()}")
                st.rerun()
        with col_admin2:
            if st.button("⏸️ Pause (Stop Auto-Run)", key="admin_pause_batch", disabled=not st.session_state.admin_run_batch):
                st.session_state.admin_run_batch = False
                st.session_state.admin_batch_log.append(f"Batch paused: {datetime.datetime.now()}")
                st.rerun()
        with col_admin3:
            if st.button("🔄 Reset All Progress", key="admin_reset_progress"):
                st.session_state.current_soc_index = 0
                st.session_state.admin_processed_count = 0
                st.session_state.admin_run_batch = False
                st.session_state.admin_batch_log = ["Progress Reset."]
                st.session_state.admin_failed_socs = []
                st.session_state.admin_warning_socs = []
                st.rerun()

        # Batch processing logic
        if st.session_state.admin_run_batch and bls_api_key and engine:
            # Determine how many SOCs to process in this run
            end_index = min(st.session_state.current_soc_index + st.session_state.admin_batch_size, st.session_state.admin_total_socs)
            
            # Ensure engine is available
            current_engine = bls_job_mapper.get_db_engine()
            if not current_engine:
                st.session_state.admin_batch_log.append(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - ERROR: Database engine not available. Batch paused.")
                st.session_state.admin_run_batch = False
                st.rerun()

            for i in range(st.session_state.current_soc_index, end_index):
                if not st.session_state.admin_run_batch: break # Allow pausing mid-batch
                
                soc_code_info = TARGET_SOC_CODES[i]
                soc_code = soc_code_info['soc_code']
                soc_title = soc_code_info['title']
                log_message = ""

                st.markdown(f"Processing SOC: {soc_code} ('{soc_title}')")
                logger.info(f"Admin: Processing SOC: {soc_code} ('{soc_title}')")
                st.session_state.admin_batch_log.append(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Processing SOC: {soc_code} ('{soc_title}')")
                
                try:
                    job_category = bls_job_mapper.get_job_category_from_soc(soc_code)
                    processed_data = bls_job_mapper.fetch_and_process_soc_data(
                        soc_code=soc_code,
                        original_job_title=soc_title, 
                        standardized_soc_title=soc_title, 
                        job_category=job_category
                    )

                    if "error" in processed_data: # Check for critical errors from fetch_and_process
                        log_message = f"ERROR SOC {soc_code}: {processed_data['error']}. Source: {processed_data.get('source', 'fetch_process_error')}"
                        logger.error(log_message)
                        st.session_state.admin_failed_socs.append({"soc": soc_code, "title": soc_title, "reason": processed_data['error'], "source": processed_data.get('source')})
                    else:
                        save_success = bls_job_mapper.save_bls_data_to_db(processed_data, current_engine)
                        if save_success:
                            oes_raw_response = json.loads(processed_data.get('raw_oes_data_json', '{}'))
                            ep_raw_response = json.loads(processed_data.get('raw_ep_data_json', '{}'))
                            api_had_errors = False
                            api_error_messages = []

                            # Check BLS API status (REQUEST_SUCCEEDED is good) vs internal 'success'
                            if not (oes_raw_response.get('status', '').upper() == 'REQUEST_SUCCEEDED' and oes_raw_response.get('Results', {}).get('series')):
                                api_had_errors = True
                                api_error_messages.extend(oes_raw_response.get('message', ['OES API fetch issue']))
                            if not (ep_raw_response.get('status', '').upper() == 'REQUEST_SUCCEEDED' and ep_raw_response.get('Results', {}).get('series')):
                                api_had_errors = True
                                api_error_messages.extend(ep_raw_response.get('message', ['EP API fetch issue']))
                            
                            if api_had_errors:
                                unique_api_errors = list(set(filter(None, api_error_messages)))
                                error_summary = "; ".join(unique_api_errors) if unique_api_errors else "Unknown API data issue."
                                log_message = f"PARTIAL SOC {soc_code} ({soc_title}): Saved, but API fetch had issues: {error_summary}"
                                logger.warning(log_message)
                                st.session_state.admin_warning_socs.append({"soc": soc_code, "title": soc_title, "reason": f"API issues: {error_summary}", "source": "api_incomplete_db_saved"})
                            else:
                                log_message = f"SUCCESS SOC {soc_code} ({soc_title}): Data fetched/processed and saved. OES Year: {processed_data.get('oes_data_year', 'N/A')}, EP Base: {processed_data.get('ep_base_year', 'N/A')}"
                                logger.info(log_message)
                            st.session_state.admin_processed_count += 1
                        else:
                            db_save_error_reason = "Database save operation failed. Check bls_job_mapper logs for details."
                            log_message = f"ERROR SOC {soc_code} ({soc_title}): {db_save_error_reason}. API Source: {processed_data.get('source', 'check_api_data')}"
                            logger.error(log_message)
                            st.session_state.admin_failed_socs.append({"soc": soc_code, "title": soc_title, "reason": db_save_error_reason, "source": "db_save_failed"})
                
                except Exception as e:
                    log_message = f"ERROR SOC {soc_code} ({soc_title}): App-level exception - {str(e)}. Source: app_processing_exception"
                    logger.error(log_message, exc_info=True)
                    st.session_state.admin_failed_socs.append({"soc": soc_code, "title": soc_title, "reason": str(e), "source": "app_exception"})
                
                st.session_state.admin_batch_log.append(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {log_message}")
                st.session_state.current_soc_index = i + 1
                time.sleep(st.session_state.admin_api_delay) # Delay between processing each SOC

            if st.session_state.current_soc_index >= st.session_state.admin_total_socs:
                st.session_state.admin_run_batch = False # Batch finished
                st.session_state.admin_batch_log.append(f"Batch completed: {datetime.datetime.now()}")
            
            st.rerun() # Rerun to update progress and log

        # Admin Log Display
        st.subheader("Admin Batch Log")
        log_display_area = st.container(height=200, border=True)
        for entry in reversed(st.session_state.admin_batch_log): # Show newest first
            log_display_area.text(entry)
        
        # Failed SOCs Summary
        st.markdown("---")
        st.subheader("Summary of Failed SOC Populations")
        if st.session_state.admin_failed_socs:
            failed_df = pd.DataFrame(st.session_state.admin_failed_socs)
            st.dataframe(failed_df, use_container_width=True)
        else:
            st.info("No SOC codes are currently marked as having failed population.")

        # Warning SOCs Summary
        st.subheader("Summary of Partially Successful SOC Populations (Warnings)")
        if st.session_state.admin_warning_socs:
            warning_df = pd.DataFrame(st.session_state.admin_warning_socs)
            st.dataframe(warning_df, use_container_width=True)
        else:
            st.info("No SOC codes have warnings from the last batch.")


# --- Main Application Tabs (Single Job Analysis, Job Comparison) ---
# (Code for these tabs remains largely the same as previous versions, using job_api_integration_database_only.py)

# Tab 1: Single Job Analysis
with tabs[0]:
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    if not bls_api_key: # This check is mostly for the admin tool, but good to have a general one
        st.warning("BLS API Key is not configured. Some data might be limited or unavailable if not already in the database.")
    
    st.markdown("Enter any job title to analyze")
    search_job_title = job_title_autocomplete(
        label="Enter your job title",
        key="job_title_search_single",
        placeholder="Start typing to see suggestions...",
        help="Type a job title and select from matching suggestions"
    )
    
    if st.button("🗑️ Clear Entry", key="clear_button_single"):
        st.session_state.job_title_search_single = "" # Clear the specific input
        # To clear results, we might need to set a flag or clear specific session state for results
        if 'single_job_analysis_results' in st.session_state:
            del st.session_state.single_job_analysis_results
        st.rerun()
    
    search_clicked = st.button("Analyze Job Risk", key="analyze_single_job_risk")

    if 'single_job_analysis_results' in st.session_state and not search_clicked:
        # Display cached results if available and button not clicked again
        job_data = st.session_state.single_job_analysis_results
        # (Display logic for job_data - same as below)
        # This part needs to be refactored into a display function to avoid duplication
        # For now, let's assume the main display logic is triggered by search_clicked
        pass

    if search_clicked and search_job_title:
        with st.spinner(f"Analyzing {search_job_title}..."):
            try:
                # Use the database-only integration for the main app
                job_data = job_api_integration.get_job_data(search_job_title)
                st.session_state.single_job_analysis_results = job_data # Cache results
            except Exception as e:
                logger.error(f"Error in get_job_data for '{search_job_title}': {e}", exc_info=True)
                st.error(f"An unexpected error occurred while analyzing '{search_job_title}'. Please try again or contact support.")
                st.stop()

            if job_data.get("error"):
                st.error(f"{job_data['error']}. {job_data.get('message', '')}")
                if job_data.get("source") == "soc_lookup_failed" or "not found in BLS database" in job_data.get("error", ""):
                     st.info("Please use the Admin Controls (in sidebar) to populate data for new job titles if you are an administrator, or contact support.")
                st.stop()

            if database_available:
                try:
                    database.save_job_search(search_job_title, {
                        'year_1_risk': job_data.get('risk_scores', {}).get('year_1', 0),
                        'year_5_risk': job_data.get('risk_scores', {}).get('year_5', 0),
                        'risk_category': job_data.get('risk_category', 'Unknown'),
                        'job_category': job_data.get('job_category', 'Unknown')
                    })
                except Exception as e_db_save:
                    logger.warning(f"Failed to save user search for '{search_job_title}' to JobSearch table: {e_db_save}")


            st.subheader(f"AI Displacement Risk Analysis: {job_data.get('job_title', search_job_title)}")
            job_info_col, risk_gauge_col, risk_factors_col = st.columns([1.2, 1, 1.2]) # Adjusted column widths

            with job_info_col:
                st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Job Information</h3>", unsafe_allow_html=True)
                st.markdown(f"**Occupation Code (SOC):** {job_data.get('occupation_code', 'N/A')}")
                st.markdown(f"**Job Category:** {job_data.get('job_category', 'N/A')}")
                emp_data = job_data.get('projections', {})
                st.markdown(f"**Current Employment (BLS):** {emp_data.get('current_employment', 'N/A'):,}")
                st.markdown(f"**Projected Change (BLS, to {job_data.get('ep_proj_year', 'N/A')}):** {emp_data.get('percent_change', 'N/A')}%")
                st.markdown(f"**Annual Job Openings (BLS):** {emp_data.get('annual_job_openings', 'N/A'):,}")
                st.markdown(f"**Median Annual Wage (BLS, {job_data.get('oes_data_year', 'N/A')}):** ${job_data.get('median_wage', 'N/A'):,}")

            with risk_gauge_col:
                risk_category = job_data.get("risk_category", "Moderate")
                year_5_risk = job_data.get("risk_scores", {}).get("year_5", 50.0)
                st.markdown(f"<h3 style='text-align: center; margin-bottom: 10px;'>Overall AI Risk (5-Yr): {risk_category}</h3>", unsafe_allow_html=True)
                
                gauge_color_map = {"Low": "#4CAF50", "Moderate": "#FFC107", "High": "#FF9800", "Very High": "#F44336"}
                fig_gauge = go.Figure(go.Indicator(
                    mode = "gauge+number", value = year_5_risk,
                    domain = {'x': [0, 1], 'y': [0, 1]}, title = {'text': ""},
                    number = {'suffix': '%', 'font': {'size': 28}},
                    gauge = {'axis': {'range': [0, 100]}, 'bar': {'color': gauge_color_map.get(risk_category, "#6c757d")},
                             'steps': [{'range': [0, 25], 'color': 'lightgreen'}, {'range': [25,50], 'color': 'lightyellow'}, {'range':[50,75], 'color':'lightsalmon'}, {'range':[75,100], 'color':'lightcoral'}]}))
                fig_gauge.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_gauge, use_container_width=True)
                
                col_risk1, col_risk2 = st.columns(2)
                with col_risk1: st.metric("1-Year Risk", f"{job_data.get('risk_scores', {}).get('year_1', 0):.1f}%")
                with col_risk2: st.metric("5-Year Risk", f"{job_data.get('risk_scores', {}).get('year_5', 0):.1f}%")

            with risk_factors_col:
                st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Key Factors</h3>", unsafe_allow_html=True)
                st.markdown("**Risk Factors:**")
                for factor in job_data.get("risk_factors", ["Data not available"]): st.markdown(f"➖ {factor}")
                st.markdown("**Protective Factors:**")
                for factor in job_data.get("protective_factors", ["Data not available"]): st.markdown(f"➕ {factor}")
            
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Key Insights & Analysis</h3>", unsafe_allow_html=True)
            st.markdown(job_data.get("analysis", "Detailed analysis not available."))
            st.markdown(f"**Summary:** {job_data.get('summary', 'Summary not available.')}")

            # Employment Trend Chart
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Employment Trend (BLS Data)</h3>", unsafe_allow_html=True)
            trend_data = job_data.get("trend_data", {})
            if trend_data and trend_data.get("years") and trend_data.get("employment") and any(e is not None for e in trend_data["employment"]):
                df_trend = pd.DataFrame(trend_data)
                fig_trend = px.line(df_trend, x="years", y="employment", title=f"Employment Trend for {job_data.get('job_title', search_job_title)}", labels={'employment': 'Number of Jobs', 'years': 'Year'}, markers=True)
                fig_trend.update_layout(height=350, margin=dict(l=40, r=40, t=60, b=40))
                st.plotly_chart(fig_trend, use_container_width=True)
            else:
                st.info("Employment trend data from BLS is not available or incomplete for this occupation.")

            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown(career_navigator.get_html(), unsafe_allow_html=True) # Call to action

# Tab 2: Job Comparison
with tabs[1]:
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    st.markdown("Compare AI displacement risk and BLS data for multiple jobs. Add up to 5 jobs.")

    if 'compare_jobs_list' not in st.session_state: st.session_state.compare_jobs_list = []

    new_job_to_compare = job_title_autocomplete(
        label="Enter a job title to add to comparison:",
        key="compare_job_input",
        placeholder="Start typing...",
        help="Select a job to add it to the comparison list below."
    )

    if st.button("➕ Add to Comparison", key="add_to_compare_btn") and new_job_to_compare:
        if new_job_to_compare not in st.session_state.compare_jobs_list and len(st.session_state.compare_jobs_list) < 5:
            st.session_state.compare_jobs_list.append(new_job_to_compare)
            st.session_state.compare_job_input = "" # Clear input after adding
            st.rerun()
        elif len(st.session_state.compare_jobs_list) >= 5:
            st.warning("Maximum of 5 jobs can be compared.")
        elif new_job_to_compare in st.session_state.compare_jobs_list:
            st.info(f"'{new_job_to_compare}' is already in the comparison list.")
    
    if st.session_state.compare_jobs_list:
        st.markdown("#### Jobs to Compare:")
        cols = st.columns(len(st.session_state.compare_jobs_list) + 1)
        for idx, job_name in enumerate(st.session_state.compare_jobs_list):
            cols[idx].markdown(job_name)
            if cols[idx].button("➖", key=f"remove_compare_{idx}", help=f"Remove {job_name}"):
                st.session_state.compare_jobs_list.pop(idx)
                st.rerun()
        
        if st.session_state.compare_jobs_list and cols[-1].button("Clear All", key="clear_all_compare"):
            st.session_state.compare_jobs_list = []
            st.rerun()

    if st.session_state.compare_jobs_list:
        with st.spinner("Fetching comparison data..."):
            comparison_job_data = job_api_integration.get_jobs_comparison_data(st.session_state.compare_jobs_list)
        
        if comparison_job_data and not all(job.get("error") for job in comparison_job_data.values()):
            comparison_tabs = st.tabs(["📊 Risk Chart", "📈 Employment Chart", "📋 Detailed Table", "🕸️ Radar Chart", "🔥 Heatmap"])
            
            with comparison_tabs[0]:
                chart = simple_comparison.create_comparison_chart(comparison_job_data)
                if chart: st.plotly_chart(chart, use_container_width=True)
                else: st.info("Not enough data to create risk comparison chart.")
            
            with comparison_tabs[1]:
                emp_chart = simple_comparison.create_employment_comparison(comparison_job_data)
                if emp_chart: st.plotly_chart(emp_chart, use_container_width=True)
                else: st.info("Not enough employment data to create comparison chart.")

            with comparison_tabs[2]:
                df_compare = simple_comparison.create_comparison_table(comparison_job_data)
                if df_compare is not None and not df_compare.empty: st.dataframe(df_compare, use_container_width=True)
                else: st.info("Not enough data for a detailed table.")

            with comparison_tabs[3]:
                radar_chart = simple_comparison.create_radar_chart(comparison_job_data)
                if radar_chart: st.plotly_chart(radar_chart, use_container_width=True)
                else: st.info("Not enough data for radar chart.")
            
            with comparison_tabs[4]:
                heatmap_chart = simple_comparison.create_risk_heatmap(comparison_job_data)
                if heatmap_chart: st.plotly_chart(heatmap_chart, use_container_width=True)
                else: st.info("Not enough data for risk heatmap.")
            
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown(career_navigator.get_html(), unsafe_allow_html=True)
        else:
            st.error("Could not retrieve enough data for comparison. Some selected jobs might not be in the BLS database.")


# --- Footer ---
st.markdown("---")
st.markdown("""
<div style="text-align: center;">
    <p style="font-size: 12px; color: #6c757d;">
        © 2025 iThriveAI - AI Job Displacement Risk Analyzer. All rights reserved. <br>
        Powered by real-time Bureau of Labor Statistics data | 
        <a href="https://www.bls.gov/ooh/" target="_blank">BLS Occupational Outlook Handbook</a>
    </p>
    <p style="font-size: 10px; color: #adb5bd;">
        Created by edolszanowski | 
        <a href="https://www.ithriveai.com" target="_blank">iThriveAI.com</a>
    </p>
</div>
""", unsafe_allow_html=True)

# Status.io Embed
st.markdown("""
<script src="https://cdn.statuspage.io/se-v2.js"></script>
<script>
  var sp = new StatusPage.page({ page : 'g7p2p8m0rz19' });
  sp.load({
    href: 'https://www.streamlitstatus.com/?utm_source=embed',
    borderColor: '#dddddd',
    borderRadius: '4px',
    fontFamily: 'Helvetica, Arial, sans-serif',
    linkColor: '#007bff',
    width: '300px',
    height: '150px',
    position: 'bottom-left',
    target: '_blank',
    backgroundColor: '#ffffff',
    textColor: '#333333',
    labelColor: '#666666',
    barColor: '#007bff'
  });
</script>
""", unsafe_allow_html=True)
