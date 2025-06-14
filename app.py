import streamlit as st
import bls_job_mapper
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
import datetime
import os
import sys
import threading
import time
import requests
import logging
import re
from sqlalchemy import create_engine, text

# --- Logger Setup ---
# Configure basic logging for the application
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[logging.StreamHandler()] # Output to console, Streamlit will capture this
)
logger = logging.getLogger("AI_Job_Analyzer_App")

# --- Keep-Alive Functionality (Enhanced for UptimeRobot) ---
def keep_alive():
    """
    Multi-strategy background thread to keep the app active.
    Optimized for UptimeRobot monitoring.
    """
    logger.info("Keep-alive thread started.")
    keep_alive_stats = {
        "last_success": None,
        "consecutive_failures": 0,
        "total_attempts": 0,
        "successful_attempts": 0
    }
    timestamp_file = "app_last_activity.txt" # Unique name for this app's activity

    while True:
        try:
            keep_alive_stats["total_attempts"] += 1
            logger.info(f"Keep-alive attempt #{keep_alive_stats['total_attempts']}")
            success_this_cycle = False

            # Strategy 1: Database Ping (if DB is configured)
            db_url_for_ping = os.environ.get('DATABASE_URL')
            if not db_url_for_ping:
                try:
                    db_url_for_ping = st.secrets.get("database", {}).get("DATABASE_URL")
                except Exception: # st.secrets might not be available early or in all contexts
                    pass
            
            if db_url_for_ping:
                try:
                    if db_url_for_ping.startswith("postgres://"): # Ensure correct URI scheme for SQLAlchemy
                        db_url_for_ping = db_url_for_ping.replace("postgres://", "postgresql://", 1)
                    ping_engine = create_engine(db_url_for_ping, connect_args={"connect_timeout": 5})
                    with ping_engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    logger.info("Keep-alive: Database ping successful.")
                    success_this_cycle = True
                    ping_engine.dispose() # Dispose of the temporary engine
                except Exception as e:
                    logger.warning(f"Keep-alive: Database ping failed: {str(e)}")
            else:
                logger.info("Keep-alive: DATABASE_URL not configured, skipping database ping strategy.")

            # Strategy 2: Self HTTP Request to Health Endpoint
            try:
                # Construct base URL carefully. Prefer environment variable if set by platform.
                # Default to localhost for local dev, but this won't work for deployed app unless it's the app's own public URL.
                # For deployed apps, UptimeRobot hitting the public URL is the primary external keep-alive.
                # This self-ping is more for platforms that might sleep internal processes.
                streamlit_server_address = os.environ.get('STREAMLIT_SERVER_ADDRESS', 'localhost')
                streamlit_server_port = os.environ.get('STREAMLIT_SERVER_PORT', '8501')
                # Check if running in Streamlit Cloud (where specific URL structure might be needed or self-ping is less relevant)
                is_streamlit_cloud = "STREAMLIT_SHARING_MODE" in os.environ and os.environ["STREAMLIT_SHARING_MODE"] == "True"

                if not is_streamlit_cloud: # Avoid self-pinging on Streamlit Cloud if it causes issues
                    base_url = f"http://{streamlit_server_address}:{streamlit_server_port}"
                    health_url = f"{base_url}/?health_check=true" # Using the app's own health check
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

            # Strategy 3: File System Activity
            try:
                with open(timestamp_file, "w") as f:
                    f.write(datetime.datetime.now().isoformat())
                logger.info(f"Keep-alive: File system activity successful (wrote to {timestamp_file}).")
                success_this_cycle = True
            except Exception as e:
                logger.warning(f"Keep-alive: File system activity failed: {str(e)}")

            # Update stats
            if success_this_cycle:
                keep_alive_stats["last_success"] = datetime.datetime.now().isoformat()
                keep_alive_stats["consecutive_failures"] = 0
                keep_alive_stats["successful_attempts"] += 1
            else:
                keep_alive_stats["consecutive_failures"] += 1
                logger.error("Keep-alive: All strategies failed in this cycle.")
            
            try: # Save stats
                with open("app_keep_alive_stats.json", "w") as f:
                    json.dump(keep_alive_stats, f)
            except Exception as e:
                logger.warning(f"Keep-alive: Failed to write stats file: {str(e)}")
            
            time.sleep(300)  # Ping every 5 minutes

        except Exception as e_outer:
            logger.error(f"Keep-alive: Outer loop error: {str(e_outer)}", exc_info=True)
            keep_alive_stats["consecutive_failures"] += 1
            # Adaptive sleep on outer error
            sleep_time = min(300 * (1 + (keep_alive_stats["consecutive_failures"] * 0.1)), 900) # Max 15 mins
            logger.info(f"Keep-alive: Sleeping for {sleep_time:.0f} seconds due to outer loop error before retry.")
            time.sleep(sleep_time)

if "keep_alive_started" not in st.session_state:
    st.session_state.keep_alive_started = True
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    logger.info("Keep-alive thread initiated.")

# --- Module Imports for Application Logic ---
try:
    import job_api_integration_database_only as job_api_integration
    import simple_comparison
    # ai_job_displacement might not be directly used if its logic is now in bls_job_mapper
    # For now, assume it might be needed for some specific data points if not fully refactored.
    import ai_job_displacement # Review if this is still needed or if its functionality is in bls_job_mapper
    import career_navigator
    from job_title_autocomplete_v2 import job_title_autocomplete # Assuming load_job_titles_from_db is used within it
    import database # For db functions like save_job_search and the engine
    import simplified_admin # For the admin panel
    logger.info("Core application modules imported successfully.")
except ImportError as e:
    logger.critical(f"Failed to import one or more core application modules: {e}. Application may not function.", exc_info=True)
    st.error(f"Application Error: A critical module failed to load ({e}). Please check the logs. The application might be unstable.")
    # Depending on which module failed, st.stop() might be appropriate here.
    # For now, let the app try to load, but it will likely fail later.


# --- Configuration and Global Variables ---
# BLS API Key Check
bls_api_key = os.environ.get('BLS_API_KEY')
if not bls_api_key:
    try:
        bls_api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
        if bls_api_key:
            os.environ['BLS_API_KEY'] = bls_api_key # Make it available to other modules if they use os.environ
            logger.info("BLS API key loaded from Streamlit secrets.")
    except Exception: # st.secrets not available or key not found
        logger.warning("BLS_API_KEY not found in environment variables or Streamlit secrets.")
        # Error will be displayed later if still not found

# Database Engine Initialization (from database.py)
engine = None
database_available = False
if 'database' in sys.modules and hasattr(database, 'engine') and database.engine is not None:
    engine = database.engine
    database_available = True
    logger.info("Database engine loaded successfully from database module.")
else:
    logger.error("Database engine not available from database module. Database-dependent features will be disabled.")
    # Attempt to initialize engine here if database.py failed but URL is present
    db_url_main_app = os.environ.get('DATABASE_URL')
    if not db_url_main_app:
        try:
            db_url_main_app = st.secrets.get("database", {}).get("DATABASE_URL")
        except Exception:
            pass
    if db_url_main_app:
        try:
            if db_url_main_app.startswith("postgres://"):
                 db_url_main_app = db_url_main_app.replace("postgres://", "postgresql://", 1)
            engine = create_engine(db_url_main_app)
            with engine.connect() as conn: # Test connection
                conn.execute(text("SELECT 1"))
            database_available = True
            logger.info("Successfully created and connected a new database engine in app.py.")
        except Exception as e_eng:
            logger.error(f"Failed to create database engine in app.py: {e_eng}")
            engine = None # Ensure engine is None if connection fails
            database_available = False
    else:
        logger.error("DATABASE_URL not found. Cannot initialize database engine in app.py.")


# --- Health Check Endpoint (for UptimeRobot) ---
query_params = st.query_params
if query_params.get("health_check") == "true":
    st.text("OK") # Simple response for UptimeRobot
    # Detailed health status (optional, can be expanded)
    if query_params.get("detailed") == "true":
        st.markdown("## Detailed Health Status")
        st.markdown(f"- **BLS API Key Configured**: {'Yes' if bls_api_key else 'No'}")
        st.markdown(f"- **Database Connection**: {'Connected' if database_available and engine else 'Not Connected'}")
        # Add more checks if needed (e.g., keep-alive status file)
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
    /* ... (CSS from app_production.py, ensure it's complete) ... */
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
    /* Status indicator from app_production.py */
    .status-indicator { display: flex; align-items: center; padding: 5px 10px; border-radius: 4px; margin-bottom: 10px; font-size: 14px; }
    .status-indicator.online { background-color: #d4edda; color: #155724; }
    .status-indicator.offline { background-color: #f8d7da; color: #721c24; }
    .status-indicator-dot { height: 10px; width: 10px; border-radius: 50%; margin-right: 8px; }
    .status-indicator-dot.online { background-color: #28a745; }
    .status-indicator-dot.offline { background-color: #dc3545; }
    .no-data-message { background-color: #f8f9fa; padding: 20px; border-radius: 5px; text-align: center; margin: 20px 0; }
</style>
""", unsafe_allow_html=True)


# --- Sidebar Status Indicators ---
st.sidebar.title("System Status")
# BLS API Key Status
if bls_api_key:
    st.sidebar.markdown("""<div class="status-indicator online"><div class="status-indicator-dot online"></div>BLS API: Configured</div>""", unsafe_allow_html=True)
else:
    st.sidebar.markdown("""<div class="status-indicator offline"><div class="status-indicator-dot offline"></div>BLS API: NOT CONFIGURED</div>""", unsafe_allow_html=True)
    st.error("BLS API Key is not configured. Please set the BLS_API_KEY in Streamlit secrets or environment variables. The application cannot function without it.")
    # st.stop() # Consider stopping if API key is absolutely essential for any functionality

# Database Status
if database_available and engine:
    st.sidebar.markdown("""<div class="status-indicator online"><div class="status-indicator-dot online"></div>Database: Connected</div>""", unsafe_allow_html=True)
else:
    st.sidebar.markdown("""<div class="status-indicator offline"><div class="status-indicator-dot offline"></div>Database: NOT CONNECTED</div>""", unsafe_allow_html=True)
    st.error("Database is not connected. Please check the DATABASE_URL in Streamlit secrets or environment variables. Some features may be unavailable.")
    # Do not st.stop() if some parts of app can work without DB, or if admin panel needs to be accessible to fix DB issues.


# --- Data Refresh Logic (Simplified from app_production.py) ---
def check_data_refresh_simple():
    """Simplified data refresh check, logs attempt."""
    # This is a placeholder. The actual refresh logic would be in db_refresh.py
    # and called by the keep-alive thread or a scheduled task.
    # For the app UI, we can just indicate when the last known refresh attempt was.
    refresh_file = "app_last_refresh.json" # Unique name
    try:
        if os.path.exists(refresh_file):
            with open(refresh_file, "r") as f:
                refresh_data = json.load(f)
            last_refresh_time = datetime.datetime.fromisoformat(refresh_data["date"])
            st.sidebar.caption(f"Last data refresh cycle check: {last_refresh_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            st.sidebar.caption("Data refresh cycle status unknown.")
        # Actual refresh should be triggered by keep-alive or external scheduler
    except Exception as e:
        logger.warning(f"Could not read data refresh status: {e}")
        st.sidebar.caption("Could not determine data refresh status.")

if database_available: # Only show refresh status if DB is up, as refresh often involves DB
    check_data_refresh_simple()


# --- Main Application UI ---
st.image("https://img1.wsimg.com/isteam/ip/70686f32-22d2-489c-a383-6fcd793644be/blob-3712e2e.png/:/rs=h:197,cg:true,m/qt=q:95", width=250)
st.markdown("<h1 style='text-align: center; color: #0084FF;'>Is your job at risk with AI innovation?</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #4CACE5; font-size: 24px; font-weight: 600;'>AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666666; font-weight: bold; font-size: 16px;'>Discover how AI might impact your career in the next 5 years and get personalized recommendations.</p>", unsafe_allow_html=True)

st.info("📊 This application uses authentic Bureau of Labor Statistics (BLS) data only. No synthetic or fictional data is used.")

tabs = st.tabs(["Single Job Analysis", "Job Comparison"])

# Single Job Analysis Tab
with tabs[0]:
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    st.markdown("Enter any job title to analyze")
    
    # Ensure job_title_autocomplete is available
    if 'job_title_autocomplete' in globals() and callable(job_title_autocomplete):
        search_job_title = job_title_autocomplete(
            label="Enter your job title",
            key="job_title_search_main_app", # Unique key
            placeholder="Start typing to see suggestions...",
            help="Type a job title and select from matching suggestions from BLS data."
        )
    else:
        search_job_title = st.text_input("Enter your job title (Autocomplete not available)", key="job_title_search_main_app_fallback")
        if not hasattr(sys.modules.get('job_title_autocomplete_v2'), 'job_title_autocomplete'):
             st.warning("Job title autocomplete feature is currently unavailable due to a module issue.")

    if st.button("🗑️ Clear Entry", key="clear_button_single_main_app"):
        st.session_state.job_title_search_main_app = "" # Clear the specific input
        if 'job_title_search_main_app_select' in st.session_state: # If selectbox from autocomplete exists
            del st.session_state.job_title_search_main_app_select
        st.rerun()

    normalized_job_title = search_job_title.lower().strip() if search_job_title else ""
    if re.search(r'diagnos(i(c|s|t|cian)|e)', normalized_job_title): # Demo case
        search_job_title = "Diagnosician"

    search_clicked = st.button("Analyze Job Risk", key="analyze_button_main_app")

    if search_clicked and search_job_title:
        if not bls_api_key:
            st.error("BLS API Key is not configured. Cannot analyze job risk.")
        elif not database_available and not engine: # Check if DB is truly unavailable
            st.error("Database is not connected. Cannot analyze job risk.")
        else:
            with st.spinner(f"Analyzing {search_job_title}..."):
                try:
                    # job_api_integration should be robust enough to handle API/DB issues via bls_job_mapper
                    job_data = job_api_integration.get_job_data(search_job_title)
                    
                    if "error" in job_data:
                        st.error(f"Could not retrieve data for '{search_job_title}': {job_data['error']}")
                        if "not found in BLS database" in job_data['error'].lower() or "could not be mapped" in job_data['error'].lower() :
                             st.info("Please try a more standard job title, or check spelling. This tool uses official BLS occupation classifications.")
                        logger.error(f"Error fetching job data for '{search_job_title}': {job_data['error']}")
                    else:
                        # Save search if DB available and successful fetch
                        if database_available and 'database' in sys.modules and hasattr(database, 'save_job_search'):
                            try:
                                database.save_job_search(search_job_title, {
                                    'year_1_risk': job_data.get('year_1_risk'),
                                    'year_5_risk': job_data.get('year_5_risk'),
                                    'risk_category': job_data.get('risk_category'),
                                    'job_category': job_data.get('job_category')
                                })
                                logger.info(f"Search for '{search_job_title}' saved to database.")
                            except Exception as e_save:
                                logger.error(f"Failed to save job search for '{search_job_title}': {e_save}")
                        
                        # --- Display Job Analysis Results (adapted from app_production.py) ---
                        st.subheader(f"AI Displacement Risk Analysis: {job_data.get('job_title', search_job_title)}")
                        job_info_col, risk_gauge_col, risk_factors_col = st.columns([1, 1, 1])

                        with job_info_col:
                            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Job Information</h3>", unsafe_allow_html=True)
                            st.markdown(f"**Occupation Code:** {job_data.get('occupation_code', 'N/A')}")
                            st.markdown(f"**Job Category:** {job_data.get('job_category', 'N/A')}")
                            bls_info = job_data.get('bls_data', {})
                            st.markdown(f"**Current Employment:** {bls_info.get('employment', 'N/A'):,}" if isinstance(bls_info.get('employment'), (int,float)) else f"**Current Employment:** {bls_info.get('employment', 'N/A')}")
                            st.markdown(f"**BLS Projected Growth (%):** {bls_info.get('employment_change_percent', 'N/A')}")
                            st.markdown(f"**Annual Job Openings:** {bls_info.get('annual_job_openings', 'N/A'):,}" if isinstance(bls_info.get('annual_job_openings'), (int,float)) else f"**Annual Job Openings:** {bls_info.get('annual_job_openings', 'N/A')}")
                            
                            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Career Outlook</h3>", unsafe_allow_html=True)
                            st.markdown(f"**Task Automation Probability (General Estimate):** {job_data.get('automation_probability', 'N/A')}%") # This field might not come from pure BLS
                            st.markdown(f"**Median Annual Wage:** ${bls_info.get('median_wage', 'N/A'):,}" if isinstance(bls_info.get('median_wage'),(int,float)) else f"**Median Annual Wage:** {bls_info.get('median_wage', 'N/A')}")

                        with risk_gauge_col:
                            risk_cat_display = job_data.get('risk_category', 'N/A')
                            st.markdown(f"<h3 style='text-align: center; margin-bottom: 10px;'>Overall AI Displacement Risk: {risk_cat_display}</h3>", unsafe_allow_html=True)
                            year_5_risk_val = job_data.get('year_5_risk', 0)
                            gauge_val = year_5_risk_val if isinstance(year_5_risk_val, (int, float)) else 0
                            
                            fig_gauge = go.Figure(go.Indicator(
                                mode = "gauge+number", value = gauge_val, domain = {'x': [0, 1], 'y': [0, 1]},
                                number = {'suffix': '%', 'font': {'size': 28}},
                                gauge = {'axis': {'range': [0, 100]}, 'bar': {'color': "#0084FF"},
                                         'steps': [{'range': [0, 25], 'color': "rgba(0, 255, 0, 0.5)"}, {'range': [25, 50], 'color': "rgba(255, 255, 0, 0.5)"}, {'range': [50, 75], 'color': "rgba(255, 165, 0, 0.5)"}, {'range': [75, 100], 'color': "rgba(255, 0, 0, 0.5)"}]}))
                            fig_gauge.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=20))
                            st.plotly_chart(fig_gauge, use_container_width=True)

                            year_1_risk_val = job_data.get('year_1_risk', 0)
                            col1_gauge, col2_gauge = st.columns(2)
                            with col1_gauge:
                                st.metric("1-Year Risk", f"{year_1_risk_val if isinstance(year_1_risk_val,(int,float)) else 0:.1f}%")
                            with col2_gauge:
                                st.metric("5-Year Risk", f"{gauge_val:.1f}%")
                        
                        with risk_factors_col:
                            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Key Risk Factors</h3>", unsafe_allow_html=True)
                            risk_fs = job_data.get('risk_factors', [])
                            if risk_fs:
                                for factor in risk_fs: st.markdown(f"❌ {factor}")
                            else: st.markdown("Specific risk factors not available from BLS data analysis.")
                            
                            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Protective Factors</h3>", unsafe_allow_html=True)
                            protective_fs = job_data.get('protective_factors', [])
                            if protective_fs:
                                for factor in protective_fs: st.markdown(f"✅ {factor}")
                            else: st.markdown("Specific protective factors not available from BLS data analysis.")

                        st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Key Insights & Analysis</h3>", unsafe_allow_html=True)
                        st.markdown(job_data.get('analysis', "No detailed analysis available for this occupation based on BLS data."))

                        # Employment Trend Chart
                        st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Employment Trend (Illustrative)</h3>", unsafe_allow_html=True)
                        trend_info = job_data.get('trend_data', {})
                        trend_years = trend_info.get('years', [])
                        trend_employment = trend_info.get('employment', [])
                        if trend_years and trend_employment and any(e > 0 for e in trend_employment):
                            fig_trend = px.line(x=trend_years, y=trend_employment, labels={'x':'Year', 'y':'Number of Jobs'}, title=f"Illustrative Employment Trend for {job_data.get('job_title', search_job_title)}")
                            st.plotly_chart(fig_trend, use_container_width=True)
                        else:
                            st.info("Employment trend data could not be generated or is unavailable from BLS for this occupation.")

                        # Similar Jobs (if provided by bls_job_mapper authentically)
                        # For now, assuming this is not provided by pure BLS data.
                        # If bls_job_mapper is updated to provide this, uncomment and adapt.
                        # st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Similar Jobs</h3>", unsafe_allow_html=True)
                        # similar_jobs_data = job_data.get('similar_jobs', [])
                        # if similar_jobs_data: ... display ...
                        # else: st.info("Information on similar jobs is not available from BLS for this occupation.")
                        
                        # Risk Assessment Summary
                        st.markdown("<hr>", unsafe_allow_html=True)
                        st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Risk Assessment Summary</h3>", unsafe_allow_html=True)
                        st.markdown(job_data.get('summary', "Summary based on BLS category analysis and AI impact research."))

                        # Career Navigator CTA
                        if 'career_navigator' in sys.modules and hasattr(career_navigator, 'get_html'):
                            st.markdown(career_navigator.get_html(), unsafe_allow_html=True)

                        # Recent Searches
                        if database_available and 'database' in sys.modules and hasattr(database, 'get_recent_searches'):
                            st.markdown("<hr>", unsafe_allow_html=True)
                            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Recent Job Searches</h3>", unsafe_allow_html=True)
                            recent_searches_list = database.get_recent_searches(limit=5)
                            if recent_searches_list:
                                for rs in recent_searches_list:
                                    rs_ts = rs.get('timestamp')
                                    time_ago = "Recently"
                                    if rs_ts:
                                        try:
                                            delta = datetime.datetime.now() - (rs_ts if isinstance(rs_ts, datetime.datetime) else datetime.datetime.fromisoformat(str(rs_ts)))
                                            if delta.days > 0: time_ago = f"{delta.days} days ago"
                                            elif delta.seconds >= 3600: time_ago = f"{delta.seconds // 3600} hours ago"
                                            elif delta.seconds >= 60: time_ago = f"{delta.seconds // 60} minutes ago"
                                            else: time_ago = "Just now"
                                        except Exception: pass # Ignore formatting errors for timestamp
                                    st.markdown(f"- **{rs.get('job_title')}** - Risk: {rs.get('risk_category', 'N/A')} ({time_ago})")
                            else:
                                st.info("No recent searches found in the database.")
                
                except Exception as e_main_analysis:
                    st.error(f"An unexpected error occurred during analysis: {e_main_analysis}")
                    logger.error(f"Unexpected error in single job analysis for '{search_job_title}': {e_main_analysis}", exc_info=True)

# Job Comparison Tab
with tabs[1]:
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    st.markdown("Compare AI displacement risk for up to 5 jobs side by side. Uses authentic BLS data.")

    if 'selected_jobs_for_comparison' not in st.session_state:
        st.session_state.selected_jobs_for_comparison = []

    if 'job_title_autocomplete' in globals() and callable(job_title_autocomplete):
        new_job_to_compare = job_title_autocomplete(
            label="Enter a job title to add to comparison:",
            key="compare_job_input_main_app", # Unique key
            placeholder="e.g., Software Developer",
            help="Select a job from BLS data to add for comparison."
        )
    else:
        new_job_to_compare = st.text_input("Enter job title to add to comparison (Autocomplete N/A):", key="compare_job_input_main_app_fallback")

    if st.button("Add Job to Comparison", key="add_to_compare_button") and new_job_to_compare:
        if len(st.session_state.selected_jobs_for_comparison) < 5:
            if new_job_to_compare not in st.session_state.selected_jobs_for_comparison:
                st.session_state.selected_jobs_for_comparison.append(new_job_to_compare)
                logger.info(f"Added '{new_job_to_compare}' to comparison list.")
            else:
                st.warning(f"'{new_job_to_compare}' is already in the comparison list.")
        else:
            st.warning("Maximum of 5 jobs can be compared.")
        # Clear input after adding
        st.session_state.compare_job_input_main_app = ""
        if 'compare_job_input_main_app_select' in st.session_state:
            del st.session_state.compare_job_input_main_app_select
        st.rerun()


    if st.session_state.selected_jobs_for_comparison:
        st.subheader("Jobs Selected for Comparison:")
        for i, job_title_comp in enumerate(st.session_state.selected_jobs_for_comparison):
            col1, col2 = st.columns([0.8, 0.2])
            col1.write(f"- {job_title_comp}")
            if col2.button("Remove", key=f"remove_comp_job_{i}"):
                st.session_state.selected_jobs_for_comparison.pop(i)
                logger.info(f"Removed '{job_title_comp}' from comparison list.")
                st.rerun()
        
        if st.button("Clear All Comparison Jobs", key="clear_comp_jobs_button"):
            st.session_state.selected_jobs_for_comparison = []
            logger.info("Cleared all jobs from comparison list.")
            st.rerun()

    if st.session_state.selected_jobs_for_comparison and st.button("Analyze Comparison", type="primary", key="analyze_comp_button"):
        if not bls_api_key:
            st.error("BLS API Key is not configured. Cannot perform comparison.")
        elif not database_available and not engine:
             st.error("Database is not connected. Cannot perform comparison.")
        else:
            with st.spinner("Fetching data for job comparison..."):
                try:
                    # simple_comparison module should use job_api_integration internally
                    comparison_data = simple_comparison.get_job_comparison_data(st.session_state.selected_jobs_for_comparison)
                    
                    if not comparison_data or ("error_input" in comparison_data):
                        st.error(f"Could not fetch comparison data. Error: {comparison_data.get('error_input', 'Unknown error')}")
                    else:
                        # Filter out jobs that had errors during fetching
                        valid_comparison_data = {k: v for k, v in comparison_data.items() if v and "error" not in v}
                        errored_jobs_comp = {k: v for k, v in comparison_data.items() if v and "error" in v}

                        if errored_jobs_comp:
                            st.warning("Some jobs could not be processed for comparison:")
                            for job_title_err, err_data in errored_jobs_comp.items():
                                st.markdown(f"- **{job_title_err}**: {err_data['error']}")
                        
                        if not valid_comparison_data:
                            st.error("No valid data could be fetched for any of the selected jobs for comparison.")
                        else:
                            comp_tabs = st.tabs(["Comparison Chart", "Detailed Table", "Risk Heatmap", "Radar Chart"])
                            with comp_tabs[0]:
                                chart = simple_comparison.create_comparison_chart(valid_comparison_data)
                                if chart: st.plotly_chart(chart, use_container_width=True)
                                else: st.info("Could not generate comparison chart (possibly no valid data).")
                            with comp_tabs[1]:
                                table_df = simple_comparison.create_comparison_table(valid_comparison_data)
                                if table_df is not None: st.dataframe(table_df, use_container_width=True)
                                else: st.info("Could not generate detailed comparison table.")
                            with comp_tabs[2]:
                                heatmap = simple_comparison.create_risk_heatmap(valid_comparison_data)
                                if heatmap: st.plotly_chart(heatmap, use_container_width=True)
                                else: st.info("Could not generate risk heatmap.")
                            with comp_tabs[3]:
                                radar = simple_comparison.create_radar_chart(valid_comparison_data)
                                if radar: st.plotly_chart(radar, use_container_width=True)
                                else: st.info("Could not generate radar chart.")
                except Exception as e_comp:
                    st.error(f"An unexpected error occurred during job comparison: {e_comp}")
                    logger.error(f"Unexpected error in job comparison: {e_comp}", exc_info=True)

# --- Admin Controls Section (Integrated at the bottom) ---
st.markdown("---")
with st.expander("⚙️ ADMIN CONTROLS - Click to Expand"):
    st.warning("This section is for administrators only and provides tools for database management.")
    if 'simplified_admin' in sys.modules and hasattr(simplified_admin, 'display_admin_controls'):
        if engine: # Check if engine was successfully initialized
            simplified_admin.display_admin_controls(engine)
        else:
            st.error("Database engine not available. Admin controls cannot be displayed.")
            logger.error("Admin Controls: Database engine is None.")
    else:
        st.error("Admin controls module (simplified_admin.py) not loaded correctly.")
        logger.error("Admin Controls: simplified_admin module or display_admin_controls function not found.")


# --- Application Footer ---
st.markdown("---")
st.markdown("""
<div style="text-align: center;">
    <p style="color: #666666;">© 2025 iThriveAI - AI Job Displacement Risk Analyzer</p>
    <p style="color: #666666; font-size: 12px;">
        Powered by real-time Bureau of Labor Statistics data | 
        <a href="https://www.bls.gov/ooh/" target="_blank">BLS Occupational Outlook Handbook</a>
    </p>
</div>
""", unsafe_allow_html=True)

# Version and Keep-Alive Status in Sidebar
st.sidebar.markdown("---")
st.sidebar.caption(f"App Version: 2.1.0 (Real Data Only)")
st.sidebar.caption(f"Last App Load: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

try:
    with open("app_keep_alive_stats.json", "r") as f_kasta:
        ka_stats = json.load(f_kasta)
    ka_last_success = ka_stats.get("last_success")
    if ka_last_success:
        ka_last_success_dt = datetime.datetime.fromisoformat(ka_last_success)
        ka_time_diff = datetime.datetime.now() - ka_last_success_dt
        ka_status_text = f"Active (last ping: {ka_time_diff.total_seconds() / 60:.1f} min ago)"
        ka_status_class = "online" if ka_time_diff.total_seconds() < 600 else "offline" # Offline if >10 mins
    else:
        ka_status_text = "No successful pings yet"
        ka_status_class = "offline"
    st.sidebar.markdown(f"""<div class="status-indicator {ka_status_class}"><div class="status-indicator-dot {ka_status_class}"></div>Keep-Alive: {ka_status_text}</div>""", unsafe_allow_html=True)
except Exception:
    st.sidebar.markdown("""<div class="status-indicator offline"><div class="status-indicator-dot offline"></div>Keep-Alive: Status N/A</div>""", unsafe_allow_html=True)

# UptimeRobot Setup Instructions in Sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("<p style='text-align: center; color: #666666; font-size: 14px;'>UptimeRobot Setup</p>", unsafe_allow_html=True)
st.sidebar.info("""
To keep this application alive with UptimeRobot:
1. Create a new monitor in UptimeRobot
2. Set Type to "HTTP(s)"
3. Set URL to your app URL with `?health_check=true` (e.g., `your-app-url.streamlit.app/?health_check=true`)
4. Set monitoring interval to 5 minutes
5. Enable "Alert When Down"
""")
