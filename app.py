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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[logging.StreamHandler()]
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
                except Exception:
                    pass
            
            if db_url_for_ping:
                try:
                    if db_url_for_ping.startswith("postgres://"):
                        db_url_for_ping = db_url_for_ping.replace("postgres://", "postgresql://", 1)
                    ping_engine = create_engine(db_url_for_ping, connect_args={"connect_timeout": 5})
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
                streamlit_server_address = os.environ.get('STREAMLIT_SERVER_ADDRESS', 'localhost')
                streamlit_server_port = os.environ.get('STREAMLIT_SERVER_PORT', '8501')
                is_streamlit_cloud = "STREAMLIT_SHARING_MODE" in os.environ and os.environ["STREAMLIT_SHARING_MODE"] == "True"

                if not is_streamlit_cloud:
                    base_url = f"http://{streamlit_server_address}:{streamlit_server_port}"
                    health_url = f"{base_url}/?health_check=true"
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
            
            time.sleep(300)

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
    import job_api_integration_database_only as job_api_integration
    import simple_comparison
    import ai_job_displacement
    import career_navigator
    import bls_job_mapper # Added this import
    from job_title_autocomplete_v2 import job_title_autocomplete
    import database
    # simplified_admin module's functionality will be integrated directly or called if its functions are self-contained
    logger.info("Core application modules imported successfully.")
except ImportError as e:
    logger.critical(f"Failed to import one or more core application modules: {e}. Application may not function.", exc_info=True)
    st.error(f"Application Error: A critical module failed to load ({e}). Please check the logs. The application might be unstable.")

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
if 'database' in sys.modules and hasattr(database, 'engine') and database.engine is not None:
    engine = database.engine
    database_available = True
    logger.info("Database engine loaded successfully from database module.")
else:
    logger.error("Database engine not available from database module. Database-dependent features will be disabled.")
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
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            database_available = True
            logger.info("Successfully created and connected a new database engine in app.py.")
        except Exception as e_eng:
            logger.error(f"Failed to create database engine in app.py: {e_eng}")
            engine = None
            database_available = False
    else:
        logger.error("DATABASE_URL not found. Cannot initialize database engine in app.py.")

# --- Health Check Endpoint ---
query_params = st.query_params
if query_params.get("health_check") == "true":
    st.text("OK")
    if query_params.get("detailed") == "true":
        st.markdown("## Detailed Health Status")
        st.markdown(f"- **BLS API Key Configured**: {'Yes' if bls_api_key else 'No'}\n")
        st.markdown(f"- **Database Connection**: {'Connected' if database_available and engine else 'Not Connected'}\n")
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
    .no-data-message { background-color: #f8f9fa; padding: 20px; border-radius: 5px; text-align: center; margin: 20px 0; }
</style>
""", unsafe_allow_html=True)

# --- Sidebar Status Indicators ---
st.sidebar.title("System Status")
if bls_api_key:
    st.sidebar.markdown("""<div class="status-indicator online"><div class="status-indicator-dot online"></div>BLS API: Configured</div>""", unsafe_allow_html=True)
else:
    st.sidebar.markdown("""<div class="status-indicator offline"><div class="status-indicator-dot offline"></div>BLS API: NOT CONFIGURED</div>""", unsafe_allow_html=True)
    st.error("BLS API Key is not configured. Please set the BLS_API_KEY in Streamlit secrets or environment variables. The application cannot function without it.")
    # Potentially st.stop() here if API key is absolutely critical for any app function

if database_available and engine:
    st.sidebar.markdown("""<div class="status-indicator online"><div class="status-indicator-dot online"></div>Database: Connected</div>""", unsafe_allow_html=True)
else:
    st.sidebar.markdown("""<div class="status-indicator offline"><div class="status-indicator-dot offline"></div>Database: Disconnected</div>""", unsafe_allow_html=True)
    st.warning("Database is not connected. Some features like saving searches or admin controls might be unavailable or use fallback data if implemented.")

# Display last keep-alive ping status
try:
    with open("app_keep_alive_stats.json", "r") as f:
        keep_alive_data = json.load(f)
        last_success_str = keep_alive_data.get("last_success")
        if last_success_str:
            last_success_dt = datetime.datetime.fromisoformat(last_success_str)
            now = datetime.datetime.now()
            # Check if last_success_dt is timezone-aware or naive
            if last_success_dt.tzinfo is None: # Naive
                 # Assume UTC if naive, or make it timezone-aware based on server's timezone
                 # For simplicity, if it's naive, compare with naive now
                 # This might need adjustment based on how timestamps are stored / server timezone
                 time_since_last_ping = (datetime.datetime.utcnow() - last_success_dt).total_seconds() / 60
            else: # Aware
                 time_since_last_ping = (datetime.datetime.now(datetime.timezone.utc) - last_success_dt).total_seconds() / 60

            st.sidebar.markdown(f"""<div class="status-indicator online"><div class="status-indicator-dot online"></div>Keep-Alive: Active (last ping: {time_since_last_ping:.1f} min ago)</div>""", unsafe_allow_html=True)
        else:
            st.sidebar.markdown("""<div class="status-indicator offline"><div class="status-indicator-dot offline"></div>Keep-Alive: No successful pings yet</div>""", unsafe_allow_html=True)
except FileNotFoundError:
    st.sidebar.markdown("""<div class="status-indicator offline"><div class="status-indicator-dot offline"></div>Keep-Alive: Status file not found</div>""", unsafe_allow_html=True)
except Exception as e:
    st.sidebar.markdown(f"""<div class="status-indicator offline"><div class="status-indicator-dot offline"></div>Keep-Alive: Error reading status ({e})</div>""", unsafe_allow_html=True)


st.sidebar.markdown("---")
st.sidebar.markdown(f"App Version: 2.1.0 (Real Data Only)") # Updated version
st.sidebar.markdown(f"Last App Load: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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


# --- Main Application UI ---
st.image("https://img1.wsimg.com/isteam/ip/70686f32-22d2-489c-a383-6fcd793644be/blob-3712e2e.png/:/rs=h:197,cg:true,m/qt=q:95", width=250)
st.markdown("<h1 style='text-align: center; color: #0084FF;'>Is your job at risk with AI innovation?</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #4CACE5; font-size: 24px; font-weight: 600;'>AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666666; font-weight: bold; font-size: 16px;'>Discover how AI might impact your career in the next 5 years and get personalized recommendations.</p>", unsafe_allow_html=True)
st.info("📊 This application uses authentic Bureau of Labor Statistics (BLS) data only. No synthetic or fictional data is used.")

tabs = st.tabs(["Single Job Analysis", "Job Comparison"])

with tabs[0]:
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    
    search_job_title = job_title_autocomplete(
        label="Enter any job title to analyze",
        key="job_title_search_single",
        placeholder="Start typing to see suggestions...",
        help="Type a job title and select from matching suggestions from BLS data."
    )
    
    if st.button("🗑️ Clear Entry", key="clear_button_single"):
        st.session_state.job_title_search_single_input = "" # Clear the input part of autocomplete
        st.rerun()
    
    search_clicked = st.button("Analyze Job Risk", key="analyze_single_job")
    
    if search_clicked and search_job_title:
        with st.spinner(f"Analyzing {search_job_title}..."):
            try:
                job_data = job_api_integration.get_job_data(search_job_title)
                
                if "error" in job_data:
                    st.error(f"Error: {job_data['error']}")
                    if "not found in BLS database" in job_data['error']:
                         st.info("Please try a different job title or use the admin dashboard to add this occupation if it's a valid BLS occupation.")
                    st.stop()

                if database_available:
                    database.save_job_search(search_job_title, {
                        'year_1_risk': job_data.get('year_1_risk'),
                        'year_5_risk': job_data.get('year_5_risk'),
                        'risk_category': job_data.get('risk_category'),
                        'job_category': job_data.get('job_category')
                    })
                
                st.subheader(f"AI Displacement Risk Analysis: {job_data.get('job_title', search_job_title)}")
                job_info_col, risk_gauge_col, risk_factors_col = st.columns([1,1,1])

                with job_info_col:
                    st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Job Information</h3>", unsafe_allow_html=True)
                    st.markdown(f"**Occupation Code:** {job_data.get('occupation_code', 'N/A')}")
                    st.markdown(f"**Job Category:** {job_data.get('job_category', 'N/A')}")
                    employment = job_data.get('employment')
                    st.markdown(f"**Current Employment:** {int(employment):,}" if employment is not None else "N/A")
                    growth = job_data.get('employment_change_percent')
                    st.markdown(f"**BLS Projected Growth:** {growth:+.1f}%" if growth is not None else "N/A")
                    openings = job_data.get('annual_job_openings')
                    st.markdown(f"**Annual Job Openings:** {int(openings):,}" if openings is not None else "N/A")

                    st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Career Outlook</h3>", unsafe_allow_html=True)
                    automation_prob = (job_data.get('year_1_risk',0) + job_data.get('year_5_risk',0)) / 2 # Example calculation
                    st.markdown(f"**Task Automation Potential (Est.):** {automation_prob:.1f}%")
                    median_wage = job_data.get('median_wage')
                    st.markdown(f"**Median Annual Wage:** ${int(median_wage):,.0f}" if median_wage is not None else "N/A")

                with risk_gauge_col:
                    risk_category_display = job_data.get("risk_category", "Unknown")
                    st.markdown(f"<h3 style='text-align: center; margin-bottom: 10px;'>Overall AI Displacement Risk: {risk_category_display}</h3>", unsafe_allow_html=True)
                    gauge_value = job_data.get("year_5_risk", 0)
                    fig = go.Figure(go.Indicator(
                        mode = "gauge+number", value = gauge_value,
                        domain = {'x': [0, 1], 'y': [0, 1]}, title = {'text': ""},
                        number = {'suffix': '%', 'font': {'size': 28}},
                        gauge = {'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                                 'bar': {'color': "#0084FF"}, 'bgcolor': "white", 'borderwidth': 2, 'bordercolor': "gray",
                                 'steps': [{'range': [0, 25], 'color': "rgba(0, 255, 0, 0.5)"}, {'range': [25, 50], 'color': "rgba(255, 255, 0, 0.5)"},
                                           {'range': [50, 75], 'color': "rgba(255, 165, 0, 0.5)"}, {'range': [75, 100], 'color': "rgba(255, 0, 0, 0.5)"}],
                                 'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': gauge_value}}))
                    fig.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=20))
                    st.plotly_chart(fig, use_container_width=True)
                    col1_risk, col2_risk = st.columns(2)
                    with col1_risk:
                        st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>1-Year Risk</h4></div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{job_data.get('year_1_risk', 0):.1f}%</div>", unsafe_allow_html=True)
                    with col2_risk:
                        st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>5-Year Risk</h4></div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{job_data.get('year_5_risk', 0):.1f}%</div>", unsafe_allow_html=True)

                with risk_factors_col:
                    st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Key Risk Factors</h3>", unsafe_allow_html=True)
                    for factor in job_data.get("risk_factors", ["Data not available."]): st.markdown(f"❌ {factor}")
                    st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Protective Factors</h3>", unsafe_allow_html=True)
                    for factor in job_data.get("protective_factors", ["Data not available."]): st.markdown(f"✅ {factor}")

                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Key Insights</h3>", unsafe_allow_html=True)
                st.markdown(job_data.get("analysis", "Analysis not available."))

                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Employment Trend</h3>", unsafe_allow_html=True)
                trend_data = job_data.get("trend_data", {})
                if trend_data and "years" in trend_data and "employment" in trend_data and any(trend_data["employment"]):
                    trend_fig = go.Figure()
                    trend_fig.add_trace(go.Scatter(x=trend_data["years"], y=trend_data["employment"], mode='lines+markers', name='Employment', line=dict(color='#0084FF', width=2), marker=dict(size=8)))
                    trend_fig.update_layout(title=f'Employment Trend for {job_data.get("job_title", search_job_title)}', xaxis_title='Year', yaxis_title='Number of Jobs', height=350, margin=dict(l=40, r=40, t=60, b=40))
                    st.plotly_chart(trend_fig, use_container_width=True)
                else:
                    st.info("📊 Employment trend data from Bureau of Labor Statistics not available for this position.")
                
                st.markdown("<hr>", unsafe_allow_html=True)
                st.markdown(career_navigator.get_html(), unsafe_allow_html=True) # Career navigator CTA

                if database_available:
                    st.markdown("<hr>", unsafe_allow_html=True)
                    st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Recent Job Searches</h3>", unsafe_allow_html=True)
                    recent_searches = database.get_recent_searches(limit=5)
                    if recent_searches:
                        # ... (display recent searches logic from app_production.py) ...
                        for i, search in enumerate(recent_searches): # Added enumerate for unique keys
                            job_title_display = search.get("job_title", "Unknown Job")
                            risk_category_display = search.get("risk_category", "Unknown")
                            timestamp_display = search.get("timestamp")
                            
                            if timestamp_display:
                                now = datetime.datetime.now(datetime.timezone.utc) # Make now timezone-aware
                                if isinstance(timestamp_display, str):
                                    try:
                                        timestamp_display = datetime.datetime.fromisoformat(timestamp_display.replace("Z", "+00:00"))
                                    except ValueError: # Handle if it's already a datetime object or different format
                                        if isinstance(timestamp_display, datetime.datetime): pass # Already datetime
                                        else: timestamp_display = datetime.datetime.now(datetime.timezone.utc) # Fallback
                                if timestamp_display.tzinfo is None: # If still naive, assume UTC
                                    timestamp_display = timestamp_display.replace(tzinfo=datetime.timezone.utc)
                                
                                delta = now - timestamp_display
                                # ... rest of time_ago logic ...
                                if delta.days > 0: time_ago = f"{delta.days} days ago"
                                elif delta.seconds >= 3600: hours = delta.seconds // 3600; time_ago = f"{hours} hour{'s' if hours > 1 else ''} ago"
                                elif delta.seconds >= 60: minutes = delta.seconds // 60; time_ago = f"{minutes} minute{'s' if minutes > 1 else ''} ago"
                                else: time_ago = "Just now"
                            else: time_ago = "Recently"

                            risk_color = {"Very High": "#FF4B4B", "High": "#FF8C42", "Moderate": "#FFCC3E", "Low": "#4CAF50"}.get(risk_category_display, "#666666")
                            
                            r_col1, r_col2, r_col3 = st.columns([3,2,2])
                            with r_col1:
                                search_key = f"recent_search_{job_title_display.replace(' ', '_')}_{i}_{abs(hash(str(search))) % 10000}"
                                if st.button(job_title_display, key=search_key, help=f"Search for {job_title_display} again"):
                                    st.session_state.job_title_search_single_input = job_title_display
                                    st.rerun()
                            with r_col2: st.markdown(f"<p style='color: {risk_color};'>{risk_category_display}</p>", unsafe_allow_html=True)
                            with r_col3: st.write(time_ago)
                    else:
                        st.info("No recent searches yet.")
            except Exception as e:
                logger.error(f"Error during single job analysis for '{search_job_title}': {str(e)}", exc_info=True)
                st.error(f"An unexpected error occurred while analyzing '{search_job_title}'. Details: {str(e)}")

with tabs[1]:
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    st.markdown("Compare the AI displacement risk for multiple jobs side by side. Add up to 5 jobs.")
    
    if 'selected_jobs_for_comparison' not in st.session_state:
        st.session_state.selected_jobs_for_comparison = []

    new_job_to_compare = job_title_autocomplete(
        label="Enter a job title to add to comparison:",
        key="job_title_compare_input",
        placeholder="Start typing...",
        help="Select a job from suggestions to add it for comparison."
    )

    if st.button("Add to Comparison", key="add_to_compare_btn"):
        if new_job_to_compare and new_job_to_compare not in st.session_state.selected_jobs_for_comparison and len(st.session_state.selected_jobs_for_comparison) < 5:
            st.session_state.selected_jobs_for_comparison.append(new_job_to_compare)
            st.session_state.job_title_compare_input_input = "" # Clear the input part
            st.rerun()
        elif len(st.session_state.selected_jobs_for_comparison) >= 5:
            st.warning("Maximum of 5 jobs can be compared at a time.")
        elif new_job_to_compare in st.session_state.selected_jobs_for_comparison:
            st.info(f"'{new_job_to_compare}' is already in the comparison list.")

    if st.session_state.selected_jobs_for_comparison:
        st.subheader("Jobs Selected for Comparison:")
        cols = st.columns(len(st.session_state.selected_jobs_for_comparison) + 1) # +1 for clear all
        for i, job_title_comp in enumerate(st.session_state.selected_jobs_for_comparison):
            with cols[i]:
                st.markdown(f"**{job_title_comp}**")
                if st.button("Remove", key=f"remove_compare_{i}"):
                    st.session_state.selected_jobs_for_comparison.pop(i)
                    st.rerun()
        with cols[-1]:
            if st.button("Clear All", key="clear_compare_list"):
                st.session_state.selected_jobs_for_comparison = []
                st.rerun()
    
    if st.session_state.selected_jobs_for_comparison and len(st.session_state.selected_jobs_for_comparison) >= 1:
        if st.button("Analyze Comparison", key="analyze_comparison_btn", type="primary"):
            with st.spinner(f"Analyzing {len(st.session_state.selected_jobs_for_comparison)} jobs..."):
                try:
                    comparison_job_data = simple_comparison.get_job_comparison_data(st.session_state.selected_jobs_for_comparison)
                    
                    # Filter out jobs that had errors during data fetching
                    valid_comparison_data = {k: v for k, v in comparison_job_data.items() if "error" not in v}
                    errored_jobs_comp = {k: v["error"] for k, v in comparison_job_data.items() if "error" in v}

                    if errored_jobs_comp:
                        for job_title_err, err_msg in errored_jobs_comp.items():
                            st.error(f"Could not fetch data for '{job_title_err}': {err_msg}")
                    
                    if not valid_comparison_data:
                        st.warning("No valid data to compare. Please check job titles or try again.")
                        st.stop()

                    comparison_tabs = st.tabs(["Comparison Chart", "Comparative Analysis", "Risk Heatmap", "Risk Factors"])
                    with comparison_tabs[0]:
                        st.markdown("<h3 style='color: #0084FF;'>5-Year AI Displacement Risk Comparison</h3>", unsafe_allow_html=True)
                        chart = simple_comparison.create_comparison_chart(valid_comparison_data)
                        if chart: st.plotly_chart(chart, use_container_width=True)
                        else: st.info("Not enough data to create comparison chart.")
                    with comparison_tabs[1]:
                        st.markdown("<h3 style='color: #0084FF;'>Detailed Comparison</h3>", unsafe_allow_html=True)
                        comparison_df = simple_comparison.create_comparison_table(valid_comparison_data)
                        if comparison_df is not None: st.dataframe(comparison_df, use_container_width=True)
                        else: st.info("Not enough data for detailed table.")
                    with comparison_tabs[2]:
                        st.markdown("<h3 style='color: #0084FF;'>Risk Progression Heatmap</h3>", unsafe_allow_html=True)
                        heatmap = simple_comparison.create_risk_heatmap(valid_comparison_data)
                        if heatmap: st.plotly_chart(heatmap, use_container_width=True)
                        else: st.info("Not enough data for heatmap.")
                    with comparison_tabs[3]:
                        st.markdown("<h3 style='color: #0084FF;'>Risk Factor Analysis (Radar)</h3>", unsafe_allow_html=True)
                        radar = simple_comparison.create_radar_chart(valid_comparison_data)
                        if radar: st.plotly_chart(radar, use_container_width=True)
                        else: st.info("Not enough data for radar chart.")
                    
                    st.markdown("<hr>", unsafe_allow_html=True)
                    st.markdown(career_navigator.get_html(), unsafe_allow_html=True)

                except Exception as e:
                    logger.error(f"Error during job comparison: {str(e)}", exc_info=True)
                    st.error(f"An unexpected error occurred during comparison: {str(e)}")

# --- Admin Controls Section ---
if database_available: # Only show admin controls if DB is connected
    with st.sidebar.expander("⚙️ ADMIN CONTROLS - Click to Expand", expanded=False):
        st.markdown("**This section is for administrators only and provides tools for database management.**")
        
        # Load target SOC codes from bls_job_mapper
        if 'bls_job_mapper' in sys.modules and hasattr(bls_job_mapper, 'TARGET_SOC_CODES'):
            TARGET_SOC_CODES = bls_job_mapper.TARGET_SOC_CODES
        else:
            TARGET_SOC_CODES = [{"soc_code": "00-0000", "title": "No SOC codes loaded"}]
            st.warning("TARGET_SOC_CODES not found in bls_job_mapper. Admin tool will not function correctly.")

        # Initialize session state for admin panel
        if "run_batch_population" not in st.session_state:
            st.session_state.run_batch_population = False
        if "current_soc_index" not in st.session_state:
            st.session_state.current_soc_index = 0
        if "admin_log" not in st.session_state:
            st.session_state.admin_log = []
        if "failed_soc_populations" not in st.session_state:
            st.session_state.failed_soc_populations = {} # Store SOC: error_message

        # Load progress if exists
        progress_file = "simplified_admin_population_progress.json"
        if os.path.exists(progress_file) and st.session_state.current_soc_index == 0 and not st.session_state.run_batch_population:
            try:
                with open(progress_file, "r") as f:
                    progress_data = json.load(f)
                    st.session_state.current_soc_index = progress_data.get("current_soc_index", 0)
                    st.session_state.failed_soc_populations = progress_data.get("failed_soc_populations", {})
                    st.session_state.admin_log.append(f"--- Resumed progress from index {st.session_state.current_soc_index} ---")
                    logger.info(f"Admin tool: Resumed progress from index {st.session_state.current_soc_index}.")
            except Exception as e:
                logger.error(f"Admin tool: Error loading progress file: {e}")
                st.session_state.admin_log.append(f"--- Error loading progress: {e} ---")


        st.subheader("Simplified Admin: Database Population Tool")
        
        total_socs = len(TARGET_SOC_CODES)
        processed_socs = st.session_state.current_soc_index
        progress_message = f"Overall Progress: {processed_socs} SOCs processed out of {total_socs} target SOCs. Next to process: Index {st.session_state.current_soc_index}."
        st.caption(progress_message)
        
        # Batch controls
        batch_size = st.number_input("Batch Size (SOCs per run)", min_value=1, max_value=50, value=10, step=1)
        api_delay = st.number_input("Delay Between API Calls (seconds)", min_value=0.1, max_value=10.0, value=1.0, step=0.1)

        col_admin1, col_admin2, col_admin3 = st.columns(3)
        with col_admin1:
            if st.button("▶️ Start/Resume Batch", key="start_batch_admin"):
                if not bls_api_key:
                    st.error("BLS API Key is not configured. Cannot start batch.")
                    st.session_state.admin_log.append("--- ERROR: BLS API Key not configured. Batch aborted. ---")
                else:
                    st.session_state.run_batch_population = True
                    st.session_state.admin_log.append(f"--- New Batch Started at {datetime.datetime.now().strftime('%H:%M:%S')} ---")
                    logger.info("Admin tool: Batch population started/resumed by admin.")
                    st.rerun() # Rerun to start processing immediately
        with col_admin2:
            if st.button("⏸️ Pause (Stop Auto-Run)", key="pause_batch_admin"):
                st.session_state.run_batch_population = False
                st.session_state.admin_log.append("--- Batch Paused by Admin ---")
                logger.info("Admin tool: Batch population paused by admin.")
                # Save current progress when pausing
                try:
                    with open(progress_file, "w") as f:
                        json.dump({
                            "current_soc_index": st.session_state.current_soc_index,
                            "failed_soc_populations": st.session_state.failed_soc_populations
                        }, f)
                    st.session_state.admin_log.append(f"--- Progress saved at index {st.session_state.current_soc_index} ---")
                except Exception as e:
                    st.session_state.admin_log.append(f"--- Error saving progress: {e} ---")
                st.rerun()
        with col_admin3:
            if st.button("🔄 Reset All Progress", key="reset_progress_admin"):
                if st.checkbox("Confirm Reset (this will clear all progress and failed logs)", key="confirm_reset_admin_check"):
                    st.session_state.run_batch_population = False
                    st.session_state.current_soc_index = 0
                    st.session_state.admin_log = ["--- Progress Reset by Admin ---"]
                    st.session_state.failed_soc_populations = {}
                    if os.path.exists(progress_file):
                        try:
                            os.remove(progress_file)
                            st.session_state.admin_log.append("--- Progress file deleted. ---")
                        except Exception as e:
                            st.session_state.admin_log.append(f"--- Error deleting progress file: {e} ---")
                    logger.info("Admin tool: All population progress reset by admin.")
                    st.rerun()
                else:
                    st.warning("Please confirm reset by checking the box.")


        # Display current batch log
        st.text_area("Current Batch Log", value="\n".join(st.session_state.admin_log), height=200, key="admin_log_area")

        # Automated batch processing logic
        if st.session_state.get("run_batch_population", False) and bls_api_key:
            if st.session_state.current_soc_index < total_socs:
                
                soc_to_process_this_iteration = st.session_state.target_soc_codes[st.session_state.current_soc_index]
                soc_code = soc_to_process_this_iteration["soc_code"]
                representative_title = soc_to_process_this_iteration["title"]
                
                log_entry = f"Processing SOC: {soc_code} (Rep. Title: '{representative_title}')"
                st.session_state.admin_log.append(log_entry)
                logger.info(f"Admin tool: {log_entry}")
                
                try:
                    # Call the main data fetching and processing function from bls_job_mapper
                    # This function now handles DB saving internally.
                    job_data = bls_job_mapper.get_complete_job_data(representative_title) # Use representative title for initial lookup
                    
                    if "error" in job_data or job_data.get("source") == "error_soc_mapping" or job_data.get("source") == "error_bls_api_fetch" or job_data.get("source") == "bls_api_fetch_error_or_db_save_failed":
                        err_msg = job_data.get('error', 'Unknown error during processing.')
                        log_entry = f"ERROR processing SOC {soc_code}: {err_msg}. Source: {job_data.get('source', 'N/A')}"
                        st.session_state.admin_log.append(log_entry)
                        logger.error(f"Admin tool: {log_entry}")
                        st.session_state.failed_soc_populations[soc_code] = err_msg
                    else:
                        log_entry = f"SUCCESS: Data fetched/updated for SOC {soc_code} ('{job_data.get('job_title', representative_title)}'). Source: {job_data.get('source', 'N/A')}"
                        st.session_state.admin_log.append(log_entry)
                        logger.info(f"Admin tool: {log_entry}")
                        # Remove from failed list if it was there previously and now succeeded
                        if soc_code in st.session_state.failed_soc_populations:
                            del st.session_state.failed_soc_populations[soc_code]
                            
                except AttributeError as ae:
                    # This catches if bls_job_mapper.get_complete_job_data is not found (e.g. due to import error in bls_job_mapper itself)
                    log_entry = f"CRITICAL_ERROR for SOC {soc_code}: The function bls_job_mapper.get_complete_job_data is not available. Details: {ae}"
                    st.session_state.admin_log.append(log_entry)
                    logger.critical(f"Admin tool: {log_entry}", exc_info=True)
                    st.session_state.failed_soc_populations[soc_code] = f"AttributeError: {ae}"
                    st.session_state.run_batch_population = False # Stop batch on critical module error
                except Exception as e:
                    log_entry = f"UNEXPECTED_ERROR for SOC {soc_code}: {str(e)}"
                    st.session_state.admin_log.append(log_entry)
                    logger.error(f"Admin tool: {log_entry}", exc_info=True)
                    st.session_state.failed_soc_populations[soc_code] = str(e)
                    st.session_state.run_batch_population = False # Stop batch on major unexpected error

                st.session_state.current_soc_index += 1
                
                # Save progress after each SOC
                try:
                    with open(progress_file, "w") as f:
                        json.dump({
                            "current_soc_index": st.session_state.current_soc_index,
                            "failed_soc_populations": st.session_state.failed_soc_populations
                        }, f)
                except Exception as e_save:
                    st.session_state.admin_log.append(f"--- Error saving progress after SOC {soc_code}: {e_save} ---")
                    logger.error(f"Admin tool: Error saving progress file: {e_save}")

                # Check if batch size reached or end of list
                if (st.session_state.current_soc_index % batch_size == 0) or (st.session_state.current_soc_index >= total_socs):
                    st.session_state.admin_log.append(f"--- Batch of {batch_size} (or end of list) reached. Pausing if auto-run not continuous. ---")
                    # If you want continuous auto-run, remove this line:
                    # st.session_state.run_batch_population = False # Pause after one batch
                    if st.session_state.current_soc_index >= total_socs:
                         st.session_state.admin_log.append("--- All target SOCs processed. Batch complete. ---")
                         st.session_state.run_batch_population = False # Stop when all done
                    st.rerun() # Rerun to update UI and continue if still run_batch_population
                else:
                    # Delay before next API call in the same batch run
                    time.sleep(api_delay)
                    st.rerun() # Rerun to process next item in batch
            else: # current_soc_index >= total_socs
                st.session_state.admin_log.append("--- All target SOCs have been processed. ---")
                st.session_state.run_batch_population = False
                st.success("Database population complete for all target SOCs!")
                logger.info("Admin tool: Database population complete for all target SOCs.")
                st.rerun()
        
        st.markdown("---")
        st.subheader("Summary of Failed SOC Populations")
        if st.session_state.failed_soc_populations:
            for soc, err in st.session_state.failed_soc_populations.items():
                st.error(f"SOC: {soc} - Error: {err}")
        else:
            st.info("No SOC codes are currently marked as having failed population.")
else:
    st.sidebar.warning("Admin controls disabled: Database not connected.")


# --- Footer ---
st.markdown("---")
st.markdown("""
<div style="text-align: center;">
    <p style="color: #666666;">© 2025 iThriveAI - AI Job Displacement Risk Analyzer</p>
    <p style="color: #666666;">Powered by real-time Bureau of Labor Statistics data | 
    <a href="https://www.bls.gov/ooh/" target="_blank" style="color: #0084FF;">BLS Occupational Outlook Handbook</a></p>
</div>
""", unsafe_allow_html=True)

# UptimeRobot status embed (optional)
st.markdown("""
<iframe src="https://stats.uptimerobot.com/LpYQ8UzVjG" height="0" width="0" frameborder="0" scrolling="no" style="display:none;"></iframe>
<script>
  function UptimeRobotCallback(data) {
    if (data.status === 2) { // Monitor is UP
      // console.log("UptimeRobot: App is UP");
    } else {
      // console.log("UptimeRobot: App is " + data.status_text);
    }
  }
</script>
<script src="https://status.uptimerobot.com/api/getstatuspage?id=LpYQ8UzVjG&callback=UptimeRobotCallback" async defer></script>
""", unsafe_allow_html=True)
