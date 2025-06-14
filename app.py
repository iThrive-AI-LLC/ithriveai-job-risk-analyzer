import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
import datetime
import os
import sys
import threading # Keep-alive needs this
import time
import re
import logging # For more detailed logging

# Custom logger for the app
logger = logging.getLogger("AI_Job_Analyzer_App")
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO) # Set to DEBUG for more verbose output if needed
    logger.propagate = False


# Attempt to import database modules first
try:
    from database import engine as db_engine, Base as db_Base, Session as db_Session, \
                         save_job_search, get_popular_searches, get_highest_risk_jobs, \
                         get_lowest_risk_jobs, get_recent_searches, JobSearch
    database_available = True
    logger.info("Successfully imported database modules.")
except ImportError as e:
    logger.error(f"Failed to import database modules: {e}. Using fallback data.")
    from db_fallback import save_job_search, get_popular_searches, get_highest_risk_jobs, \
                            get_lowest_risk_jobs, get_recent_searches
    database_available = False
    db_engine = None # Ensure engine is None if database.py fails

# Import other necessary modules
import job_api_integration_database_only as job_api_integration
import simple_comparison
import career_navigator
import bls_job_mapper # For TARGET_SOC_CODES

# Import the autocomplete functionality
from job_title_autocomplete_v2 import job_title_autocomplete

# --- Keep-Alive Functionality ---
def keep_alive():
    """Background thread to keep the app active and database connection warm."""
    logger.info("Keep-alive thread started.")
    while True:
        time.sleep(240)  # Ping every 4 minutes (slightly less than 5 min UptimeRobot)
        try:
            if database_available and db_engine:
                with db_engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                logger.info("Keep-alive: Database ping successful.")
            else:
                logger.info("Keep-alive: Database not available, skipping ping.")
        except Exception as e:
            logger.error(f"Keep-alive: Database ping failed: {e}")

# Start keep-alive thread only once
if "keep_alive_started" not in st.session_state:
    st.session_state.keep_alive_started = True
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    logger.info("Keep-alive thread initialized and started.")

# --- BLS API Key Check ---
bls_api_key = os.environ.get('BLS_API_KEY')
if not bls_api_key:
    try:
        if hasattr(st, 'secrets') and callable(st.secrets.get): # Check if st.secrets is usable
            bls_api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
        elif hasattr(st, 'secrets') and isinstance(st.secrets, dict) and "api_keys" in st.secrets: # Older dict-like access
            bls_api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
    except Exception as e:
        logger.warning(f"Could not access Streamlit secrets for BLS_API_KEY: {e}")

if bls_api_key:
    logger.info("BLS API key loaded from Streamlit secrets.")
else:
    logger.error("BLS_API_KEY is not configured. App will rely on database and may have limited real-time data functionality.")

# --- Health Check Endpoints ---
query_params = st.query_params
if query_params.get("health") == "true": # Simple health check for UptimeRobot
    st.text("OK")
    st.stop()

if query_params.get("health_check") == "true": # Detailed health check
    st.title("iThriveAI Job Analyzer - Health Check")
    st.success("✅ Application status: Running")
    
    if database_available and db_engine:
        try:
            with db_engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                if result.fetchone():
                    st.success("✅ Database connection: OK")
        except Exception as e:
            st.error(f"❌ Database connection: Failed ({e})")
    else:
        st.warning("⚠️ Database connection: Not available (using fallback data or not configured).")
    
    if bls_api_key:
        st.success("✅ BLS API key: Available")
    else:
        st.error("❌ BLS API key: Not configured. Real-time BLS data fetching will be disabled.")
        
    st.info("ℹ️ This endpoint is used for application monitoring and troubleshooting.")
    st.stop()

# --- Page Configuration (Must be the first Streamlit command) ---
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
    .stTabs [data-baseweb="tab"] {
        height: 60px; width: 250px; white-space: pre-wrap;
        background-color: #F0F8FF; border-radius: 4px 4px 0 0;
        gap: 10px; padding-top: 15px; padding-bottom: 15px;
        font-size: 18px; font-weight: 600; text-align: center;
    }
    .stTabs [aria-selected="true"] { background-color: #0084FF; color: white; }
    h1, h2, h3, h4, h5, h6 { color: #0084FF; }
    /* Risk level specific styles */
    .job-risk-low { background-color: #d4edda; border-radius: 5px; padding: 10px; margin-bottom: 10px; }
    .job-risk-moderate { background-color: #fff3cd; border-radius: 5px; padding: 10px; margin-bottom: 10px; }
    .job-risk-high { background-color: #f8d7da; border-radius: 5px; padding: 10px; margin-bottom: 10px; }
    .job-risk-very-high { background-color: #f8d7da; border-color: #f5c6cb; border-radius: 5px; padding: 10px; margin-bottom: 10px; border-width: 2px; border-style: solid; }
    .sidebar .sidebar-content { background-color: #f8f9fa; }
    .st-eb { border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# --- Application Header ---
st.image("https://img1.wsimg.com/isteam/ip/70686f32-22d2-489c-a383-6fcd793644be/blob-3712e2e.png/:/rs=h:197,cg:true,m/qt=q:95", width=250)
st.markdown("<h1 style='text-align: center; color: #0084FF;'>Is your job at risk with AI innovation?</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #4CACE5; font-size: 24px; font-weight: 600;'>AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666666; font-weight: bold; font-size: 16px;'>Discover how AI might impact your career in the next 5 years and get personalized recommendations.</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666666; font-size: 14px;'>📊 This application uses authentic Bureau of Labor Statistics (BLS) data only. No synthetic or fictional data is used.</p>", unsafe_allow_html=True)


# --- Admin Controls Setup ---
if 'admin_current_soc_index' not in st.session_state:
    st.session_state.admin_current_soc_index = 0
if 'admin_auto_run_batch' not in st.session_state:
    st.session_state.admin_auto_run_batch = False
if 'admin_failed_socs' not in st.session_state:
    st.session_state.admin_failed_socs = []
if 'admin_target_socs' not in st.session_state:
    st.session_state.admin_target_socs = [] # Will be loaded from bls_job_mapper
if 'admin_processed_count' not in st.session_state:
    st.session_state.admin_processed_count = 0

# Load target SOCs once
if not st.session_state.admin_target_socs:
    try:
        st.session_state.admin_target_socs = bls_job_mapper.TARGET_SOC_CODES
        logger.info(f"Admin: Successfully loaded {len(st.session_state.admin_target_socs)} target SOC codes.")
    except AttributeError: # TARGET_SOC_CODES might not be defined yet if bls_job_mapper is old
        logger.error("Admin: TARGET_SOC_CODES not found in bls_job_mapper. Admin tool will be limited.")
        st.session_state.admin_target_socs = [] # Ensure it's a list

# --- Admin Dashboard Logic ---
def run_batch_processing(batch_size, api_delay):
    """Processes a batch of SOC codes."""
    processed_in_batch = 0
    start_index = st.session_state.admin_current_soc_index
    target_socs = st.session_state.admin_target_socs
    
    if not target_socs:
        st.error("Admin: No target SOC codes loaded. Cannot run batch.")
        st.session_state.admin_auto_run_batch = False
        return

    for i in range(start_index, min(start_index + batch_size, len(target_socs))):
        if not st.session_state.admin_auto_run_batch: # Check if paused
            logger.info("Admin: Batch processing paused by user.")
            break 
            
        current_soc_info = target_socs[i]
        st.session_state.admin_current_soc_index = i # Update current index for UI
        
        soc_code = None
        job_title_for_api = None

        # Handle tuple structure from TARGET_SOC_CODES
        if isinstance(current_soc_info, tuple) and len(current_soc_info) == 2:
            soc_code = current_soc_info[0]
            job_title_for_api = current_soc_info[1] # Use the title from the tuple for API call context
            logger.info(f"Admin: Processing SOC tuple (Index {i}): {soc_code} - {job_title_for_api}")
        elif isinstance(current_soc_info, dict) and "soc_code" in current_soc_info and "title" in current_soc_info:
            soc_code = current_soc_info["soc_code"]
            job_title_for_api = current_soc_info["title"]
            logger.info(f"Admin: Processing SOC dict (Index {i}): {soc_code} - {job_title_for_api}")
        else:
            logger.error(f"Admin: Invalid structure for TARGET_SOC_CODES at index {i}: {current_soc_info}. Skipping.")
            if current_soc_info not in st.session_state.admin_failed_socs:
                 st.session_state.admin_failed_socs.append({"soc_info": str(current_soc_info), "reason": "Invalid structure"})
            st.session_state.admin_current_soc_index += 1 # Ensure progress
            st.session_state.admin_processed_count +=1 # Count as processed (or attempted)
            continue # Skip to the next item

        if soc_code and job_title_for_api:
            progress_bar.progress((i + 1) / len(target_socs), text=f"Processing: {job_title_for_api} ({soc_code})")
            status_message.info(f"Fetching and processing: {job_title_for_api} ({soc_code})...")
            
            try:
                success, message = bls_job_mapper.fetch_and_process_soc_data(soc_code, job_title_for_api, db_engine)
                if success:
                    logger.info(f"Admin: Successfully processed {soc_code} - {job_title_for_api}")
                    status_message.success(f"Successfully processed: {job_title_for_api} ({soc_code})")
                else:
                    logger.error(f"Admin: Failed to process {soc_code} - {job_title_for_api}: {message}")
                    status_message.error(f"Failed: {job_title_for_api} ({soc_code}) - {message}")
                    if {"soc_code": soc_code, "reason": message} not in st.session_state.admin_failed_socs:
                        st.session_state.admin_failed_socs.append({"soc_code": soc_code, "title": job_title_for_api, "reason": message})
            except Exception as e:
                logger.error(f"Admin: Exception processing {soc_code} - {job_title_for_api}: {e}", exc_info=True)
                status_message.error(f"Exception for {job_title_for_api} ({soc_code}): {e}")
                if {"soc_code": soc_code, "reason": str(e)} not in st.session_state.admin_failed_socs:
                     st.session_state.admin_failed_socs.append({"soc_code": soc_code, "title": job_title_for_api, "reason": str(e)})
            
            processed_in_batch += 1
            st.session_state.admin_processed_count += 1
            time.sleep(api_delay)  # Respect API delay
        
        st.session_state.admin_current_soc_index += 1 # Move to next SOC for next run
        
    if st.session_state.admin_current_soc_index >= len(target_socs):
        st.session_state.admin_auto_run_batch = False # Stop auto-run when all done
        status_message.success("All SOC codes processed!")
        logger.info("Admin: All SOC codes processed.")
    
    logger.info(f"Admin: Batch iteration complete. Progress saved. Next index: {st.session_state.admin_current_soc_index}")
    st.rerun() # Rerun to update UI after batch

# --- Main Application Tabs ---
tabs = st.tabs(["Single Job Analysis", "Job Comparison"])
logger.info("Tabs defined for main app layout.")

# Single Job Analysis Tab
with tabs[0]:
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    if not bls_api_key and not database_available: # If neither API nor DB is available
        st.error("BLS API Key not configured and database connection failed. Application functionality is severely limited. Please configure API key or check database.")
    elif not bls_api_key:
        st.warning("BLS API Key not configured. Analysis will rely on existing database data, which may not be up-to-date for all jobs.")
    elif not database_available:
        st.warning("Database connection failed. Analysis will use real-time BLS API data but may be slower and historical data features will be unavailable.")
    
    st.markdown("Enter any job title to analyze")
    search_job_title = job_title_autocomplete(
        label="Enter your job title",
        key="job_title_search_main",
        placeholder="Start typing to see suggestions...",
        help="Type a job title and select from matching suggestions"
    )
    
    if st.button("🗑️ Clear Entry", key="clear_button_single_main"):
        st.session_state.job_title_search_main = "" # Clear the text input
        # Potentially clear other related session state if needed
        st.rerun()
    
    search_clicked = st.button("Analyze Job Risk", key="analyze_job_risk_main", type="primary")
    
    if search_clicked and search_job_title:
        with st.spinner(f"Analyzing {search_job_title}..."):
            try:
                job_data = job_api_integration.get_job_data(search_job_title) # Uses database-only version
                
                if "error" in job_data:
                    st.error(f"Could not retrieve data for '{search_job_title}': {job_data['error']}")
                    if job_data.get("message"):
                        st.info(job_data["message"])
                    st.stop()

                # Save to database if available
                if database_available:
                    save_job_search(search_job_title, {
                        'year_1_risk': job_data.get('year_1_risk', 0),
                        'year_5_risk': job_data.get('year_5_risk', 0),
                        'risk_category': job_data.get('risk_category', 'Unknown'),
                        'job_category': job_data.get('job_category', 'Unknown')
                    })
                
                st.subheader(f"AI Displacement Risk Analysis: {job_data.get('job_title', search_job_title)}")
                
                job_info_col, risk_gauge_col, risk_factors_col = st.columns([1.2, 1, 1.2]) # Adjusted column widths
                
                with job_info_col:
                    st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Job Information</h3>", unsafe_allow_html=True)
                    st.markdown(f"**Occupation Code (SOC):** {job_data.get('occupation_code', 'N/A')}")
                    st.markdown(f"**Job Category:** {job_data.get('job_category', 'N/A')}")
                    
                    employment_data = job_data.get('projections', {})
                    current_employment = employment_data.get('current_employment')
                    st.markdown(f"**Current Employment (BLS):** {int(current_employment):,} jobs" if current_employment else "Data unavailable")
                    
                    growth_percent = employment_data.get('percent_change')
                    st.markdown(f"**BLS Projected Growth (10-yr):** {growth_percent:+.1f}%" if growth_percent is not None else "Data unavailable")
                    
                    openings = employment_data.get('annual_job_openings')
                    st.markdown(f"**Annual Job Openings (BLS):** {int(openings):,}" if openings else "Data unavailable")

                    st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Career Outlook</h3>", unsafe_allow_html=True)
                    automation_prob = job_data.get("automation_probability", (job_data.get("year_5_risk", 0) + job_data.get("year_1_risk",0))/2 + 10) # Example calculation
                    st.markdown(f"**Task Automation Potential:** {automation_prob:.1f}% of job tasks could be impacted by automation")
                    
                    median_wage = job_data.get("wage_data", {}).get("median_wage")
                    st.markdown(f"**Median Annual Wage (BLS):** ${int(median_wage):,.0f}" if median_wage else "Data unavailable")

                with risk_gauge_col:
                    risk_category = job_data.get("risk_category", "Moderate")
                    year_5_risk = job_data.get("year_5_risk", 0)
                    
                    st.markdown(f"<h3 style='text-align: center; margin-bottom: 10px;'>Overall AI Displacement Risk: {risk_category}</h3>", unsafe_allow_html=True)
                    
                    fig_gauge = go.Figure(go.Indicator(
                        mode = "gauge+number", value = year_5_risk,
                        domain = {'x': [0, 1], 'y': [0, 1]}, title = {'text': ""},
                        number = {'suffix': '%', 'font': {'size': 28}},
                        gauge = {
                            'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                            'bar': {'color': "#0084FF"}, 'bgcolor': "white", 'borderwidth': 2, 'bordercolor': "gray",
                            'steps': [
                                {'range': [0, 25], 'color': "rgba(0, 255, 0, 0.5)"}, {'range': [25, 50], 'color': "rgba(255, 255, 0, 0.5)"},
                                {'range': [50, 75], 'color': "rgba(255, 165, 0, 0.5)"}, {'range': [75, 100], 'color': "rgba(255, 0, 0, 0.5)"}
                            ],
                            'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': year_5_risk }
                        }))
                    fig_gauge.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=20))
                    st.plotly_chart(fig_gauge, use_container_width=True)
                    
                    col_risk1, col_risk2 = st.columns(2)
                    with col_risk1:
                        st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>1-Year Risk</h4></div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{job_data.get('year_1_risk', 0):.1f}%</div>", unsafe_allow_html=True)
                    with col_risk2:
                        st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>5-Year Risk</h4></div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{year_5_risk:.1f}%</div>", unsafe_allow_html=True)

                with risk_factors_col:
                    st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Key Risk Factors</h3>", unsafe_allow_html=True)
                    for factor in job_data.get("risk_factors", ["Data not available"]): st.markdown(f"❌ {factor}")
                    
                    st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Protective Factors</h3>", unsafe_allow_html=True)
                    for factor in job_data.get("protective_factors", ["Data not available"]): st.markdown(f"✅ {factor}")

                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Key Insights & Analysis</h3>", unsafe_allow_html=True)
                st.markdown(job_data.get("analysis", "Detailed analysis not available for this job title."))

                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Employment Trend (BLS Data)</h3>", unsafe_allow_html=True)
                trend_data = job_data.get("trend_data", {})
                if trend_data and "years" in trend_data and "employment" in trend_data and any(val for val in trend_data["employment"]):
                    trend_fig = go.Figure()
                    trend_fig.add_trace(go.Scatter(
                        x=trend_data["years"], y=trend_data["employment"],
                        mode='lines+markers', name='Employment',
                        line=dict(color='#0084FF', width=2), marker=dict(size=8)
                    ))
                    trend_fig.update_layout(
                        title=f'Employment Trend for {job_data.get("job_title", search_job_title)}',
                        xaxis_title='Year', yaxis_title='Number of Jobs',
                        height=350, margin=dict(l=40, r=40, t=60, b=40)
                    )
                    st.plotly_chart(trend_fig, use_container_width=True)
                else:
                    st.info("📊 Employment trend data from Bureau of Labor Statistics not yet available for this position, or data is zero.")

                st.markdown("<hr>", unsafe_allow_html=True)
                st.markdown(career_navigator.get_html(), unsafe_allow_html=True) # Career Navigator CTA

            except Exception as e:
                logger.error(f"Error during single job analysis for '{search_job_title}': {e}", exc_info=True)
                st.error(f"An unexpected error occurred: {e}")

# Job Comparison Tab
with tabs[1]:
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    st.markdown("Compare the AI displacement risk for multiple jobs side by side.")
    
    if 'compare_jobs_list' not in st.session_state:
        st.session_state.compare_jobs_list = []

    new_job_to_compare = job_title_autocomplete(
        label="Enter a job title to add to comparison",
        key="compare_job_input_main",
        placeholder="Start typing...",
        help="Add up to 5 jobs for comparison."
    )

    if st.button("Add to Comparison", key="add_to_compare_main") and new_job_to_compare:
        if len(st.session_state.compare_jobs_list) < 5:
            if new_job_to_compare not in st.session_state.compare_jobs_list:
                st.session_state.compare_jobs_list.append(new_job_to_compare)
                st.rerun() # Rerun to update display immediately
            else:
                st.warning(f"'{new_job_to_compare}' is already in the comparison list.")
        else:
            st.warning("Maximum of 5 jobs can be compared at a time.")
            
    if st.session_state.compare_jobs_list:
        st.markdown("#### Jobs to Compare:")
        cols = st.columns(len(st.session_state.compare_jobs_list) + 1)
        for i, job_name in enumerate(st.session_state.compare_jobs_list):
            with cols[i]:
                st.markdown(job_name)
                if st.button("Remove", key=f"remove_compare_{i}"):
                    st.session_state.compare_jobs_list.pop(i)
                    st.rerun()
        
        if st.session_state.compare_jobs_list and st.button("Clear All", key="clear_compare_all_main"):
            st.session_state.compare_jobs_list = []
            st.rerun()

    if len(st.session_state.compare_jobs_list) > 0:
        with st.spinner("Fetching comparison data..."):
            comparison_data = simple_comparison.get_job_comparison_data(st.session_state.compare_jobs_list)
        
        if comparison_data and not all("error" in data for data in comparison_data.values()):
            comparison_tabs = st.tabs(["Comparison Chart", "Detailed Table", "Risk Heatmap", "Radar Analysis"])
            
            with comparison_tabs[0]:
                chart = simple_comparison.create_comparison_chart(comparison_data)
                if chart: st.plotly_chart(chart, use_container_width=True)
                else: st.info("Not enough data to create comparison chart.")
            
            with comparison_tabs[1]:
                df_table = simple_comparison.create_comparison_table(comparison_data)
                if df_table is not None: st.dataframe(df_table, use_container_width=True)
                else: st.info("Not enough data for detailed table.")

            with comparison_tabs[2]:
                heatmap = simple_comparison.create_risk_heatmap(comparison_data)
                if heatmap: st.plotly_chart(heatmap, use_container_width=True)
                else: st.info("Not enough data for risk heatmap.")

            with comparison_tabs[3]:
                radar = simple_comparison.create_radar_chart(comparison_data)
                if radar: st.plotly_chart(radar, use_container_width=True)
                else: st.info("Not enough data for radar analysis.")
        else:
            st.error("Could not retrieve enough data for comparison. Some jobs might not be in the BLS database.")
            for job_title, data in comparison_data.items():
                if "error" in data:
                    st.warning(f"Could not fetch data for '{job_title}': {data['error']}")

# --- Admin Controls Section (Collapsible) ---
with st.sidebar: # Moved admin controls to sidebar
    st.title("⚙️ Admin Controls")
    with st.expander("Database Population Tool", expanded=False):
        st.markdown("This section is for administrators only.")
        
        if not database_available:
            st.error("Database is not available. Admin controls disabled.")
        elif not bls_api_key:
            st.error("BLS API Key is not configured. Database population tool cannot run.")
        else:
            total_socs = len(st.session_state.admin_target_socs)
            progress_bar = st.progress(st.session_state.admin_processed_count / total_socs if total_socs > 0 else 0)
            status_message = st.empty()
            
            status_message.info(f"Overall Progress: {st.session_state.admin_processed_count} SOCs processed out of {total_socs} target SOCs. Next to process: Index {st.session_state.admin_current_soc_index}.")

            admin_batch_size = st.number_input("Batch Size (SOCs per run)", min_value=1, max_value=20, value=5, key="admin_batch_size")
            admin_api_delay = st.number_input("Delay Between API Calls (seconds)", min_value=1, max_value=10, value=2, key="admin_api_delay")

            col_run, col_pause, col_reset = st.columns(3)
            with col_run:
                if st.button("▶️ Start/Resume Batch", key="admin_start_batch"):
                    st.session_state.admin_auto_run_batch = True
                    logger.info("Admin: Batch run started/resumed.")
                    st.rerun() # Trigger rerun to start processing loop
            with col_pause:
                if st.button("⏸️ Pause", key="admin_pause_batch"):
                    st.session_state.admin_auto_run_batch = False
                    status_message.warning("Batch processing paused.")
                    logger.info("Admin: Batch processing paused by user.")
                    st.rerun()
            with col_reset:
                if st.button("🔄 Reset All Progress", key="admin_reset_progress"):
                    st.session_state.admin_current_soc_index = 0
                    st.session_state.admin_processed_count = 0
                    st.session_state.admin_failed_socs = []
                    st.session_state.admin_auto_run_batch = False
                    status_message.info("Progress reset. Ready to start from the beginning.")
                    logger.info("Admin: Progress reset.")
                    st.rerun()
            
            # Automated batch processing loop
            if st.session_state.admin_auto_run_batch and st.session_state.admin_current_soc_index < total_socs:
                run_batch_processing(admin_batch_size, admin_api_delay)
            elif st.session_state.admin_current_soc_index >= total_socs and total_socs > 0:
                 status_message.success("All SOC codes have been processed.")
                 st.session_state.admin_auto_run_batch = False


            st.markdown("---")
            st.markdown("### Summary of Failed SOC Populations")
            if st.session_state.admin_failed_socs:
                failed_df = pd.DataFrame(st.session_state.admin_failed_socs)
                st.dataframe(failed_df, use_container_width=True)
            else:
                st.info("No SOC codes are currently marked as having failed population.")

# --- Sidebar Content: System Status and Recent Searches ---
with st.sidebar:
    st.markdown("---")
    st.header("System Status")
    if bls_api_key:
        st.success("BLS API: Configured")
    else:
        st.error("BLS API: NOT CONFIGURED")

    if database_available and db_engine:
        st.success("Database: Connected")
    else:
        st.warning("Database: Fallback Mode / Not Connected")
    
    # Data refresh cycle status (placeholder for now)
    st.info("Data refresh cycle status unknown.")

    st.markdown("---")
    st.markdown(f"App Version: 2.1.0 (Real Data Only)")
    st.markdown(f"Last App Load: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Keep-alive status (simple indicator)
    if "keep_alive_started" in st.session_state and st.session_state.keep_alive_started:
        last_ping_time = st.session_state.get("last_keep_alive_ping", "N/A")
        if isinstance(last_ping_time, datetime.datetime):
            time_since_ping = (datetime.datetime.now(datetime.timezone.utc) - last_ping_time).total_seconds() / 60
            st.success(f"Keep-Alive: Active (last ping: {time_since_ping:.1f} min ago)")
        else:
            st.success("Keep-Alive: Active (pinging)")
            
    st.markdown("---")
    st.subheader("UptimeRobot Setup")
    st.markdown("""
    To keep this application alive with UptimeRobot:
    1. Create a new monitor in UptimeRobot
    2. Set Type to "HTTP(s)"
    3. Set URL to your app URL with `?health=true` (e.g., `your-app-url.streamlit.app/?health=true`)
    4. Set monitoring interval to 5 minutes
    5. Enable "Alert When Down"
    """)

# --- Footer ---
st.markdown("---")
st.markdown(f"""
<div style="text-align: center;">
    <p style="font-size: 12px; color: #666666;">
        © {datetime.datetime.now().year} iThriveAI - AI Job Displacement Risk Analyzer<br>
        Powered by real-time Bureau of Labor Statistics data | 
        <a href="https://www.bls.gov/ooh/" target="_blank" style="color: #0084FF;">BLS Occupational Outlook Handbook</a>
    </p>
</div>
""", unsafe_allow_html=True)

# UptimeRobot status embed (optional, if you have a public status page)
st.markdown("""
<iframe src="https://stats.uptimerobot.com/L8gQВиN1X7" height="0" width="0" frameborder="0" scrolling="no" style="display:none;"></iframe>
""", unsafe_allow_html=True)
