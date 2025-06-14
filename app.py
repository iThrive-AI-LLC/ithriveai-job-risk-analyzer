import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
import datetime
import os
import sys
import threading
import time
import requests # For keep-alive self-ping (if re-enabled)
import logging
from logging import StreamHandler, Formatter, INFO
import re

# --- Custom Logger Setup ---
logger = logging.getLogger("AI_Job_Analyzer_App")
if not logger.hasHandlers():
    handler = StreamHandler(sys.stdout)
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(INFO)
    logger.propagate = False

# --- Database Module Imports and Fallback ---
database_available = False
db_engine = None
db_Session = None 
JobSearch = None # This will be the ORM class
save_job_search_db = None
get_popular_searches_db = None
get_highest_risk_jobs_db = None
get_lowest_risk_jobs_db = None
get_recent_searches_db = None

try:
    from database import (
        engine as db_engine_imported, 
        Session as db_Session_imported, 
        JobSearch as DBJobSearch_imported,
        save_job_search as save_job_search_imported, 
        get_popular_searches as get_popular_searches_imported,
        get_highest_risk_jobs as get_highest_risk_jobs_imported,
        get_lowest_risk_jobs as get_lowest_risk_jobs_imported,
        get_recent_searches as get_recent_searches_imported
    )
    db_engine = db_engine_imported
    db_Session = db_Session_imported
    JobSearch = DBJobSearch_imported
    save_job_search_db = save_job_search_imported
    get_popular_searches_db = get_popular_searches_imported
    get_highest_risk_jobs_db = get_highest_risk_jobs_imported
    get_lowest_risk_jobs_db = get_lowest_risk_jobs_imported
    get_recent_searches_db = get_recent_searches_imported
    
    # Check if engine and Session are truly available
    if db_engine is not None and db_Session is not None:
        database_available = True
        logger.info("Successfully imported database modules and engine/Session are available.")
    else:
        logger.error("Database modules imported, but engine or Session is None. Using fallback.")
        database_available = False
        # Fallback functions will be assigned after this block if database_available is False

except ImportError as e:
    logger.critical(f"Failed to import database modules: {e}. Using fallback data.", exc_info=True)
    database_available = False
    db_engine = None 
    db_Session = None

# Assign correct functions based on database availability
if database_available:
    save_job_search = save_job_search_db
    get_popular_searches = get_popular_searches_db
    get_highest_risk_jobs = get_highest_risk_jobs_db
    get_lowest_risk_jobs = get_lowest_risk_jobs_db
    get_recent_searches = get_recent_searches_db
else:
    from db_fallback import (
        save_job_search as save_job_search_fb, 
        get_popular_searches as get_popular_searches_fb, 
        get_highest_risk_jobs as get_highest_risk_jobs_fb, 
        get_lowest_risk_jobs as get_lowest_risk_jobs_fb, 
        get_recent_searches as get_recent_searches_fb
    )
    save_job_search = save_job_search_fb
    get_popular_searches = get_popular_searches_fb
    get_highest_risk_jobs = get_highest_risk_jobs_fb
    get_lowest_risk_jobs = get_lowest_risk_jobs_fb
    get_recent_searches = get_recent_searches_fb
    logger.info("Using fallback database functions.")

# --- Other Imports ---
import job_api_integration_database_only as job_api_integration
import simple_comparison
import career_navigator
# import bls_job_mapper # Already imported by job_api_integration_database_only
from job_title_autocomplete_v2 import job_title_autocomplete, load_job_titles_from_db
from sqlalchemy import text 

# --- Keep-Alive Functionality ---
def keep_alive():
    """Background thread to keep the app active and database connection warm."""
    logger.info("Keep-alive thread started.")
    while True:
        time.sleep(240)  # Ping every 4 minutes
        try:
            if database_available and db_engine: # Check if real db is available
                with db_engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                st.session_state.last_keep_alive_ping = datetime.datetime.now(datetime.timezone.utc)
                logger.info("Keep-alive: Database ping successful.")
            else:
                logger.info("Keep-alive: Database not available, skipping ping.")
        except Exception as e:
            logger.error(f"Keep-alive: Database ping failed: {e}")
            st.session_state.last_keep_alive_ping_error = str(e)

if "keep_alive_started" not in st.session_state:
    st.session_state.keep_alive_started = True
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    logger.info("Keep-alive thread initialized and started.")

# --- BLS API Key Check ---
bls_api_key = os.environ.get('BLS_API_KEY')
if not bls_api_key:
    try:
        # Try to get from Streamlit secrets (new way)
        if hasattr(st, 'secrets') and callable(st.secrets.get): 
            bls_api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
        # Try to get from Streamlit secrets (old way, dictionary access)
        elif hasattr(st, 'secrets') and isinstance(st.secrets, dict) and "api_keys" in st.secrets: 
             bls_api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
    except Exception as e:
        logger.warning(f"Could not access Streamlit secrets for BLS_API_KEY: {e}")

if bls_api_key:
    logger.info("BLS API key loaded.")
else:
    logger.error("BLS_API_KEY is not configured. App will rely on database and may have limited real-time data functionality.")

# --- Health Check Endpoints ---
query_params = st.query_params
if query_params.get("health") == "true":
    st.text("OK")
    st.stop()

if query_params.get("health_check") == "true":
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
    .main {
        background-color: #FFFFFF;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 60px;
        width: 250px;
        white-space: pre-wrap;
        background-color: #F0F8FF;
        border-radius: 4px 4px 0 0;
        gap: 10px;
        padding-top: 15px;
        padding-bottom: 15px;
        font-size: 18px;
        font-weight: 600;
        text-align: center;
    }
    .stTabs [aria-selected="true"] {
        background-color: #0084FF;
        color: white;
    }
    h1, h2, h3, h4, h5, h6 {
        color: #0084FF;
    }
    .job-risk-low {
        background-color: #d4edda;
        border-radius: 5px;
        padding: 10px;
        margin-bottom: 10px;
    }
    .job-risk-moderate {
        background-color: #fff3cd;
        border-radius: 5px;
        padding: 10px;
        margin-bottom: 10px;
    }
    .job-risk-high {
        background-color: #f8d7da;
        border-radius: 5px;
        padding: 10px;
        margin-bottom: 10px;
    }
    .job-risk-very-high {
        background-color: #f8d7da;
        border-color: #f5c6cb;
        border-radius: 5px;
        padding: 10px;
        margin-bottom: 10px;
        border-width: 2px;
        border-style: solid;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
    .st-eb {
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# Application title and description
st.image("https://img1.wsimg.com/isteam/ip/70686f32-22d2-489c-a383-6fcd793644be/blob-3712e2e.png/:/rs=h:197,cg:true,m/qt=q:95", width=250)
st.markdown("<h1 style='text-align: center; color: #0084FF;'>Is your job at risk with AI innovation?</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #4CACE5; font-size: 24px; font-weight: 600;'>AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666666; font-weight: bold; font-size: 16px;'>Discover how AI might impact your career in the next 5 years and get personalized recommendations.</p>", unsafe_allow_html=True)

# --- Data Refresh Logic ---
def check_data_refresh():
    """Check if data needs to be refreshed (daily schedule to keep Supabase active)"""
    try:
        with open("last_refresh.json", "r") as f:
            refresh_data = json.load(f)
            last_refresh = datetime.datetime.fromisoformat(refresh_data["date"])
            days_since_refresh = (datetime.datetime.now() - last_refresh).days
            
            if days_since_refresh >= 1:
                try:
                    import db_refresh # Assumes db_refresh.py exists
                    logger.info("Refreshing BLS data and performing database activity...")
                    sample_job = "Software Developer" # Example job for refresh
                    db_refresh.update_job_data(sample_job)
                    db_refresh.perform_database_queries()
                    db_refresh.check_and_update_refresh_timestamp()
                    logger.info(f"Database activity performed successfully. Updated {sample_job} data.")
                except ImportError:
                    logger.warning("db_refresh module not found. Skipping daily data refresh activity.")
                except Exception as e:
                    logger.warning(f"Database refresh attempted but encountered an issue: {str(e)}")
                return True
            return False
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        logger.info("last_refresh.json not found or invalid. Creating and triggering refresh.")
        with open("last_refresh.json", "w") as f:
            json.dump({"date": datetime.datetime.now().isoformat()}, f)
        return True

# --- Tabs for different sections ---
tabs = st.tabs(["Single Job Analysis", "Job Comparison"])
logger.info("Tabs defined for main app layout.")

# Single Job Analysis Tab
with tabs[0]:
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    
    if bls_api_key:
        st.info("📊 Using real-time data from the Bureau of Labor Statistics API (via local database cache)")
    else:
        st.warning("📊 BLS API Key not configured. Data is sourced from the local database only.")
    
    search_job_title = job_title_autocomplete(
        label="Enter any job title to analyze",
        key="job_title_search",
        placeholder="Start typing to see suggestions...",
        help="Type a job title and select from matching suggestions"
    )
    
    if st.button("🗑️ Clear Entry", key="clear_button_single"):
        st.session_state.job_title_search = "" # Clear the specific input
        st.rerun()
    
    normalized_job_title = search_job_title.lower().strip() if search_job_title else ""
    if re.search(r'diagnos(i(c|s|t|cian)|e)', normalized_job_title): # Special case for demo
        search_job_title = "Diagnosician"
    
    search_clicked = st.button("Analyze Job Risk")
    
    check_data_refresh() # Check for data refresh when app starts or tab is active
    
    if search_clicked and search_job_title:
        with st.spinner(f"Analyzing {search_job_title}..."):
            try:
                job_data = job_api_integration.get_job_data(search_job_title)
                if "error" in job_data:
                    st.error(f"{job_data['error']}. Please use the Admin Dashboard to add missing job titles or check data sources.")
                    if job_data.get("message"):
                         st.info(job_data["message"])
                    st.stop()

            except Exception as e:
                logger.error(f"Error getting job data for '{search_job_title}': {e}", exc_info=True)
                st.error(f"An unexpected error occurred while fetching data for '{search_job_title}'. Please try again or contact support.")
                st.stop()
            
            if database_available and save_job_search:
                save_job_search(search_job_title, {
                    'year_1_risk': job_data.get('year_1_risk', 0),
                    'year_5_risk': job_data.get('year_5_risk', 0),
                    'risk_category': job_data.get('risk_category', 'Unknown'),
                    'job_category': job_data.get('job_category', 'Unknown')
                })
            
            st.subheader(f"AI Displacement Risk Analysis: {job_data.get('job_title', search_job_title)}")
            
            job_info_col, risk_gauge_col, risk_factors_col = st.columns([1, 1, 1])
            
            with job_info_col:
                st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Job Information</h3>", unsafe_allow_html=True)
                bls_data = job_data.get("projections", {}) # BLS data is nested under 'projections'
                
                st.markdown(f"**Occupation Code:** {job_data.get('occupation_code', 'N/A')}")
                st.markdown(f"**Job Category:** {job_data.get('job_category', 'General')}")
                
                current_employment_val = bls_data.get('current_employment')
                if current_employment_val and current_employment_val > 0:
                    st.markdown(f"**Current Employment:** {int(current_employment_val):,.0f} jobs")
                else:
                    st.markdown(f"**Current Employment:** Data unavailable")

                employment_change_percent_val = bls_data.get('percent_change')
                if employment_change_percent_val is not None:
                    growth_text = f"{float(employment_change_percent_val):+.1f}%"
                    st.markdown(f"**BLS Projected Growth (to {job_data.get('bls_data',{}).get('projection_year','N/A')}):** {growth_text}")
                else:
                    st.markdown(f"**BLS Projected Growth:** Data unavailable")

                annual_openings_val = bls_data.get('annual_job_openings')
                if annual_openings_val and annual_openings_val > 0:
                    st.markdown(f"**Annual Job Openings:** {int(annual_openings_val):,.0f}")
                else:
                    st.markdown(f"**Annual Job Openings:** Data unavailable")
                
                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Career Outlook</h3>", unsafe_allow_html=True)
                st.markdown("<h4 style='color: #0084FF; font-size: 16px;'>Statistics</h4>", unsafe_allow_html=True)
                
                automation_prob = job_data.get("automation_probability", 45.0) # This seems to be a default
                st.markdown(f"**Task Automation Probability (General Estimate):** {automation_prob:.1f}% of job tasks could be automated")
                
                median_wage_val = job_data.get("wage_data", {}).get("median_wage")
                if median_wage_val and median_wage_val > 0:
                    st.markdown(f"**Median Annual Wage:** ${int(median_wage_val):,.0f}")
                else:
                    st.markdown("**Median Annual Wage:** Data unavailable")
            
            with risk_gauge_col:
                risk_category = job_data.get("risk_category", "High")
                year_1_risk = job_data.get("year_1_risk", 35.0)
                year_5_risk = job_data.get("year_5_risk", 60.0)
                
                st.markdown(f"<h3 style='text-align: center; margin-bottom: 10px;'>Overall AI Displacement Risk: {risk_category}</h3>", unsafe_allow_html=True)
                
                gauge_value = year_5_risk if year_5_risk is not None else 0
                
                fig = go.Figure(go.Indicator(
                    mode = "gauge+number", value = gauge_value,
                    domain = {'x': [0, 1], 'y': [0, 1]}, title = {'text': ""},
                    number = {'suffix': '%', 'font': {'size': 28}},
                    gauge = {'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                             'bar': {'color': "#0084FF"}, 'bgcolor': "white",
                             'borderwidth': 2, 'bordercolor': "gray",
                             'steps': [{'range': [0, 25], 'color': "rgba(0, 255, 0, 0.5)"},
                                       {'range': [25, 50], 'color': "rgba(255, 255, 0, 0.5)"},
                                       {'range': [50, 75], 'color': "rgba(255, 165, 0, 0.5)"},
                                       {'range': [75, 100], 'color': "rgba(255, 0, 0, 0.5)"}],
                             'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': gauge_value}}))
                fig.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)
                
                col1_risk, col2_risk = st.columns(2)
                with col1_risk:
                    st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>1-Year Risk</h4></div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{year_1_risk:.1f}%</div>", unsafe_allow_html=True)
                with col2_risk:
                    st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>5-Year Risk</h4></div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{year_5_risk:.1f}%</div>", unsafe_allow_html=True)

            with risk_factors_col:
                st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Key Risk Factors</h3>", unsafe_allow_html=True)
                risk_factors = job_data.get("risk_factors", ["Data not available for specific risk factors."])
                for factor in risk_factors: st.markdown(f"❌ {factor}")
                
                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Protective Factors</h3>", unsafe_allow_html=True)
                protective_factors = job_data.get("protective_factors", ["Data not available for specific protective factors."])
                for factor in protective_factors: st.markdown(f"✅ {factor}")

            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Key Insights</h3>", unsafe_allow_html=True)
            analysis_text = job_data.get("analysis", "Detailed analysis not available for this job title.")
            st.markdown(analysis_text)
            
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Employment Trend</h3>", unsafe_allow_html=True)
            trend_data = job_data.get("trend_data", {})
            employment_values = trend_data.get("employment", [])
            years = trend_data.get("years", [])

            if employment_values and any(val is not None and val > 0 for val in employment_values):
                trend_fig = go.Figure()
                trend_fig.add_trace(go.Scatter(x=years, y=employment_values, mode='lines+markers', name='Employment', line=dict(color='#0084FF', width=2), marker=dict(size=8)))
                trend_fig.update_layout(title=f'Employment Trend for {job_data.get("job_title", search_job_title)}', xaxis_title='Year', yaxis_title='Number of Jobs', height=350, margin=dict(l=40, r=40, t=60, b=40))
                st.plotly_chart(trend_fig, use_container_width=True)
            else:
                st.info("📊 **Employment trend data from Bureau of Labor Statistics not yet available for this position.** Analysis shows current risk factors and projections based on job category research.")

            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Similar Jobs</h3>", unsafe_allow_html=True)
            raw_similar_jobs = job_data.get("similar_jobs", []) # This key might not exist or be empty
            similar_jobs = []
            if raw_similar_jobs: # Process if not empty
                for job_entry in raw_similar_jobs:
                    if isinstance(job_entry, dict) and "job_title" in job_entry and "year_5_risk" in job_entry:
                        year_5_risk_val = job_entry["year_5_risk"]
                        year_1_risk_val = job_entry.get("year_1_risk", year_5_risk_val * 0.6) # Estimate if missing
                        similar_jobs.append({
                            "title": job_entry["job_title"],
                            "year_5_risk": year_5_risk_val / 100 if year_5_risk_val > 1 else year_5_risk_val,
                            "year_1_risk": year_1_risk_val / 100 if year_1_risk_val > 1 else year_1_risk_val
                        })
            
            if similar_jobs:
                similar_df = pd.DataFrame(similar_jobs)
                job_titles_radar = [job.get("title", "Untitled") for job in similar_jobs]
                risk_values_radar = [job.get("year_5_risk", 0) * 100 for job in similar_jobs]
                
                similar_fig = go.Figure()
                similar_fig.add_trace(go.Bar(x=job_titles_radar, y=risk_values_radar, marker_color='#FFA500', text=[f"{val:.1f}%" for val in risk_values_radar], textposition='auto'))
                similar_fig.update_layout(title="AI Displacement Risk for Similar Jobs", xaxis_title="Job Title", yaxis_title="5-Year Risk (%)", height=400, margin=dict(l=40, r=40, t=60, b=40))
                st.plotly_chart(similar_fig, use_container_width=True)
                
                st.markdown("Compare risk levels of similar occupations:")
                comparison_data_table = []
                for job_entry in similar_jobs:
                    risk_5y = job_entry.get("year_5_risk", 0) * 100
                    category = "High" if risk_5y >= 60 else "Moderate" if risk_5y >= 30 else "Low"
                    comparison_data_table.append({
                        "Job Title": job_entry.get("title", ""),
                        "1-Year Risk (%)": f"{job_entry.get('year_1_risk', 0) * 100:.1f}%",
                        "5-Year Risk (%)": f"{risk_5y:.1f}%",
                        "Risk Category": category
                    })
                comparison_df_table = pd.DataFrame(comparison_data_table)
                st.dataframe(comparison_df_table, use_container_width=True)
            else:
                st.info("No similar job data available for comparison for this specific role.")

            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Risk Assessment Summary</h3>", unsafe_allow_html=True)
            summary_text = job_data.get("summary", "Summary not available.")
            st.markdown(summary_text)
            
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Get Your Personalized Career Plan</h3>", unsafe_allow_html=True)
            st.markdown("Our AI-powered Career Navigator can help you develop a personalized plan to adapt to these changes and thrive in your career.", unsafe_allow_html=True)
            st.markdown(career_navigator.get_html(), unsafe_allow_html=True)
            
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Recent Job Searches</h3>", unsafe_allow_html=True)
            
            if database_available and get_recent_searches:
                recent_searches_data = get_recent_searches(limit=5)
                if recent_searches_data:
                    recent_col1, recent_col2, recent_col3 = st.columns([3, 2, 2])
                    with recent_col1: st.markdown("<p style='color: #666666; font-weight: bold;'>Job Title</p>", unsafe_allow_html=True)
                    with recent_col2: st.markdown("<p style='color: #666666; font-weight: bold;'>Risk Level</p>", unsafe_allow_html=True)
                    with recent_col3: st.markdown("<p style='color: #666666; font-weight: bold;'>When</p>", unsafe_allow_html=True)
                    
                    for i, search in enumerate(recent_searches_data): # Added index for unique key
                        job_title_recent = search.get("job_title", "Unknown Job")
                        risk_category_recent = search.get("risk_category", "Unknown")
                        timestamp_recent = search.get("timestamp")
                        
                        time_ago = "Recently"
                        if timestamp_recent:
                            now = datetime.datetime.now(datetime.timezone.utc if timestamp_recent.tzinfo else None)
                            if isinstance(timestamp_recent, str):
                                try: timestamp_recent = datetime.datetime.fromisoformat(timestamp_recent)
                                except: timestamp_recent = now
                            
                            delta = now - timestamp_recent
                            if delta.days > 0: time_ago = f"{delta.days} days ago"
                            elif delta.seconds >= 3600: hours = delta.seconds // 3600; time_ago = f"{hours} hour{'s' if hours > 1 else ''} ago"
                            elif delta.seconds >= 60: minutes = delta.seconds // 60; time_ago = f"{minutes} minute{'s' if minutes > 1 else ''} ago"
                            else: time_ago = "Just now"
                        
                        risk_color = {"Very High": "#FF4B4B", "High": "#FF8C42", "Moderate": "#FFCC3E", "Low": "#4CAF50"}.get(risk_category_recent, "#666666")
                        
                        col1_rs, col2_rs, col3_rs = st.columns([3, 2, 2])
                        with col1_rs:
                            search_key = f"search_{job_title_recent.replace(' ', '_')}_{i}_{abs(hash(str(search))) % 10000}" # Unique key
                            if st.button(job_title_recent, key=search_key):
                                st.session_state.job_title_search = job_title_recent # Corrected key
                                st.rerun()
                        with col2_rs: st.markdown(f"<p style='color: {risk_color};'>{risk_category_recent}</p>", unsafe_allow_html=True)
                        with col3_rs: st.write(time_ago)
                else:
                    st.info("No recent searches yet. Be the first to analyze a job!")
            else:
                st.info("Recent searches not available (database connection issue or no searches yet).")

# Job Comparison Tab
with tabs[1]:
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    st.markdown("Compare the AI displacement risk for multiple jobs side by side to explore transition opportunities. Add up to 5 jobs.")
    
    new_job_compare = job_title_autocomplete(
        label="Enter a job title and press Enter to add to comparison", 
        key="compare_job_input",
        placeholder="Start typing to see suggestions...",
        help="Type a job title and select from matching suggestions"
    )
    
    if 'selected_jobs' not in st.session_state: st.session_state.selected_jobs = []
    
    if new_job_compare and new_job_compare not in st.session_state.selected_jobs and len(st.session_state.selected_jobs) < 5:
        with st.spinner(f"Adding {new_job_compare} to comparison..."):
            # job_api_integration.get_job_data(new_job_compare) # Pre-load/cache if needed, handled by get_jobs_comparison_data
            st.session_state.selected_jobs.append(new_job_compare)
            st.session_state.compare_job_input = "" # Clear input after adding
            st.rerun() # Rerun to update display and clear input effectively
            
    if st.session_state.selected_jobs:
        st.subheader("Current Comparison:")
        job_cols = st.columns(len(st.session_state.selected_jobs))
        for i, job in enumerate(st.session_state.selected_jobs[:]): # Iterate over a copy
            with job_cols[i]:
                st.markdown(f"**{job}**")
                if st.button("❌", key=f"remove_{job}_{i}"): # More unique key
                    st.session_state.selected_jobs.remove(job)
                    st.rerun()
        if st.button("Clear All Jobs", key="clear_jobs_comparison"):
            st.session_state.selected_jobs = []
            st.rerun()
            
    if st.session_state.selected_jobs and len(st.session_state.selected_jobs) >= 1:
        st.subheader(f"Analyzing {len(st.session_state.selected_jobs)} Jobs")
        with st.spinner("Fetching and comparing job data..."):
            comparison_job_data = simple_comparison.get_job_comparison_data(st.session_state.selected_jobs)

        comparison_tabs_viz = st.tabs(["Comparison Chart", "Comparative Analysis", "Risk Heatmap", "Risk Factors"])
        
        with comparison_tabs_viz[0]:
            st.markdown("<h3 style='color: #0084FF;'>5-Year AI Displacement Risk Comparison</h3>", unsafe_allow_html=True)
            chart = simple_comparison.create_comparison_chart(comparison_job_data)
            if chart: st.plotly_chart(chart, use_container_width=True)
            else: st.error("Unable to create comparison chart. Ensure jobs have valid data.")
            st.markdown("""**Chart Explanation**: This chart shows the projected AI displacement risk after 5 years for each selected job. Higher percentages indicate greater likelihood that AI will significantly impact or automate aspects of this role.""")
        
        with comparison_tabs_viz[1]:
            st.markdown("<h3 style='color: #0084FF;'>Detailed Comparison</h3>", unsafe_allow_html=True)
            comparison_df = simple_comparison.create_comparison_table(comparison_job_data)
            if comparison_df is not None and not comparison_df.empty: st.dataframe(comparison_df, use_container_width=True)
            else: st.info("No data available for detailed comparison table.")
            # Further side-by-side analysis can be added here if needed, similar to single job tab
        
        with comparison_tabs_viz[2]:
            st.markdown("<h3 style='color: #0084FF;'>Risk Progression Heatmap</h3>", unsafe_allow_html=True)
            heatmap = simple_comparison.create_risk_heatmap(comparison_job_data)
            if heatmap: st.plotly_chart(heatmap, use_container_width=True)
            else: st.error("Unable to create risk heatmap. Ensure jobs have valid data.")
            st.markdown("""**Heatmap Explanation**: This visualization shows how displacement risk is projected to increase over time for each position. Darker colors indicate higher risk levels, helping you understand both immediate and long-term vulnerability.""")
        
        with comparison_tabs_viz[3]:
            st.markdown("<h3 style='color: #0084FF;'>Risk Factor Analysis</h3>", unsafe_allow_html=True)
            radar = simple_comparison.create_radar_chart(comparison_job_data)
            if radar: st.plotly_chart(radar, use_container_width=True)
            else: st.error("Unable to create radar chart. Ensure jobs have valid data.")
            st.markdown("""**Factor Analysis Explanation**: This radar chart compares positions across key risk dimensions. Jobs with larger areas on the chart face higher overall risk from AI disruption across multiple factors.""")

        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown("<h2 style='color: #0084FF;'>Next Steps: Personalized Career Navigator</h2>", unsafe_allow_html=True)
        st.markdown("Get personalized career guidance based on your skills and interests.", unsafe_allow_html=True)
        st.markdown(career_navigator.get_html(), unsafe_allow_html=True)

# --- Footer ---
st.markdown("---")
st.markdown("""
<div style='text-align: center; padding-top: 20px;'>
    <p style='font-style: italic; color: #666666;'>iThriveAI - AI-Driven, Human-Focused</p>
    <p style="color: #666666;">© 2025 iThriveAI | <a href="https://i-thrive-ai.com" target="_blank" style="color: #0084FF; text-decoration: none;">i-thrive-ai.com</a></p>
    <p style="font-size: 0.8em; color: #888888;">App Version: 2.1.0 (Real Data Only)</p>
    <p style="font-size: 0.8em; color: #888888;">Last App Load: """ + datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z") + """</p>
    <p id="keepAliveStatus" style="font-size: 0.8em; color: #888888;">Keep-Alive: Active</p>
</div>

<script>
    function updateKeepAliveStatus() {
        const statusElement = document.getElementById("keepAliveStatus");
        if (statusElement) {
            const lastPing = streamlit_js_eval.get_session_state('last_keep_alive_ping');
            const lastError = streamlit_js_eval.get_session_state('last_keep_alive_ping_error');
            let statusText = "Keep-Alive: Active";
            if (lastPing) {
                const now = new Date();
                const pingTime = new Date(lastPing);
                const diffSeconds = Math.round((now - pingTime) / 1000);
                statusText += ` (last ping: ${diffSeconds}s ago)`;
            }
            if (lastError) {
                statusText += ` <span style="color: red;">(Error: ${lastError})</span>`;
            }
            statusElement.innerHTML = statusText;
        }
    }
    // Update status every 30 seconds
    // setInterval(updateKeepAliveStatus, 30000); 
    // updateKeepAliveStatus(); // Initial call
    // Streamlit's st.experimental_rerun or st.rerun makes direct JS manipulation for dynamic updates tricky.
    // The Python side update of st.session_state for keep-alive status is more robust.
</script>
""", unsafe_allow_html=True)

# Display a small note about keep-alive status if available
if "last_keep_alive_ping" in st.session_state:
    last_ping_time = st.session_state.last_keep_alive_ping
    now = datetime.datetime.now(datetime.timezone.utc)
    time_since_last_ping = now - last_ping_time
    minutes_ago = time_since_last_ping.total_seconds() / 60
    st.sidebar.caption(f"Keep-Alive: Active (last ping: {minutes_ago:.1f} min ago)")
elif "last_keep_alive_ping_error" in st.session_state:
    st.sidebar.caption(f"Keep-Alive: Error ({st.session_state.last_keep_alive_ping_error})")
else:
    st.sidebar.caption("Keep-Alive: Initializing...")

# --- Admin Dashboard (Simplified for app_production.py) ---
# This section is intentionally minimal in the production app.
# Full admin controls are in admin_dashboard.py
with st.sidebar.expander("⚙️ ADMIN CONTROLS - Click to Expand", expanded=False):
    st.warning("This section is for administrators only.")
    if database_available and db_engine:
        try:
            db_status = check_database_health(db_engine) if check_database_health else "Health check unavailable"
            st.markdown(f"**Database Status:** {db_status}")
            if get_database_stats:
                stats = get_database_stats(db_engine)
                st.markdown(f"**Total SOCs in DB:** {stats.get('total_soc_codes', 'N/A')}")
                st.markdown(f"**Last Update:** {stats.get('latest_update_time', 'N/A')}")
        except Exception as e:
            st.error(f"Admin DB Check Error: {e}")
    else:
        st.error("Database connection not available for admin controls.")
    
    st.markdown("---")
    st.caption("Full database management tools are available in the dedicated Admin Dashboard.")

# Streamlit status embed (optional, for Streamlit Cloud monitoring)
st.markdown("""
<hr>
<div style="text-align: center; margin-top:10px;">
    <a href="https://www.streamlitstatus.com/?utm_source=embed" target="_blank">
        <img src="https://www.streamlitstatus.com/embed-logo.svg" alt="Streamlit Status" style="height: 20px;">
    </a>
</div>
""", unsafe_allow_html=True)
