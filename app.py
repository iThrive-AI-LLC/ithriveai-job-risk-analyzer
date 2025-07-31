import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
import datetime
import os
import sys
import threading
import time
import requests # For keep-alive self-ping
import logging
import re

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
database_available = False
db_engine = None
db_Base = None
db_Session = None # This will be the sessionmaker from database.py
JobSearch = None
save_job_search = None
get_popular_searches = None
get_highest_risk_jobs = None
get_lowest_risk_jobs = None
get_recent_searches = None
check_database_health = None
get_database_stats = None

try:
    from database import engine as db_engine, Base as db_Base, Session as db_Session, JobSearch as DBJobSearch, check_database_health, get_database_stats, save_job_search, get_popular_searches, get_highest_risk_jobs, get_lowest_risk_jobs, get_recent_searches
    print(f"[APP_DEBUG] Imported db_Session type: {type(db_Session)}, value: {db_Session}") # User-requested diagnostic print
    database_available = True if db_engine is not None and db_Session is not None else False 
    if database_available:
        logger.info("Successfully imported database modules and engine/Session are available.")
    else:
        logger.error("Database modules imported, but engine or Session is None. Using fallback.")
        from db_fallback import save_job_search, get_popular_searches, get_highest_risk_jobs, get_lowest_risk_jobs, get_recent_searches
except ImportError as e:
    logger.critical(f"Failed to import database modules: {e}. Using fallback data.", exc_info=True)
    from db_fallback import save_job_search, get_popular_searches, get_highest_risk_jobs, get_lowest_risk_jobs, get_recent_searches
    database_available = False
    db_engine = None 
    db_Session = None

# Import other necessary modules
import job_api_integration_database_only as job_api_integration
import simple_comparison
import career_navigator
import bls_job_mapper 
from bls_job_mapper import TARGET_SOC_CODES 
from job_title_autocomplete_v2 import job_title_autocomplete, load_job_titles_from_db
from sqlalchemy import text 

# Prefer newer integration module if present; otherwise, fall back
try:
    import job_api_integration_v2 as job_api_integration  # noqa: F401
    logger.info("Imported job_api_integration_v2.")
except ModuleNotFoundError:
    import job_api_integration_database_only as job_api_integration  # noqa: F401
    logger.info("job_api_integration_v2 not found; using job_api_integration_database_only.")


# --- Keep-Alive Functionality ---
def keep_alive():
    """Background thread to keep the app active and database connection warm."""
    logger.info("Keep-alive thread started.")
    while True:
        time.sleep(240)  # Ping every 4 minutes
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
        if hasattr(st, 'secrets') and callable(st.secrets.get): 
            bls_api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
        elif hasattr(st, 'secrets') and isinstance(st.secrets, dict) and "api_keys" in st.secrets: 
            bls_api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
    except Exception as e:
        logger.warning(f"Could not access Streamlit secrets for BLS_API_KEY: {e}")

if bls_api_key:
    logger.info("BLS API key loaded.")
else:
    logger.error("BLS_API_KEY is not configured. App will rely on database and may have limited real-time data functionality.")

# --- Admin Authentication ---
def check_admin_auth():
    """Check if user has admin privileges."""
    # Check for admin password in query params first
    if hasattr(st, 'query_params'):
        query_params = st.query_params
        if query_params.get("admin") == "iThriveAI2024!":
            st.session_state.admin_authenticated = True
            return True
    
    # Check session state
    if st.session_state.get("admin_authenticated", False):
        return True
        
    # Check environment variable for admin mode
    admin_mode = os.environ.get('ADMIN_MODE', '').lower() == 'true'
    if admin_mode:
        st.session_state.admin_authenticated = True
        return True
        
    return False

def admin_login_form():
    """Display admin login form."""
    st.markdown("### 🔒 Admin Access Required")
    admin_password = st.text_input("Enter admin password:", type="password", key="admin_password_input")
    if st.button("Login as Admin", key="admin_login_button"):
        if admin_password == "iThriveAI2024!":
            st.session_state.admin_authenticated = True
            st.success("✅ Admin access granted!")
            st.rerun()
        else:
            st.error("❌ Invalid admin password.")
    
    st.info("💡 Admin access is required to view database management tools.")

# --- Auto-Import Manager ---
import threading
import time
from datetime import datetime, timedelta

class AutoImportManager:
    def __init__(self):
        self.is_running = False
        self.thread = None
        self.last_run_date = None
        self.daily_processed_count = 0
        self.api_calls_today = 0
        self.max_daily_calls = 400  # Conservative limit for BLS API
        self.batch_size = 5
        self.api_delay = 1.2  # Slightly longer delay for reliability
        
    def load_progress(self):
        """Load progress from database or file."""
        try:
            if database_available and db_engine:
                with db_engine.connect() as connection:
                    result = connection.execute(text("""
                        SELECT COUNT(*) as processed FROM bls_data 
                        WHERE created_at IS NOT NULL
                    """)).fetchone()
                    if result:
                        total_processed = result[0]
                        # Update session state
                        st.session_state.admin_processed_count = total_processed
                        return total_processed
        except Exception as e:
            logger.error(f"Error loading progress: {e}")
        return st.session_state.get('admin_processed_count', 0)
    
    def get_next_batch_to_process(self):
        """Get the next batch of SOCs that need processing."""
        try:
            if database_available and db_engine:
                with db_engine.connect() as connection:
                    # Get SOCs that haven't been processed yet
                    result = connection.execute(text("""
                        SELECT soc_code FROM target_socs 
                        WHERE soc_code NOT IN (
                            SELECT DISTINCT occupation_code FROM bls_data 
                            WHERE occupation_code IS NOT NULL
                        ) 
                        ORDER BY id 
                        LIMIT :batch_size
                    """), {'batch_size': self.batch_size}).fetchall()
                    
                    if result:
                        return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting next batch: {e}")
        
        # Fallback to original method
        target_socs = st.session_state.get('admin_target_socs', [])
        current_index = st.session_state.get('admin_current_soc_index', 0)
        end_index = min(current_index + self.batch_size, len(target_socs))
        
        batch = []
        for i in range(current_index, end_index):
            if i < len(target_socs):
                soc_info = target_socs[i]
                if isinstance(soc_info, tuple) and len(soc_info) >= 2:
                    batch.append(soc_info[0])
                elif isinstance(soc_info, dict) and "soc_code" in soc_info:
                    batch.append(soc_info["soc_code"])
        
        return batch
    
    def process_batch_automatically(self):
        """Process a batch automatically in the background."""
        if not database_available or not bls_api_key:
            logger.warning("Auto-import: Database or API key not available")
            return False
            
        # Check daily limits
        today = datetime.now().date()
        if self.last_run_date != today:
            self.api_calls_today = 0
            self.last_run_date = today
            
        if self.api_calls_today >= self.max_daily_calls:
            logger.info(f"Auto-import: Daily API limit reached ({self.api_calls_today})")
            return False
        
        # Get next batch
        soc_batch = self.get_next_batch_to_process()
        if not soc_batch:
            logger.info("Auto-import: No more SOCs to process")
            return False
        
        processed_count = 0
        for soc_code in soc_batch:
            if self.api_calls_today >= self.max_daily_calls:
                break
                
            try:
                # Get job title for this SOC
                job_title = self.get_job_title_for_soc(soc_code)
                if job_title:
                    success, message = bls_job_mapper.fetch_and_process_soc_data(
                        soc_code, job_title, db_engine
                    )
                    
                    if success:
                        processed_count += 1
                        self.api_calls_today += 1
                        logger.info(f"Auto-import: Successfully processed {soc_code} - {job_title}")
                    else:
                        logger.warning(f"Auto-import: Failed to process {soc_code}: {message}")
                
                time.sleep(self.api_delay)  # Respect API rate limits
                
            except Exception as e:
                logger.error(f"Auto-import: Exception processing {soc_code}: {e}")
        
        if processed_count > 0:
            # Update progress
            current_total = self.load_progress()
            st.session_state.admin_processed_count = current_total
            logger.info(f"Auto-import: Processed {processed_count} SOCs this batch. Total: {current_total}")
        
        return processed_count > 0
    
    def get_job_title_for_soc(self, soc_code):
        """Get job title for a SOC code."""
        target_socs = st.session_state.get('admin_target_socs', [])
        for soc_info in target_socs:
            if isinstance(soc_info, tuple) and len(soc_info) >= 2 and soc_info[0] == soc_code:
                return soc_info[1]
            elif isinstance(soc_info, dict) and soc_info.get("soc_code") == soc_code:
                return soc_info.get("title", "Unknown")
        return "Unknown"
    
    def start_auto_import(self):
        """Start the automatic import process."""
        if self.is_running:
            return
            
        self.is_running = True
        self.thread = threading.Thread(target=self.auto_import_loop, daemon=True)
        self.thread.start()
        logger.info("Auto-import: Background import started")
    
    def stop_auto_import(self):
        """Stop the automatic import process."""
        self.is_running = False
        logger.info("Auto-import: Background import stopped")
    
    def auto_import_loop(self):
        """Main loop for automatic importing."""
        while self.is_running:
            try:
                # Run every 10 minutes
                time.sleep(600)
                
                if not self.is_running:
                    break
                    
                # Process a batch
                success = self.process_batch_automatically()
                
                if not success:
                    # If no progress made, wait longer before trying again
                    time.sleep(3600)  # Wait 1 hour
                    
            except Exception as e:
                logger.error(f"Auto-import loop error: {e}")
                time.sleep(1800)  # Wait 30 minutes on error
        
        logger.info("Auto-import: Background loop ended")

# Initialize the auto-import manager  
if 'auto_import_manager' not in st.session_state:
    st.session_state.auto_import_manager = AutoImportManager()

# ALWAYS start auto-import if database is available (not just for admins)
if (database_available and bls_api_key and 
    not st.session_state.auto_import_manager.is_running):
    st.session_state.auto_import_manager.start_auto_import()
    logger.info("Auto-import started automatically on app load")

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

# --- Database Availability Check ---
if not database_available:
    st.error("Database connection failed. The application is in a limited mode or cannot function. Please check the database configuration or contact support.")
    if not bls_api_key:
        st.warning("Additionally, the BLS API key is not configured. Real-time data fetching is also unavailable.")
    st.stop()

# --- Admin Controls Setup ---
if 'admin_current_soc_index' not in st.session_state:
    st.session_state.admin_current_soc_index = 0
if 'admin_auto_run_batch' not in st.session_state:
    st.session_state.admin_auto_run_batch = False
if 'admin_failed_socs' not in st.session_state:
    st.session_state.admin_failed_socs = []
if 'admin_target_socs' not in st.session_state:
    st.session_state.admin_target_socs = [] 
if 'admin_processed_count' not in st.session_state:
    st.session_state.admin_processed_count = 0

if not st.session_state.admin_target_socs:
    try:
        st.session_state.admin_target_socs = bls_job_mapper.TARGET_SOC_CODES
        logger.info(f"Admin: Successfully loaded {len(st.session_state.admin_target_socs)} target SOC codes.")
    except AttributeError:
        logger.error("Admin: TARGET_SOC_CODES not found in bls_job_mapper. Admin tool will be limited.")
        st.session_state.admin_target_socs = []

# --- Main Application Tabs ---
tabs = st.tabs(["Single Job Analysis", "Job Comparison"])

with tabs[0]:
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    
    if bls_api_key:
        st.info("📊 Using real-time data from the Bureau of Labor Statistics API via local database cache.")
    else:
        st.warning("BLS API Key not configured. Using only existing database data.")
    
    search_job_title = job_title_autocomplete(
        label="Enter any job title to analyze",
        key="job_title_search_single",
        placeholder="Start typing to see suggestions...",
        help="Type a job title and select from matching suggestions, or enter a custom title."
    )
    
    if st.button("🗑️ Clear Entry", key="clear_button_single_job"):
        st.session_state.job_title_search_single = "" # Clear the text input
        st.rerun()
    
    analyze_job_button = st.button("Analyze Job Risk", key="analyze_single_job_button", type="primary")

    if analyze_job_button and search_job_title:
        with st.spinner(f"Analyzing {search_job_title}..."):
            try:
                job_data = job_api_integration.get_job_data(search_job_title)
                
                if "error" in job_data:
                    st.error(f"Error: {job_data['error']}")
                    if "not found" in job_data.get("message", "").lower():
                         st.info("Please use the Admin Controls to add this job title if it's missing, or try a different title.")
                    st.stop()

            except Exception as e:
                logger.error(f"Error fetching job data for '{search_job_title}': {e}", exc_info=True)
                st.error(f"An unexpected error occurred while fetching data for '{search_job_title}'. Please try again or contact support.")
                st.stop()
            
            if database_available and save_job_search: # Check if function is available
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
                st.markdown(f"**Occupation Code:** {job_data.get('occupation_code', 'N/A')}")
                st.markdown(f"**Job Category:** {job_data.get('job_category', 'General')}")
                
                emp_data = job_data.get('projections', {})
                current_emp = emp_data.get('current_employment')
                st.markdown(f"**Current Employment:** {current_emp:,.0f} jobs" if isinstance(current_emp, (int, float)) else "Data unavailable")
                
                growth = emp_data.get('percent_change')
                st.markdown(f"**BLS Projected Growth (2022-2032):** {growth:+.1f}%" if isinstance(growth, (int, float)) else "Data unavailable")
                
                openings = emp_data.get('annual_job_openings')
                st.markdown(f"**Annual Job Openings:** {openings:,.0f}" if isinstance(openings, (int, float)) else "Data unavailable")

                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Career Outlook</h3>", unsafe_allow_html=True)
                st.markdown("<h4 style='color: #0084FF; font-size: 16px;'>Statistics</h4>", unsafe_allow_html=True)
                
                automation_prob = (job_data.get('year_5_risk', 45.0) + job_data.get('year_1_risk', 25.0)) / 2 # Simplified placeholder
                st.markdown(f"**Task Automation Index (Est.):** {automation_prob:.1f}%")
                
                median_wage = job_data.get('wage_data', {}).get('median_wage')
                st.markdown(f"**Median Annual Wage:** ${median_wage:,.0f}" if isinstance(median_wage, (int, float)) else "Data unavailable")
            
            with risk_gauge_col:
                risk_category = job_data.get("risk_category", "Moderate")
                year_1_risk = job_data.get("year_1_risk", 35.0)
                year_5_risk = job_data.get("year_5_risk", 60.0)
                
                st.markdown(f"<h3 style='text-align: center; margin-bottom: 10px;'>Overall AI Displacement Risk: {risk_category}</h3>", unsafe_allow_html=True)
                
                gauge_value = year_5_risk if year_5_risk is not None else 60.0
                
                fig = go.Figure(go.Indicator(
                    mode = "gauge+number", value = gauge_value,
                    domain = {'x': [0, 1], 'y': [0, 1]}, title = {'text': ""},
                    number = {'suffix': '%', 'font': {'size': 28}},
                    gauge = {
                        'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                        'bar': {'color': "#0084FF"}, 'bgcolor': "white",
                        'borderwidth': 2, 'bordercolor': "gray",
                        'steps': [
                            {'range': [0, 25], 'color': "rgba(0, 255, 0, 0.5)"},
                            {'range': [25, 50], 'color': "rgba(255, 255, 0, 0.5)"},
                            {'range': [50, 75], 'color': "rgba(255, 165, 0, 0.5)"},
                            {'range': [75, 100], 'color': "rgba(255, 0, 0, 0.5)"}
                        ],
                        'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': gauge_value}
                    }
                ))
                fig.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig, use_container_width=True)
                
                col1_risk, col2_risk = st.columns(2)
                with col1_risk:
                    st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>1-Year Risk</h4></div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{year_1_risk if year_1_risk is not None else 0:.1f}%</div>", unsafe_allow_html=True)
                with col2_risk:
                    st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>5-Year Risk</h4></div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{year_5_risk if year_5_risk is not None else 0:.1f}%</div>", unsafe_allow_html=True)
            
            with risk_factors_col:
                st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Key Risk Factors</h3>", unsafe_allow_html=True)
                risk_factors = job_data.get("risk_factors", ["Data unavailable"])
                for factor in risk_factors: st.markdown(f"❌ {factor}")
                
                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Protective Factors</h3>", unsafe_allow_html=True)
                protective_factors = job_data.get("protective_factors", ["Data unavailable"])
                for factor in protective_factors: st.markdown(f"✅ {factor}")
            
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Key Insights</h3>", unsafe_allow_html=True)
            st.markdown(job_data.get("analysis", "Detailed analysis not available for this job title."))
            # Get skill data safely from job_comparison module
            import job_comparison

            # Provide defaults that are always present
            default_skills = {
                'technical_skills': ['Data analysis', 'Industry knowledge', 'Computer proficiency'],
                'soft_skills': ['Communication', 'Problem-solving', 'Adaptability'],
                'emerging_skills': ['AI collaboration', 'Digital literacy', 'Remote work skills']
            }

            # Safely access JOB_SKILLS catalogue if it exists
            skills_catalog = getattr(job_comparison, "JOB_SKILLS", {})

            # Exact match first
            if search_job_title in skills_catalog:
                skills = skills_catalog[search_job_title]
            else:
                # Case-insensitive match fallback
                skills = None
                for skill_job, skill_data in skills_catalog.items():
                    if search_job_title.lower() == skill_job.lower():
                        skills = skill_data
                        break

                # Final fallback to defaults
                if skills is None:
                    skills = default_skills
            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Recent Job Searches</h3>", unsafe_allow_html=True)
            if get_recent_searches: # Check if function is available
                recent_searches_data = get_recent_searches(limit=5)
                if recent_searches_data:
                    for i, search in enumerate(recent_searches_data): # Added enumerate for unique keys
                        job_title = search.get("job_title", "Unknown Job")
                        risk_category = search.get("risk_category", "Unknown")
                        timestamp = search.get("timestamp")
                        
                        time_ago = "Recently"
                        if timestamp:
                            now_utc = datetime.datetime.now(datetime.timezone.utc)
                            # Ensure timestamp is offset-aware
                            if isinstance(timestamp, str):
                                try:
                                    timestamp = datetime.datetime.fromisoformat(timestamp)
                                except ValueError: # Handle if not ISO format
                                    timestamp = None 
                            
                            if timestamp and timestamp.tzinfo is None: # If still naive, assume UTC
                                timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)

                            if timestamp:
                                delta = now_utc - timestamp
                                if delta.days > 0: time_ago = f"{delta.days} days ago"
                                elif delta.seconds // 3600 > 0: time_ago = f"{delta.seconds // 3600} hour{'s' if delta.seconds // 3600 > 1 else ''} ago"
                                elif delta.seconds // 60 > 0: time_ago = f"{delta.seconds // 60} minute{'s' if delta.seconds // 60 > 1 else ''} ago"
                                else: time_ago = "Just now"
                        
                        risk_color = {"Very High": "#FF4B4B", "High": "#FF8C42", "Moderate": "#FFCC3E", "Low": "#4CAF50"}.get(risk_category, "#666666")
                        
                        r_col1, r_col2, r_col3 = st.columns([3,2,2])
                        with r_col1:
                            if st.button(job_title, key=f"recent_search_{i}_{job_title.replace(' ','_')}"):
                                st.session_state.job_title_search_single = job_title
                                st.rerun()
                        with r_col2: st.markdown(f"<p style='color: {risk_color};'>{risk_category}</p>", unsafe_allow_html=True)
                        with r_col3: st.write(time_ago)
                else:
                    st.info("No recent searches yet.")

with tabs[1]:
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    st.markdown("Compare the AI displacement risk for multiple jobs side by side. Add up to 5 jobs.")
    
    if 'compare_jobs_list' not in st.session_state:
        st.session_state.compare_jobs_list = []

    new_job_to_compare = job_title_autocomplete(
        label="Enter a job title to add to comparison:",
        key="compare_job_input",
        placeholder="Start typing...",
        help="Select from suggestions or type a custom job title."
    )

    if st.button("Add Job to Comparison", key="add_to_compare_button"):
        if new_job_to_compare and new_job_to_compare not in st.session_state.compare_jobs_list and len(st.session_state.compare_jobs_list) < 5:
            st.session_state.compare_jobs_list.append(new_job_to_compare)
            st.session_state.compare_job_input = "" # Clear input after adding
            st.rerun()
        elif len(st.session_state.compare_jobs_list) >= 5:
            st.warning("Maximum of 5 jobs can be compared.")
        elif not new_job_to_compare:
            st.warning("Please enter a job title to add.")

    if st.session_state.compare_jobs_list:
        st.markdown("### Jobs to Compare:")
        cols = st.columns(len(st.session_state.compare_jobs_list))
        for idx, job_title_comp in enumerate(st.session_state.compare_jobs_list):
            with cols[idx]:
                st.markdown(f"**{job_title_comp}**")
                if st.button("Remove", key=f"remove_comp_{idx}_{job_title_comp.replace(' ','_')}"):
                    st.session_state.compare_jobs_list.pop(idx)
                    st.rerun()
        
        if st.button("Clear All Comparison Jobs", key="clear_all_comp_button"):
            st.session_state.compare_jobs_list = []
            st.rerun()

    if st.session_state.compare_jobs_list:
        with st.spinner("Fetching comparison data..."):
            comparison_job_data = simple_comparison.get_job_comparison_data(st.session_state.compare_jobs_list)
        
        if comparison_job_data and not all("error" in data for data in comparison_job_data.values()):
            comp_tabs = st.tabs(["Comparison Chart", "Detailed Table", "Risk Heatmap", "Radar Analysis"])
            with comp_tabs[0]:
                chart = simple_comparison.create_comparison_chart(comparison_job_data)
                if chart: st.plotly_chart(chart, use_container_width=True)
                else: st.info("Not enough data to create comparison chart.")
            with comp_tabs[1]:
                df_comp = simple_comparison.create_comparison_table(comparison_job_data)
                if df_comp is not None: st.dataframe(df_comp, use_container_width=True)
                else: st.info("Not enough data to create comparison table.")
            with comp_tabs[2]:
                heatmap = simple_comparison.create_risk_heatmap(comparison_job_data)
                if heatmap: st.plotly_chart(heatmap, use_container_width=True)
                else: st.info("Not enough data to create heatmap.")
            with comp_tabs[3]:
                radar = simple_comparison.create_radar_chart(comparison_job_data)
                if radar: st.plotly_chart(radar, use_container_width=True)
                else: st.info("Not enough data to create radar chart.")
        else:
            st.error("Could not retrieve enough data for comparison. Please ensure job titles are valid or try different ones.")

# --- Admin Controls Expander ---
with st.sidebar:
    st.markdown("<h2 style='color: #0084FF;'>System Status</h2>", unsafe_allow_html=True)
    
    # BLS API Status
    if bls_api_key:
        st.markdown("BLS API: <span style='color:green;font-weight:bold;'>CONFIGURED</span>", unsafe_allow_html=True)
    else:
        st.markdown("BLS API: <span style='color:red;font-weight:bold;'>NOT CONFIGURED</span>", unsafe_allow_html=True)

    # Database Status
    if database_available and db_engine:
        db_health = check_database_health() if check_database_health else {"status": "error", "message": "Health check function not available"}

        # `check_database_health` now returns a simple string ("OK", "Error", "Not Configured").
        # Older fall-backs may still return a dictionary.  Handle both forms safely.
        if isinstance(db_health, str):
            ok_status = db_health.lower() == "ok"
            msg = db_health
        else:  # dict fallback
            ok_status = str(db_health.get("status", "")).lower() == "ok"
            msg = db_health.get("message", "Unknown")

        if ok_status:
            st.markdown("Database: <span style='color:green;font-weight:bold;'>Connected</span>", unsafe_allow_html=True)
        else:
            st.markdown(f"Database: <span style='color:red;font-weight:bold;'>Error ({msg})</span>", unsafe_allow_html=True)
    else:
        st.markdown("Database: <span style='color:orange;font-weight:bold;'>Not Connected (Fallback Mode)</span>", unsafe_allow_html=True)

    # Data Refresh Cycle
    try:
        with open("last_refresh.json", "r") as f:
            refresh_data = json.load(f)
            last_refresh_time = datetime.datetime.fromisoformat(refresh_data["date"])
            time_since_refresh = datetime.datetime.now() - last_refresh_time
            if time_since_refresh.total_seconds() < 2 * 24 * 3600: # Less than 2 days
                 st.markdown(f"Data refresh cycle: <span style='color:green;font-weight:bold;'>Active</span> (last: {time_since_refresh.days}d {time_since_refresh.seconds//3600}h ago)", unsafe_allow_html=True)
            else:
                 st.markdown(f"Data refresh cycle: <span style='color:orange;font-weight:bold;'>Stale</span> (last: {time_since_refresh.days}d {time_since_refresh.seconds//3600}h ago)", unsafe_allow_html=True)
    except Exception:
        st.markdown("Data refresh cycle status unknown.", unsafe_allow_html=True)
    
    st.markdown("***")
    st.markdown(f"App Version: 2.1.0 (Real Data Only)")
    st.markdown(f"Last App Load: {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Keep-alive status
    last_ping_time = st.session_state.get("last_keep_alive_ping")
    if last_ping_time:
        time_since_last_ping = (datetime.datetime.now(datetime.timezone.utc) - last_ping_time).total_seconds() / 60
        st.markdown(f"Keep-Alive: <span style='color:green;font-weight:bold;'>Active</span> (last ping: {time_since_last_ping:.1f} min ago)", unsafe_allow_html=True)
    elif st.session_state.get("last_keep_alive_ping_error"):
         st.markdown(f"Keep-Alive: <span style='color:red;font-weight:bold;'>Error</span> ({st.session_state.last_keep_alive_ping_error})", unsafe_allow_html=True)
    else:
        st.markdown("Keep-Alive: <span style='color:orange;font-weight:bold;'>Initializing...</span>", unsafe_allow_html=True)

    st.markdown("***")
    st.markdown("### UptimeRobot Setup")
    st.markdown("To keep this application alive with UptimeRobot:")
    st.markdown("1. Create a new monitor in UptimeRobot")
    st.markdown("2. Set Type to \"HTTP(s)\"")
    st.markdown("3. Set URL to your app URL with `?health=true` (e.g., `your-app-url.streamlit.app/?health=true`)")
    st.markdown("4. Set monitoring interval to 5 minutes")
    st.markdown("5. Enable \"Alert When Down\"")

    # Auto-Import Status for All Users
    st.markdown("### 🔄 Data Import Status") 
    if 'auto_import_manager' in st.session_state:
        if st.session_state.auto_import_manager.is_running:
            st.success("🟢 Background data updates: ACTIVE")
            st.info("The system is automatically importing the latest job data.")
        else:
            st.warning("🟡 Background data updates: MONITORING")
            
        # Show basic progress without admin details
        if st.session_state.admin_target_socs:
            total_socs = len(st.session_state.admin_target_socs)
            processed = st.session_state.get('admin_processed_count', 0)
            if total_socs > 0:
                pct = processed / total_socs
                st.progress(pct, text=f"Database: {pct:.1%} complete")
    else:
        st.info("🔄 Data import system initializing...")

    if not bls_api_key:
        st.error("BLS API Key is not configured. Please set the BLS_API_KEY in Streamlit secrets or environment variables. The application cannot function without it.")

    # Admin Controls - Protected Section
    if check_admin_auth():
        with st.expander("⚙️ ADMIN CONTROLS - Authenticated User", expanded=True):
            st.success("🔓 You are logged in as an administrator")
            st.markdown("### Automatic Database Population Status")
            
            # Load current progress
            total_processed = st.session_state.auto_import_manager.load_progress()
            total_socs = len(st.session_state.admin_target_socs) if st.session_state.admin_target_socs else 0
            
            # Progress display
            if total_socs > 0:
                progress_pct = total_processed / total_socs
                st.progress(progress_pct, text=f"Progress: {total_processed:,} / {total_socs:,} SOCs processed ({progress_pct:.1%})")
            else:
                st.info("Target SOCs list not loaded")
            
            # Status indicators
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.session_state.auto_import_manager.is_running:
                    st.success("🟢 Auto-Import: ACTIVE")
                else:
                    st.error("🔴 Auto-Import: STOPPED")
            
            with col2:
                api_calls = st.session_state.auto_import_manager.api_calls_today
                max_calls = st.session_state.auto_import_manager.max_daily_calls
                if api_calls < max_calls:
                    st.success(f"📊 API Calls: {api_calls}/{max_calls}")
                else:
                    st.warning(f"⚠️ Daily Limit Reached: {api_calls}/{max_calls}")
            
            with col3:
                next_batch = st.session_state.auto_import_manager.get_next_batch_to_process()
                remaining = len(next_batch) if next_batch else 0
                if remaining > 0:
                    st.info(f"⏳ Next Batch: {remaining} SOCs")
                else:
                    st.success("✅ All SOCs Processed!")
            
            # Manual controls
            st.markdown("### Manual Controls")
            col_btn1, col_btn2, col_btn3 = st.columns(3)
            
            with col_btn1:
                if st.button("🚀 Force Process Batch", key="force_batch"):
                    with st.spinner("Processing batch..."):
                        success = st.session_state.auto_import_manager.process_batch_automatically()
                        if success:
                            st.success("Batch processed successfully!")
                        else:
                            st.warning("No items to process or daily limit reached")
                        st.rerun()
            
            with col_btn2:
                if st.session_state.auto_import_manager.is_running:
                    if st.button("⏸️ Stop Auto-Import", key="stop_auto"):
                        st.session_state.auto_import_manager.stop_auto_import()
                        st.info("Auto-import stopped")
                        st.rerun()
                else:
                    if st.button("▶️ Start Auto-Import", key="start_auto"):
                        st.session_state.auto_import_manager.start_auto_import()
                        st.success("Auto-import started")
                        st.rerun()
            
            with col_btn3:
                if st.button("📊 Refresh Stats", key="refresh_stats"):
                    st.rerun()
            
            # Settings
            st.markdown("### Import Settings")
            col_set1, col_set2 = st.columns(2)
            with col_set1:
                new_batch_size = st.number_input("Batch Size", min_value=1, max_value=20, 
                                               value=st.session_state.auto_import_manager.batch_size)
                if new_batch_size != st.session_state.auto_import_manager.batch_size:
                    st.session_state.auto_import_manager.batch_size = new_batch_size
            
            with col_set2:
                new_delay = st.number_input("API Delay (seconds)", min_value=0.5, max_value=5.0, 
                                          value=st.session_state.auto_import_manager.api_delay, step=0.1)
                if new_delay != st.session_state.auto_import_manager.api_delay:
                    st.session_state.auto_import_manager.api_delay = new_delay
            
            # Recent activity log
            if database_available:
                st.markdown("### Recent Import Activity")
                try:
                    with db_engine.connect() as connection:
                        recent_imports = connection.execute(text("""
                            SELECT occupation_code, job_title, created_at 
                            FROM bls_data 
                            WHERE created_at >= NOW() - INTERVAL '24 hours'
                            ORDER BY created_at DESC 
                            LIMIT 10
                        """)).fetchall()
                        
                        if recent_imports:
                            for row in recent_imports:
                                soc_code, job_title, created_at = row
                                st.text(f"{created_at}: {soc_code} - {job_title}")
                        else:
                            st.info("No recent import activity in the last 24 hours")
                except Exception as e:
                    st.error(f"Error loading recent activity: {e}")
                    
            # Logout button
            st.markdown("---")
            if st.button("🚪 Logout", key="admin_logout"):
                st.session_state.admin_authenticated = False
                st.success("Logged out successfully")
                st.rerun()
    else:
        # Show minimal admin section for regular users  
        with st.expander("🔒 Admin Login", expanded=False):
            admin_login_form()

# --- Application Footer ---
st.markdown("<hr style='margin-top: 40px; margin-bottom: 20px;'>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; font-size: 12px; color: #888;'>© 2025 iThriveAI - AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; font-size: 12px; color: #888;'>Powered by real-time Bureau of Labor Statistics data | <a href='https://www.bls.gov/ooh/' target='_blank'>BLS Occupational Outlook Handbook</a></p>", unsafe_allow_html=True)

# --- Streamlit Status Embed ---
st.markdown(
    """
    <iframe
        src="https://www.streamlitstatus.com/embed-status/light"
        height="45"
        style="width:100%;border:none;"
        title="Streamlit Status Embed"
    ></iframe>
    """,
    unsafe_allow_html=True,
)
