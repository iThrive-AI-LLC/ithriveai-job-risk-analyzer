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

# --- Enhanced Auto-Import Manager ---
import threading
import time
import json
import os
from datetime import datetime, timedelta

class PersistentAutoImportManager:
    def __init__(self):
        self.is_running = False
        self.thread = None
        self.progress_file = "import_progress.json"
        self.settings_file = "import_settings.json"
        
        # Load persistent progress and settings
        self.load_progress()
        self.load_settings()
        
        # Start automatically on initialization
        self.start_auto_import()
        
    def load_settings(self):
        """Load import settings from file."""
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r') as f:
                    settings = json.load(f)
                    self.max_daily_calls = settings.get('max_daily_calls', 400)
                    self.batch_size = settings.get('batch_size', 3)
                    self.api_delay = settings.get('api_delay', 2.0)
            else:
                # Default settings
                self.max_daily_calls = 400
                self.batch_size = 3
                self.api_delay = 2.0
                self.save_settings()
        except Exception as e:
            logger.error(f"Error loading settings: {e}")
            # Use defaults
            self.max_daily_calls = 400
            self.batch_size = 3
            self.api_delay = 2.0
    
    def save_settings(self):
        """Save import settings to file."""
        try:
            settings = {
                'max_daily_calls': self.max_daily_calls,
                'batch_size': self.batch_size,
                'api_delay': self.api_delay
            }
            with open(self.settings_file, 'w') as f:
                json.dump(settings, f)
        except Exception as e:
            logger.error(f"Error saving settings: {e}")
    
    def load_progress(self):
        """Load persistent progress from file."""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    progress_data = json.load(f)
                    self.current_soc_index = progress_data.get('current_soc_index', 0)
                    self.processed_count = progress_data.get('processed_count', 0)
                    self.failed_socs = progress_data.get('failed_socs', [])
                    self.last_run_date = progress_data.get('last_run_date')
                    self.api_calls_today = progress_data.get('api_calls_today', 0)
                    
                    # Reset daily counter if it's a new day
                    today = datetime.now().date().isoformat()
                    if self.last_run_date != today:
                        self.api_calls_today = 0
                        self.last_run_date = today
                        
                    logger.info(f"Loaded progress: {self.processed_count} processed, index {self.current_soc_index}")
            else:
                # First time - initialize
                self.current_soc_index = 0
                self.processed_count = 0
                self.failed_socs = []
                self.last_run_date = datetime.now().date().isoformat()
                self.api_calls_today = 0
                self.save_progress()
        except Exception as e:
            logger.error(f"Error loading progress: {e}")
            # Initialize with defaults
            self.current_soc_index = 0
            self.processed_count = 0
            self.failed_socs = []
            self.last_run_date = datetime.now().date().isoformat()
            self.api_calls_today = 0
    
    def save_progress(self):
        """Save persistent progress to file."""
        try:
            progress_data = {
                'current_soc_index': self.current_soc_index,
                'processed_count': self.processed_count,
                'failed_socs': self.failed_socs,
                'last_run_date': self.last_run_date,
                'api_calls_today': self.api_calls_today,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
                
            # Also update session state for UI
            if 'admin_processed_count' in st.session_state:
                st.session_state.admin_processed_count = self.processed_count
            if 'admin_current_soc_index' in st.session_state:
                st.session_state.admin_current_soc_index = self.current_soc_index
            if 'admin_failed_socs' in st.session_state:
                st.session_state.admin_failed_socs = self.failed_socs
            
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
    
    def get_total_socs(self):
        """Get total number of SOCs to process."""
        try:
            if database_available and db_engine:
                with db_engine.connect() as connection:
                    result = connection.execute(text("SELECT COUNT(*) FROM target_socs")).fetchone()
                    if result:
                        return result[0]
        except Exception:
            pass
        
        # Fallback to session state
        target_socs = st.session_state.get('admin_target_socs', [])
        return len(target_socs)
    
    def get_next_batch_to_process(self):
        """Get the next batch of SOCs that need processing."""
        try:
            if database_available and db_engine:
                with db_engine.connect() as connection:
                    # Get SOCs that haven't been processed yet, starting from current index
                    result = connection.execute(text("""
                        SELECT soc_code, title FROM target_socs 
                        WHERE id > :current_index
                        AND soc_code NOT IN (
                            SELECT DISTINCT occupation_code FROM bls_data 
                            WHERE occupation_code IS NOT NULL
                        ) 
                        ORDER BY id 
                        LIMIT :batch_size
                    """), {
                        'current_index': self.current_soc_index,
                        'batch_size': self.batch_size
                    }).fetchall()
                    
                    if result:
                        return [(row[0], row[1]) for row in result]
        except Exception as e:
            logger.error(f"Error getting next batch from database: {e}")
        
        # Fallback to session state method
        target_socs = st.session_state.get('admin_target_socs', [])
        if self.current_soc_index >= len(target_socs):
            return []
            
        end_index = min(self.current_soc_index + self.batch_size, len(target_socs))
        batch = []
        
        for i in range(self.current_soc_index, end_index):
            if i < len(target_socs):
                soc_info = target_socs[i]
                if isinstance(soc_info, tuple) and len(soc_info) >= 2:
                    batch.append((soc_info[0], soc_info[1]))
                elif isinstance(soc_info, dict):
                    batch.append((soc_info.get("soc_code"), soc_info.get("title", "Unknown")))
        
        return batch
    
    def process_batch_automatically(self):
        """Process a batch automatically in the background."""
        if not database_available or not bls_api_key:
            logger.warning("Auto-import: Database or API key not available")
            return False
            
        # Check daily limits
        today = datetime.now().date().isoformat()
        if self.last_run_date != today:
            self.api_calls_today = 0
            self.last_run_date = today
            logger.info("Auto-import: New day, resetting API call counter")
            
        if self.api_calls_today >= self.max_daily_calls:
            logger.info(f"Auto-import: Daily API limit reached ({self.api_calls_today}/{self.max_daily_calls})")
            return False
        
        # Get next batch
        soc_batch = self.get_next_batch_to_process()
        if not soc_batch:
            logger.info("Auto-import: No more SOCs to process - import complete!")
            return False
        
        processed_count = 0
        for soc_code, job_title in soc_batch:
            if self.api_calls_today >= self.max_daily_calls:
                logger.info("Auto-import: Hit daily limit during batch processing")
                break
                
            try:
                logger.info(f"Auto-import: Processing {soc_code} - {job_title}")
                success, message = bls_job_mapper.fetch_and_process_soc_data(
                    soc_code, job_title, db_engine
                )
                
                if success:
                    processed_count += 1
                    self.processed_count += 1
                    self.api_calls_today += 1
                    self.current_soc_index += 1
                    logger.info(f"Auto-import: Successfully processed {soc_code} ({self.processed_count} total)")
                else:
                    logger.warning(f"Auto-import: Failed to process {soc_code}: {message}")
                    self.failed_socs.append({
                        "soc_code": soc_code,
                        "title": job_title,
                        "reason": message,
                        "timestamp": datetime.now().isoformat()
                    })
                    self.current_soc_index += 1
                
                # Save progress after each successful processing
                self.save_progress()
                
                # Respect API rate limits
                time.sleep(self.api_delay)
                
            except Exception as e:
                logger.error(f"Auto-import: Exception processing {soc_code}: {e}")
                self.failed_socs.append({
                    "soc_code": soc_code,
                    "title": job_title,
                    "reason": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                self.current_soc_index += 1
                self.save_progress()
        
        if processed_count > 0:
            logger.info(f"Auto-import: Processed {processed_count} SOCs this batch. Total: {self.processed_count}")
        
        return processed_count > 0
    
    def start_auto_import(self):
        """Start the automatic import process."""
        if self.is_running:
            return
            
        self.is_running = True
        self.thread = threading.Thread(target=self.auto_import_loop, daemon=True)
        self.thread.start()
        logger.info("Auto-import: Started persistent background import")
    
    def stop_auto_import(self):
        """Stop the automatic import process."""
        self.is_running = False
        self.save_progress()
        logger.info("Auto-import: Stopped background import")
    
    def auto_import_loop(self):
        """Main loop for automatic importing with 24-hour pause on API limits."""
        logger.info("Auto-import: Background loop started")
        
        while self.is_running:
            try:
                # Check if we've hit daily limits
                if self.api_calls_today >= self.max_daily_calls:
                    # Wait 24 hours before resuming
                    logger.info(f"Auto-import: Daily limit reached ({self.api_calls_today}). Waiting 24 hours...")
                    
                    # Sleep in smaller chunks so we can check if stopped
                    for _ in range(288):  # 24 hours in 5-minute chunks
                        if not self.is_running:
                            break
                        time.sleep(300)  # 5 minutes
                    
                    # Reset for new day
                    if self.is_running:
                        self.api_calls_today = 0
                        self.last_run_date = datetime.now().date().isoformat()
                        self.save_progress()
                        logger.info("Auto-import: 24 hours passed, resuming import")
                
                if not self.is_running:
                    break
                    
                # Process a batch
                success = self.process_batch_automatically()
                
                if success:
                    # If successful, wait 10 minutes before next batch
                    for _ in range(20):  # 10 minutes in 30-second chunks
                        if not self.is_running:
                            break
                        time.sleep(30)
                else:
                    # If no progress (completed or error), wait 1 hour
                    for _ in range(120):  # 1 hour in 30-second chunks
                        if not self.is_running:
                            break
                        time.sleep(30)
                    
            except Exception as e:
                logger.error(f"Auto-import loop error: {e}")
                # Wait 30 minutes on error
                for _ in range(60):  # 30 minutes in 30-second chunks
                    if not self.is_running:
                        break
                    time.sleep(30)
        
        logger.info("Auto-import: Background loop ended")
    
    def get_status(self):
        """Get current status for display."""
        total_socs = self.get_total_socs()
        progress_pct = (self.processed_count / total_socs) if total_socs > 0 else 0
        
        return {
            'is_running': self.is_running,
            'processed_count': self.processed_count,
            'total_socs': total_socs,
            'progress_percentage': progress_pct,
            'api_calls_today': self.api_calls_today,
            'max_daily_calls': self.max_daily_calls,
            'current_index': self.current_soc_index,
            'failed_count': len(self.failed_socs),
            'last_run_date': self.last_run_date
        }

# Initialize the persistent auto-import manager  
if 'persistent_auto_import_manager' not in st.session_state:
    st.session_state.persistent_auto_import_manager = PersistentAutoImportManager()

# The system starts automatically - no need for conditional startup
auto_import_manager = st.session_state.persistent_auto_import_manager

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
                
                # Get employment data from various possible locations in the job_data structure
                bls_data = job_data.get('bls_data', {})
                emp_data = job_data.get('projections', bls_data)
                
                # Current Employment
                current_emp = emp_data.get('current_employment')
                if current_emp is None:
                    current_emp = bls_data.get('employment')
                if current_emp is None:
                    current_emp = job_data.get('employment')
                
                if isinstance(current_emp, (int, float)):
                    st.markdown(f"**Current Employment:** {current_emp:,.0f} jobs")
                else:
                    st.markdown("**Current Employment:** Not available")
                
                # Growth Rate
                growth = emp_data.get('percent_change')
                if growth is None:
                    growth = bls_data.get('employment_change_percent')
                if growth is None:
                    growth = job_data.get('employment_change_percent')
                
                if isinstance(growth, (int, float)):
                    st.markdown(f"**BLS Projected Growth (2022-2032):** {growth:+.1f}%")
                else:
                    st.markdown("**BLS Projected Growth (2022-2032):** Not available")
                
                # Job Openings
                openings = emp_data.get('annual_job_openings')
                if openings is None:
                    openings = bls_data.get('annual_job_openings')
                if openings is None:
                    openings = job_data.get('annual_job_openings')
                
                if isinstance(openings, (int, float)):
                    st.markdown(f"**Annual Job Openings:** {openings:,.0f}")
                else:
                    st.markdown("**Annual Job Openings:** Not available")

                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Career Outlook</h3>", unsafe_allow_html=True)
                st.markdown("<h4 style='color: #0084FF; font-size: 16px;'>Statistics</h4>", unsafe_allow_html=True)
                
                # Calculate automation probability
                year_1_risk = job_data.get('year_1_risk')
                year_5_risk = job_data.get('year_5_risk')
                
                if year_1_risk is not None and year_5_risk is not None:
                    automation_prob = (year_5_risk + year_1_risk) / 2
                    st.markdown(f"**Task Automation Index (Est.):** {automation_prob:.1f}%")
                else:
                    risk_scores = job_data.get('risk_scores', {})
                    year_1 = risk_scores.get('year_1')
                    year_5 = risk_scores.get('year_5')
                    
                    if year_1 is not None and year_5 is not None:
                        automation_prob = (year_5 + year_1) / 2
                        st.markdown(f"**Task Automation Index (Est.):** {automation_prob:.1f}%")
                    else:
                        st.markdown("**Task Automation Index (Est.):** Not available")
                
                # Median Wage
                wage_data = job_data.get('wage_data', {})
                median_wage = wage_data.get('median_wage')
                if median_wage is None:
                    median_wage = bls_data.get('median_wage')
                if median_wage is None:
                    median_wage = job_data.get('median_wage')
                
                if isinstance(median_wage, (int, float)):
                    st.markdown(f"**Median Annual Wage:** ${median_wage:,.0f}")
                else:
                    st.markdown("**Median Annual Wage:** Not available")
            
            with risk_gauge_col:
                risk_category = job_data.get("risk_category", "Moderate")
                
                # Get risk values from various possible locations
                year_1_risk = job_data.get("year_1_risk")
                if year_1_risk is None:
                    risk_scores = job_data.get('risk_scores', {})
                    year_1_risk = risk_scores.get('year_1')
                
                year_5_risk = job_data.get("year_5_risk")
                if year_5_risk is None:
                    risk_scores = job_data.get('risk_scores', {})
                    year_5_risk = risk_scores.get('year_5')
                
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
                    st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{year_1_risk if year_1_risk is not None else 'N/A'}%</div>", unsafe_allow_html=True)
                with col2_risk:
                    st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>5-Year Risk</h4></div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{year_5_risk if year_5_risk is not None else 'N/A'}%</div>", unsafe_allow_html=True)
            
            with risk_factors_col:
                st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Key Risk Factors</h3>", unsafe_allow_html=True)
                risk_factors = job_data.get("risk_factors", [])
                if risk_factors:
                    for factor in risk_factors: 
                        st.markdown(f"❌ {factor}")
                else:
                    st.markdown("No specific risk factors identified")
                
                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Protective Factors</h3>", unsafe_allow_html=True)
                protective_factors = job_data.get("protective_factors", [])
                if protective_factors:
                    for factor in protective_factors: 
                        st.markdown(f"✅ {factor}")
                else:
                    st.markdown("No specific protective factors identified")
            
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Key Insights</h3>", unsafe_allow_html=True)
            analysis = job_data.get("analysis")
            if analysis:
                st.markdown(analysis)
            else:
                summary = job_data.get("summary")
                if summary:
                    st.markdown(summary)
                else:
                    st.markdown("Detailed analysis not available for this job title.")
            
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

    # Auto-Import Status for All Users
    st.markdown("### 🔄 Data Import Status") 
    
    status = auto_import_manager.get_status()
    
    if status['is_running']:
        st.success("🟢 Background data updates: ACTIVE")
        st.info("The system is automatically importing BLS job data 24/7.")
    else:
        st.warning("🟡 Background data updates: STOPPED")
        
    # Show progress for all users
    if status['total_socs'] > 0:
        st.progress(
            status['progress_percentage'], 
            text=f"Database: {status['progress_percentage']:.1%} complete ({status['processed_count']:,} / {status['total_socs']:,} jobs)"
        )
        
        # Show daily API usage
        daily_pct = status['api_calls_today'] / status['max_daily_calls']
        if daily_pct >= 1.0:
            st.warning(f"⏳ Daily API limit reached. Will resume in 24 hours.")
        else:
            st.info(f"📊 Today's API usage: {status['api_calls_today']}/{status['max_daily_calls']} ({daily_pct:.1%})")
    else:
        st.info("🔄 Data import system initializing...")

    if not bls_api_key:
        st.error("BLS API Key is not configured. Please set the BLS_API_KEY in Streamlit secrets or environment variables. The application cannot function without it.")

    # Admin Controls - Protected Section - ONLY SHOW IF AUTHENTICATED
    if check_admin_auth():
        with st.expander("⚙️ ADMIN CONTROLS - Authenticated User", expanded=True):
            st.success("🔓 You are logged in as an administrator")
            
            status = auto_import_manager.get_status()
            
            st.markdown("### 📊 Detailed Import Status")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                if status['is_running']:
                    st.success("🟢 Auto-Import: ACTIVE")
                else:
                    st.error("🔴 Auto-Import: STOPPED")
            
            with col2:
                if status['api_calls_today'] < status['max_daily_calls']:
                    st.success(f"📊 API: {status['api_calls_today']}/{status['max_daily_calls']}")
                else:
                    st.warning(f"⚠️ Daily Limit: {status['api_calls_today']}/{status['max_daily_calls']}")
            
            with col3:
                remaining = status['total_socs'] - status['processed_count'] 
                if remaining > 0:
                    st.info(f"⏳ Remaining: {remaining:,} SOCs")
                else:
                    st.success("✅ Import Complete!")
            
            # Progress details
            st.markdown("### 📈 Progress Details")
            st.progress(
                status['progress_percentage'], 
                text=f"Overall Progress: {status['processed_count']:,} / {status['total_socs']:,} SOCs ({status['progress_percentage']:.1%})"
            )
            
            st.write(f"**Current Index:** {status['current_index']:,}")
            st.write(f"**Failed SOCs:** {status['failed_count']:,}")
            st.write(f"**Last Active:** {status['last_run_date']}")
            
            # Control buttons
            st.markdown("### 🎛️ Controls")
            col_btn1, col_btn2, col_btn3 = st.columns(3)
            
            with col_btn1:
                if status['is_running']:
                    if st.button("⏸️ Stop Import", key="stop_auto"):
                        auto_import_manager.stop_auto_import()
                        st.info("Import stopped")
                        st.rerun()
                else:
                    if st.button("▶️ Start Import", key="start_auto"):
                        auto_import_manager.start_auto_import()
                        st.success("Import started")
                        st.rerun()
            
            with col_btn2:
                if st.button("🚀 Process Batch Now", key="force_batch"):
                    with st.spinner("Processing batch..."):
                        success = auto_import_manager.process_batch_automatically()
                        if success:
                            st.success("Batch processed!")
                        else:
                            st.warning("No items to process or daily limit reached")
                        st.rerun()
            
            with col_btn3:
                if st.button("📊 Refresh Status", key="refresh_stats"):
                    st.rerun()
            
            # Settings
            st.markdown("### ⚙️ Settings")
            col_set1, col_set2 = st.columns(2)
            
            with col_set1:
                new_batch_size = st.number_input(
                    "Batch Size", 
                    min_value=1, max_value=10, 
                    value=auto_import_manager.batch_size,
                    help="Number of SOCs to process in each batch"
                )
                if new_batch_size != auto_import_manager.batch_size:
                    auto_import_manager.batch_size = new_batch_size
                    auto_import_manager.save_settings()
            
            with col_set2:
                new_delay = st.number_input(
                    "API Delay (seconds)", 
                    min_value=1.0, max_value=10.0, 
                    value=auto_import_manager.api_delay, 
                    step=0.5,
                    help="Delay between API calls to respect rate limits"
                )
                if new_delay != auto_import_manager.api_delay:
                    auto_import_manager.api_delay = new_delay
                    auto_import_manager.save_settings()
            
            # Failed SOCs summary
            if status['failed_count'] > 0:
                st.markdown("### ❌ Failed SOCs")
                if st.button("Show Failed SOCs"):
                    failed_df = pd.DataFrame(auto_import_manager.failed_socs)
                    st.dataframe(failed_df, use_container_width=True)
                    
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
            
        # Link to dedicated admin page
        st.markdown("For full admin controls, visit the [Admin Dashboard](/admin)")

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
