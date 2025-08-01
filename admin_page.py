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
import logging
import re
from sqlalchemy import text
import numpy as np

# Configure page settings
st.set_page_config(
    page_title="iThriveAI Admin Dashboard",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom logger for the admin app
logger = logging.getLogger("iThriveAI_Admin")
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

# Attempt to import database modules
database_available = False
db_engine = None
db_Base = None
db_Session = None
check_database_health = None
get_database_stats = None

try:
    from database import engine as db_engine, Base as db_Base, Session as db_Session, check_database_health, get_database_stats
    database_available = True if db_engine is not None and db_Session is not None else False 
    if database_available:
        logger.info("Successfully imported database modules and engine/Session are available.")
    else:
        logger.error("Database modules imported, but engine or Session is None.")
except ImportError as e:
    logger.critical(f"Failed to import database modules: {e}.", exc_info=True)

# Try to import BLS job mapper
try:
    import bls_job_mapper
    bls_mapper_available = True
    logger.info("Successfully imported BLS job mapper module.")
except ImportError as e:
    bls_mapper_available = False
    logger.critical(f"Failed to import BLS job mapper: {e}.", exc_info=True)

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

# --- Persistent Auto-Import Manager ---
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
        self.failed_file = "failed_socs.json"
        
        # Load persistent progress and settings
        self.load_progress()
        self.load_settings()
        self.load_failed_socs()
        
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
                    self.auto_retry_failed = settings.get('auto_retry_failed', True)
                    self.retry_interval_days = settings.get('retry_interval_days', 7)
                    self.validation_enabled = settings.get('validation_enabled', True)
            else:
                # Default settings
                self.max_daily_calls = 400
                self.batch_size = 3
                self.api_delay = 2.0
                self.auto_retry_failed = True
                self.retry_interval_days = 7
                self.validation_enabled = True
                self.save_settings()
        except Exception as e:
            logger.error(f"Error loading settings: {e}")
            # Use defaults
            self.max_daily_calls = 400
            self.batch_size = 3
            self.api_delay = 2.0
            self.auto_retry_failed = True
            self.retry_interval_days = 7
            self.validation_enabled = True
    
    def save_settings(self):
        """Save import settings to file."""
        try:
            settings = {
                'max_daily_calls': self.max_daily_calls,
                'batch_size': self.batch_size,
                'api_delay': self.api_delay,
                'auto_retry_failed': self.auto_retry_failed,
                'retry_interval_days': self.retry_interval_days,
                'validation_enabled': self.validation_enabled,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.settings_file, 'w') as f:
                json.dump(settings, f, indent=2)
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
                self.last_run_date = datetime.now().date().isoformat()
                self.api_calls_today = 0
                self.save_progress()
        except Exception as e:
            logger.error(f"Error loading progress: {e}")
            # Initialize with defaults
            self.current_soc_index = 0
            self.processed_count = 0
            self.last_run_date = datetime.now().date().isoformat()
            self.api_calls_today = 0
    
    def load_failed_socs(self):
        """Load failed SOCs from file."""
        try:
            if os.path.exists(self.failed_file):
                with open(self.failed_file, 'r') as f:
                    self.failed_socs = json.load(f)
            else:
                self.failed_socs = []
                self.save_failed_socs()
        except Exception as e:
            logger.error(f"Error loading failed SOCs: {e}")
            self.failed_socs = []
    
    def save_failed_socs(self):
        """Save failed SOCs to file."""
        try:
            with open(self.failed_file, 'w') as f:
                json.dump(self.failed_socs, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving failed SOCs: {e}")
    
    def save_progress(self):
        """Save persistent progress to file."""
        try:
            progress_data = {
                'current_soc_index': self.current_soc_index,
                'processed_count': self.processed_count,
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
        """Process a batch of SOCs automatically."""
        if not bls_mapper_available:
            logger.error("BLS job mapper not available. Cannot process batch.")
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
            # Check if we should retry failed SOCs
            if self.auto_retry_failed and self.failed_socs:
                retry_candidates = []
                now = datetime.now()
                
                for failed in self.failed_socs:
                    # Check if enough time has passed since last attempt
                    if 'timestamp' in failed:
                        try:
                            last_attempt = datetime.fromisoformat(failed['timestamp'])
                            days_since = (now - last_attempt).days
                            if days_since >= self.retry_interval_days:
                                retry_candidates.append((failed.get('soc_code'), failed.get('title')))
                                if len(retry_candidates) >= self.batch_size:
                                    break
                        except (ValueError, TypeError):
                            # If timestamp is invalid, add to retry candidates
                            retry_candidates.append((failed.get('soc_code'), failed.get('title')))
                
                if retry_candidates:
                    logger.info(f"Auto-import: Retrying {len(retry_candidates)} failed SOCs")
                    soc_batch = retry_candidates
                else:
                    logger.info("Auto-import: No more SOCs to process - import complete!")
                    return False
            else:
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
                    
                    # Remove from failed SOCs if it was there
                    self.failed_socs = [f for f in self.failed_socs if f.get('soc_code') != soc_code]
                    
                    logger.info(f"Auto-import: Successfully processed {soc_code} ({self.processed_count} total)")
                    
                    # Validate data if enabled
                    if self.validation_enabled:
                        validation_result = self.validate_soc_data(soc_code)
                        if not validation_result['valid']:
                            logger.warning(f"Data validation failed for {soc_code}: {validation_result['reason']}")
                else:
                    logger.warning(f"Auto-import: Failed to process {soc_code}: {message}")
                    # Add to failed SOCs if not already there
                    if not any(f.get('soc_code') == soc_code for f in self.failed_socs):
                        self.failed_socs.append({
                            "soc_code": soc_code,
                            "title": job_title,
                            "reason": message,
                            "timestamp": datetime.now().isoformat(),
                            "attempts": 1
                        })
                    else:
                        # Update existing failed SOC
                        for failed in self.failed_socs:
                            if failed.get('soc_code') == soc_code:
                                failed['reason'] = message
                                failed['timestamp'] = datetime.now().isoformat()
                                failed['attempts'] = failed.get('attempts', 0) + 1
                                break
                    
                    self.current_soc_index += 1
                
                # Save progress after each successful processing
                self.save_progress()
                self.save_failed_socs()
                
                # Respect API rate limits
                time.sleep(self.api_delay)
                
            except Exception as e:
                logger.error(f"Auto-import: Exception processing {soc_code}: {e}")
                # Add to failed SOCs
                if not any(f.get('soc_code') == soc_code for f in self.failed_socs):
                    self.failed_socs.append({
                        "soc_code": soc_code,
                        "title": job_title,
                        "reason": str(e),
                        "timestamp": datetime.now().isoformat(),
                        "attempts": 1
                    })
                else:
                    # Update existing failed SOC
                    for failed in self.failed_socs:
                        if failed.get('soc_code') == soc_code:
                            failed['reason'] = str(e)
                            failed['timestamp'] = datetime.now().isoformat()
                            failed['attempts'] = failed.get('attempts', 0) + 1
                            break
                
                self.current_soc_index += 1
                self.save_progress()
                self.save_failed_socs()
        
        if processed_count > 0:
            logger.info(f"Auto-import: Processed {processed_count} SOCs this batch. Total: {self.processed_count}")
        
        return processed_count > 0
    
    def validate_soc_data(self, soc_code):
        """Validate data for a SOC code."""
        if not database_available or not db_engine:
            return {'valid': False, 'reason': 'Database not available'}
            
        try:
            with db_engine.connect() as connection:
                # Check if data exists
                result = connection.execute(text("""
                    SELECT * FROM bls_data WHERE occupation_code = :soc_code
                """), {'soc_code': soc_code}).fetchone()
                
                if not result:
                    return {'valid': False, 'reason': 'No data found'}
                    
                # Check for required fields
                required_fields = ['current_employment', 'projected_employment', 'percent_change', 
                                  'annual_job_openings', 'median_wage']
                
                missing_fields = []
                for field in required_fields:
                    if field not in result or result[field] is None:
                        missing_fields.append(field)
                
                if missing_fields:
                    return {'valid': False, 'reason': f'Missing fields: {", ".join(missing_fields)}'}
                
                return {'valid': True}
        except Exception as e:
            return {'valid': False, 'reason': str(e)}
    
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
            'last_run_date': self.last_run_date,
            'validation_enabled': self.validation_enabled,
            'auto_retry_failed': self.auto_retry_failed,
            'retry_interval_days': self.retry_interval_days
        }
    
    def retry_failed_soc(self, soc_code):
        """Retry a specific failed SOC."""
        if not bls_mapper_available:
            return False, "BLS job mapper not available"
            
        # Find the SOC in failed list
        for failed in self.failed_socs:
            if failed.get('soc_code') == soc_code:
                job_title = failed.get('title', 'Unknown')
                
                try:
                    logger.info(f"Manual retry: Processing {soc_code} - {job_title}")
                    success, message = bls_job_mapper.fetch_and_process_soc_data(
                        soc_code, job_title, db_engine
                    )
                    
                    if success:
                        self.processed_count += 1
                        self.api_calls_today += 1
                        
                        # Remove from failed SOCs
                        self.failed_socs = [f for f in self.failed_socs if f.get('soc_code') != soc_code]
                        self.save_failed_socs()
                        self.save_progress()
                        
                        return True, f"Successfully processed {soc_code}"
                    else:
                        # Update failed SOC
                        failed['reason'] = message
                        failed['timestamp'] = datetime.now().isoformat()
                        failed['attempts'] = failed.get('attempts', 0) + 1
                        self.save_failed_socs()
                        
                        return False, message
                except Exception as e:
                    # Update failed SOC
                    failed['reason'] = str(e)
                    failed['timestamp'] = datetime.now().isoformat()
                    failed['attempts'] = failed.get('attempts', 0) + 1
                    self.save_failed_socs()
                    
                    return False, str(e)
        
        return False, f"SOC code {soc_code} not found in failed list"
    
    def reset_progress(self):
        """Reset progress to start from beginning."""
        self.current_soc_index = 0
        self.processed_count = 0
        self.save_progress()
        logger.info("Auto-import: Progress reset to beginning")
        return True
    
    def clear_failed_socs(self):
        """Clear the list of failed SOCs."""
        self.failed_socs = []
        self.save_failed_socs()
        logger.info("Auto-import: Failed SOCs list cleared")
        return True

# Initialize the persistent auto-import manager  
if 'persistent_auto_import_manager' not in st.session_state:
    st.session_state.persistent_auto_import_manager = PersistentAutoImportManager()

# The system starts automatically - no need for conditional startup
auto_import_manager = st.session_state.persistent_auto_import_manager

# --- Main Admin App ---
def main():
    # Check authentication first
    if not check_admin_auth():
        admin_login_form()
        return
    
    # Admin Dashboard
    st.title("⚙️ iThriveAI Admin Dashboard")
    st.markdown("### BLS Data Import Management System")
    
    # System Status Overview
    st.markdown("## 📊 System Status")
    
    status = auto_import_manager.get_status()
    
    # Status Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if status['is_running']:
            st.success("🟢 Import Status: ACTIVE")
        else:
            st.error("🔴 Import Status: STOPPED")
    
    with col2:
        if database_available:
            st.success("🟢 Database: Connected")
        else:
            st.error("🔴 Database: Not Available")
    
    with col3:
        if bls_mapper_available:
            st.success("🟢 BLS Mapper: Available")
        else:
            st.error("🔴 BLS Mapper: Not Available")
    
    with col4:
        api_usage = status['api_calls_today'] / status['max_daily_calls'] if status['max_daily_calls'] > 0 else 0
        if api_usage < 0.9:
            st.success(f"🟢 API Usage: {status['api_calls_today']}/{status['max_daily_calls']}")
        else:
            st.warning(f"🟡 API Usage: {status['api_calls_today']}/{status['max_daily_calls']}")
    
    # Progress Bar
    st.markdown("### Import Progress")
    progress_pct = status['progress_percentage']
    st.progress(progress_pct, text=f"Overall Progress: {status['processed_count']:,} / {status['total_socs']:,} SOCs ({progress_pct:.1%})")
    
    # Main Tabs
    tabs = st.tabs(["Control Panel", "Detailed Progress", "Failed SOCs", "Settings", "System Health", "Data Validation"])
    
    # Tab 1: Control Panel
    with tabs[0]:
        st.markdown("## 🎛️ Control Panel")
        
        # Control Buttons
        col1, col2, col3 = st.columns(3)
        with col1:
            if status['is_running']:
                if st.button("⏹️ Stop Import Process", key="stop_import"):
                    auto_import_manager.stop_auto_import()
                    st.success("Import process stopped")
                    st.rerun()
            else:
                if st.button("▶️ Start Import Process", key="start_import"):
                    auto_import_manager.start_auto_import()
                    st.success("Import process started")
                    st.rerun()
        
        with col2:
            if st.button("🔄 Process Batch Now", key="process_batch"):
                with st.spinner("Processing batch..."):
                    success = auto_import_manager.process_batch_automatically()
                    if success:
                        st.success("Batch processed successfully!")
                    else:
                        st.warning("No items to process or daily limit reached")
                    st.rerun()
        
        with col3:
            if st.button("🔄 Refresh Status", key="refresh_status"):
                st.rerun()
        
        # Batch Size Control
        st.markdown("### Batch Processing")
        col1, col2 = st.columns(2)
        with col1:
            new_batch_size = st.number_input(
                "Batch Size", 
                min_value=1, 
                max_value=50, 
                value=auto_import_manager.batch_size,
                help="Number of SOCs to process in each batch"
            )
            if new_batch_size != auto_import_manager.batch_size:
                auto_import_manager.batch_size = new_batch_size
                auto_import_manager.save_settings()
        
        with col2:
            new_delay = st.number_input(
                "API Delay (seconds)", 
                min_value=0.5, 
                max_value=10.0, 
                value=auto_import_manager.api_delay,
                step=0.5,
                help="Delay between API calls to respect rate limits"
            )
            if new_delay != auto_import_manager.api_delay:
                auto_import_manager.api_delay = new_delay
                auto_import_manager.save_settings()
        
        # Reset Controls
        st.markdown("### Reset Controls")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Reset Progress", key="reset_progress"):
                if st.session_state.get("confirm_reset", False):
                    auto_import_manager.reset_progress()
                    st.session_state.confirm_reset = False
                    st.success("Progress reset to beginning")
                    st.rerun()
                else:
                    st.session_state.confirm_reset = True
                    st.warning("⚠️ Are you sure? This will reset all progress. Click again to confirm.")
        
        with col2:
            if st.button("🗑️ Clear Failed SOCs", key="clear_failed"):
                if st.session_state.get("confirm_clear_failed", False):
                    auto_import_manager.clear_failed_socs()
                    st.session_state.confirm_clear_failed = False
                    st.success("Failed SOCs list cleared")
                    st.rerun()
                else:
                    st.session_state.confirm_clear_failed = True
                    st.warning("⚠️ Are you sure? This will clear all failed SOCs. Click again to confirm.")
    
    # Tab 2: Detailed Progress
    with tabs[1]:
        st.markdown("## 📈 Detailed Progress")
        
        # Progress Statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Processed SOCs", f"{status['processed_count']:,}")
        with col2:
            st.metric("Remaining SOCs", f"{status['total_socs'] - status['processed_count']:,}")
        with col3:
            st.metric("Failed SOCs", f"{status['failed_count']:,}")
        
        # Progress Chart
        if database_available and db_engine:
            try:
                with db_engine.connect() as connection:
                    # Get daily progress
                    result = connection.execute(text("""
                        SELECT DATE(created_at) as date, COUNT(*) as count
                        FROM bls_data
                        WHERE created_at IS NOT NULL
                        GROUP BY DATE(created_at)
                        ORDER BY date
                    """)).fetchall()
                    
                    if result:
                        progress_data = pd.DataFrame(result, columns=["date", "count"])
                        progress_data["cumulative"] = progress_data["count"].cumsum()
                        
                        # Create chart
                        fig = px.line(
                            progress_data, 
                            x="date", 
                            y="cumulative",
                            title="Cumulative Import Progress",
                            labels={"cumulative": "Total SOCs Imported", "date": "Date"}
                        )
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Daily import chart
                        fig2 = px.bar(
                            progress_data,
                            x="date",
                            y="count",
                            title="Daily Import Counts",
                            labels={"count": "SOCs Imported", "date": "Date"}
                        )
                        fig2.update_layout(height=400)
                        st.plotly_chart(fig2, use_container_width=True)
                    else:
                        st.info("No progress data available yet")
            except Exception as e:
                st.error(f"Error retrieving progress data: {e}")
        else:
            st.warning("Database not available. Cannot display detailed progress.")
        
        # Recent Activity
        st.markdown("### Recent Activity")
        if database_available and db_engine:
            try:
                with db_engine.connect() as connection:
                    result = connection.execute(text("""
                        SELECT occupation_code, job_title, created_at
                        FROM bls_data
                        WHERE created_at IS NOT NULL
                        ORDER BY created_at DESC
                        LIMIT 20
                    """)).fetchall()
                    
                    if result:
                        recent_data = pd.DataFrame(result, columns=["SOC Code", "Job Title", "Timestamp"])
                        st.dataframe(recent_data, use_container_width=True)
                    else:
                        st.info("No recent activity data available")
            except Exception as e:
                st.error(f"Error retrieving recent activity: {e}")
        else:
            st.warning("Database not available. Cannot display recent activity.")
    
    # Tab 3: Failed SOCs
    with tabs[2]:
        st.markdown("## ❌ Failed SOCs")
        
        if status['failed_count'] > 0:
            # Convert failed SOCs to DataFrame
            failed_df = pd.DataFrame(auto_import_manager.failed_socs)
            
            # Add retry button column
            failed_df['retry'] = False
            
            # Display in editable table
            edited_df = st.data_editor(
                failed_df,
                column_config={
                    "retry": st.column_config.CheckboxColumn(
                        "Retry",
                        help="Select SOCs to retry",
                        default=False,
                    )
                },
                use_container_width=True
            )
            
            # Process retries
            if st.button("Retry Selected SOCs", key="retry_selected"):
                with st.spinner("Retrying selected SOCs..."):
                    retry_count = 0
                    success_count = 0
                    
                    for _, row in edited_df[edited_df['retry']].iterrows():
                        soc_code = row.get('soc_code')
                        if soc_code:
                            retry_count += 1
                            success, message = auto_import_manager.retry_failed_soc(soc_code)
                            if success:
                                success_count += 1
                    
                    if retry_count > 0:
                        st.success(f"Retried {retry_count} SOCs, {success_count} successful")
                        st.rerun()
                    else:
                        st.info("No SOCs selected for retry")
            
            # Failure analysis
            if len(failed_df) > 0:
                st.markdown("### Failure Analysis")
                
                # Group by reason
                if 'reason' in failed_df.columns:
                    reason_counts = failed_df['reason'].value_counts().reset_index()
                    reason_counts.columns = ['Reason', 'Count']
                    
                    fig = px.pie(
                        reason_counts,
                        values='Count',
                        names='Reason',
                        title="Failure Reasons"
                    )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("No failed SOCs! All data has been successfully imported.")
    
    # Tab 4: Settings
    with tabs[3]:
        st.markdown("## ⚙️ Settings")
        
        st.markdown("### API Settings")
        col1, col2 = st.columns(2)
        with col1:
            new_max_calls = st.number_input(
                "Max Daily API Calls",
                min_value=1,
                max_value=1000,
                value=auto_import_manager.max_daily_calls,
                help="Maximum number of API calls per day"
            )
            if new_max_calls != auto_import_manager.max_daily_calls:
                auto_import_manager.max_daily_calls = new_max_calls
                auto_import_manager.save_settings()
        
        st.markdown("### Retry Settings")
        col1, col2 = st.columns(2)
        with col1:
            auto_retry = st.checkbox(
                "Auto-retry Failed SOCs",
                value=auto_import_manager.auto_retry_failed,
                help="Automatically retry failed SOCs after the retry interval"
            )
            if auto_retry != auto_import_manager.auto_retry_failed:
                auto_import_manager.auto_retry_failed = auto_retry
                auto_import_manager.save_settings()
        
        with col2:
            retry_days = st.number_input(
                "Retry Interval (days)",
                min_value=1,
                max_value=30,
                value=auto_import_manager.retry_interval_days,
                help="Number of days to wait before retrying failed SOCs"
            )
            if retry_days != auto_import_manager.retry_interval_days:
                auto_import_manager.retry_interval_days = retry_days
                auto_import_manager.save_settings()
        
        st.markdown("### Validation Settings")
        validation_enabled = st.checkbox(
            "Enable Data Validation",
            value=auto_import_manager.validation_enabled,
            help="Validate imported data for completeness"
        )
        if validation_enabled != auto_import_manager.validation_enabled:
            auto_import_manager.validation_enabled = validation_enabled
            auto_import_manager.save_settings()
        
        # Save settings button
        if st.button("Save All Settings", key="save_settings"):
            auto_import_manager.save_settings()
            st.success("Settings saved successfully")
    
    # Tab 5: System Health
    with tabs[4]:
        st.markdown("## 🩺 System Health")
        
        # Database Health
        st.markdown("### Database Health")
        if database_available and db_engine and check_database_health:
            try:
                health_status = check_database_health()
                if isinstance(health_status, str):
                    if health_status.lower() == "ok":
                        st.success("✅ Database is healthy")
                    else:
                        st.error(f"❌ Database health check failed: {health_status}")
                else:
                    status = health_status.get("status", "unknown")
                    message = health_status.get("message", "No details available")
                    if status.lower() == "ok":
                        st.success("✅ Database is healthy")
                    else:
                        st.error(f"❌ Database health check failed: {message}")
                
                # Database stats
                if get_database_stats:
                    stats = get_database_stats()
                    if stats:
                        st.markdown("### Database Statistics")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total SOCs", stats.get("total_socs", "N/A"))
                        with col2:
                            st.metric("Imported SOCs", stats.get("imported_socs", "N/A"))
                        with col3:
                            st.metric("Job Searches", stats.get("job_searches", "N/A"))
            except Exception as e:
                st.error(f"Error checking database health: {e}")
        else:
            st.error("❌ Database or health check function not available")
        
        # Thread Health
        st.markdown("### Thread Health")
        if auto_import_manager.thread and auto_import_manager.thread.is_alive():
            st.success("✅ Import thread is running")
        else:
            if auto_import_manager.is_running:
                st.warning("⚠️ Import thread is marked as running but thread is not active")
            else:
                st.info("ℹ️ Import thread is not running (stopped)")
        
        # File System Health
        st.markdown("### File System Health")
        col1, col2, col3 = st.columns(3)
        with col1:
            if os.path.exists(auto_import_manager.progress_file):
                st.success(f"✅ Progress file exists ({os.path.getsize(auto_import_manager.progress_file)} bytes)")
            else:
                st.error("❌ Progress file missing")
        
        with col2:
            if os.path.exists(auto_import_manager.settings_file):
                st.success(f"✅ Settings file exists ({os.path.getsize(auto_import_manager.settings_file)} bytes)")
            else:
                st.error("❌ Settings file missing")
        
        with col3:
            if os.path.exists(auto_import_manager.failed_file):
                st.success(f"✅ Failed SOCs file exists ({os.path.getsize(auto_import_manager.failed_file)} bytes)")
            else:
                st.error("❌ Failed SOCs file missing")
        
        # System Info
        st.markdown("### System Information")
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"Python Version: {sys.version.split()[0]}")
        with col2:
            st.info(f"Streamlit Version: {st.__version__}")
        
        # Memory Usage
        import psutil
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            st.info(f"Memory Usage: {memory_info.rss / (1024 * 1024):.2f} MB")
        except:
            st.info("Memory usage information not available")
    
    # Tab 6: Data Validation
    with tabs[5]:
        st.markdown("## 🔍 Data Validation")
        
        # Validation Tools
        st.markdown("### Validation Tools")
        
        validation_options = st.selectbox(
            "Select Validation Type",
            ["Complete Data Check", "Missing Fields Check", "Data Consistency Check", "Custom SOC Validation"]
        )
        
        if validation_options == "Custom SOC Validation":
            soc_to_validate = st.text_input("Enter SOC Code to Validate", "")
            if st.button("Validate SOC", key="validate_soc") and soc_to_validate:
                with st.spinner("Validating..."):
                    validation_result = auto_import_manager.validate_soc_data(soc_to_validate)
                    if validation_result['valid']:
                        st.success(f"✅ SOC {soc_to_validate} data is valid")
                    else:
                        st.error(f"❌ SOC {soc_to_validate} validation failed: {validation_result['reason']}")
        
        elif st.button("Run Validation", key="run_validation"):
            if not database_available or not db_engine:
                st.error("Database not available. Cannot run validation.")
            else:
                with st.spinner("Running validation..."):
                    try:
                        if validation_options == "Complete Data Check":
                            # Check for completeness of all SOCs
                            with db_engine.connect() as connection:
                                result = connection.execute(text("""
                                    SELECT ts.soc_code, ts.title, 
                                           CASE WHEN bd.occupation_code IS NULL THEN 'Missing' ELSE 'Present' END as status
                                    FROM target_socs ts
                                    LEFT JOIN bls_data bd ON ts.soc_code = bd.occupation_code
                                    ORDER BY status, ts.soc_code
                                """)).fetchall()
                                
                                if result:
                                    df = pd.DataFrame(result, columns=["SOC Code", "Job Title", "Status"])
                                    missing_count = len(df[df["Status"] == "Missing"])
                                    present_count = len(df[df["Status"] == "Present"])
                                    
                                    # Summary
                                    st.markdown(f"### Validation Results")
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.metric("Present SOCs", present_count)
                                    with col2:
                                        st.metric("Missing SOCs", missing_count)
                                    
                                    # Chart
                                    fig = px.pie(
                                        values=[present_count, missing_count],
                                        names=["Present", "Missing"],
                                        title="Data Completeness"
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                    
                                    # Table
                                    st.dataframe(df, use_container_width=True)
                                else:
                                    st.warning("No data available for validation")
                        
                        elif validation_options == "Missing Fields Check":
                            # Check for missing fields in existing data
                            with db_engine.connect() as connection:
                                result = connection.execute(text("""
                                    SELECT occupation_code, job_title,
                                           CASE WHEN current_employment IS NULL THEN 1 ELSE 0 END as missing_employment,
                                           CASE WHEN projected_employment IS NULL THEN 1 ELSE 0 END as missing_projected,
                                           CASE WHEN percent_change IS NULL THEN 1 ELSE 0 END as missing_change,
                                           CASE WHEN annual_job_openings IS NULL THEN 1 ELSE 0 END as missing_openings,
                                           CASE WHEN median_wage IS NULL THEN 1 ELSE 0 END as missing_wage
                                    FROM bls_data
                                    WHERE occupation_code IS NOT NULL
                                """)).fetchall()
                                
                                if result:
                                    df = pd.DataFrame(result, columns=[
                                        "SOC Code", "Job Title", "Missing Employment", "Missing Projected",
                                        "Missing Change", "Missing Openings", "Missing Wage"
                                    ])
                                    
                                    # Add total missing column
                                    df["Total Missing"] = df.iloc[:, 2:].sum(axis=1)
                                    
                                    # Filter to show only rows with missing data
                                    df_missing = df[df["Total Missing"] > 0]
                                    
                                    # Summary
                                    st.markdown(f"### Missing Fields Results")
                                    st.metric("SOCs with Missing Fields", len(df_missing))
                                    
                                    # Missing fields by type
                                    missing_by_field = {
                                        "Employment": df["Missing Employment"].sum(),
                                        "Projected": df["Missing Projected"].sum(),
                                        "Change": df["Missing Change"].sum(),
                                        "Openings": df["Missing Openings"].sum(),
                                        "Wage": df["Missing Wage"].sum()
                                    }
                                    
                                    # Chart
                                    fig = px.bar(
                                        x=list(missing_by_field.keys()),
                                        y=list(missing_by_field.values()),
                                        title="Missing Fields by Type"
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                    
                                    # Table of SOCs with missing data
                                    if len(df_missing) > 0:
                                        st.markdown("### SOCs with Missing Fields")
                                        st.dataframe(df_missing, use_container_width=True)
                                    else:
                                        st.success("No SOCs with missing fields!")
                                else:
                                    st.warning("No data available for validation")
                        
                        elif validation_options == "Data Consistency Check":
                            # Check for data consistency issues
                            with db_engine.connect() as connection:
                                result = connection.execute(text("""
                                    SELECT occupation_code, job_title,
                                           current_employment, projected_employment, percent_change,
                                           CASE 
                                               WHEN current_employment = 0 AND projected_employment > 0 THEN 'Zero current, non-zero projected'
                                               WHEN projected_employment = 0 AND current_employment > 0 THEN 'Zero projected, non-zero current'
                                               WHEN percent_change IS NOT NULL AND 
                                                    ABS((projected_employment - current_employment) / NULLIF(current_employment, 0) * 100 - percent_change) > 1 
                                                    THEN 'Percent change mismatch'
                                               ELSE 'OK'
                                           END as consistency_check
                                    FROM bls_data
                                    WHERE occupation_code IS NOT NULL
                                """)).fetchall()
                                
                                if result:
                                    df = pd.DataFrame(result, columns=[
                                        "SOC Code", "Job Title", "Current Employment", 
                                        "Projected Employment", "Percent Change", "Consistency Check"
                                    ])
                                    
                                    # Filter to show only rows with consistency issues
                                    df_issues = df[df["Consistency Check"] != "OK"]
                                    
                                    # Summary
                                    st.markdown(f"### Data Consistency Results")
                                    st.metric("SOCs with Consistency Issues", len(df_issues))
                                    
                                    # Issues by type
                                    if len(df_issues) > 0:
                                        issues_by_type = df_issues["Consistency Check"].value_counts().reset_index()
                                        issues_by_type.columns = ["Issue Type", "Count"]
                                        
                                        # Chart
                                        fig = px.bar(
                                            issues_by_type,
                                            x="Issue Type",
                                            y="Count",
                                            title="Consistency Issues by Type"
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                                        
                                        # Table of SOCs with issues
                                        st.markdown("### SOCs with Consistency Issues")
                                        st.dataframe(df_issues, use_container_width=True)
                                    else:
                                        st.success("No data consistency issues found!")
                                else:
                                    st.warning("No data available for validation")
                    except Exception as e:
                        st.error(f"Error running validation: {e}")
    
    # Logout button
    st.markdown("---")
    if st.button("🚪 Logout", key="admin_logout"):
        st.session_state.admin_authenticated = False
        st.success("Logged out successfully")
        st.rerun()

if __name__ == "__main__":
    main()
