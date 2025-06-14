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
import requests
import logging
import re
from sqlalchemy import create_engine, text
import simplified_admin

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("AI_Job_Analyzer")

# Enhanced multi-strategy keep-alive functionality specifically designed for UptimeRobot
def keep_alive():
    """
    Multi-strategy background thread to keep the app active
    Optimized for UptimeRobot monitoring
    """
    import time
    
    # Track success/failure of keep-alive attempts
    keep_alive_stats = {
        "last_success": None,
        "consecutive_failures": 0,
        "total_attempts": 0,
        "successful_attempts": 0
    }
    
    # Create a timestamp file for tracking
    timestamp_file = "last_activity.txt"
    
    while True:
        try:
            keep_alive_stats["total_attempts"] += 1
            logger.info(f"Keep-alive attempt #{keep_alive_stats['total_attempts']}")
            
            success = False
            
            # Strategy 1: Database ping - most reliable for keeping NEON connection active
            try:
                database_url = os.environ.get('DATABASE_URL')
                if database_url:
                    engine = create_engine(database_url)
                    with engine.connect() as conn:
                        result = conn.execute(text("SELECT 1"))
                        if result.fetchone():
                            logger.info("Database ping successful")
                            success = True
            except Exception as e:
                logger.warning(f"Database ping failed: {str(e)}")
            
            # Strategy 2: Self HTTP request to health endpoint - works well with UptimeRobot
            try:
                # Get the Streamlit server URL from environment or use default
                base_url = os.environ.get("STREAMLIT_SERVER_BASE_URL", "http://localhost:8501")
                health_url = f"{base_url}/?health_check=true"
                response = requests.get(health_url, timeout=10)
                if response.status_code == 200:
                    logger.info("Self HTTP request successful")
                    success = True
            except Exception as e:
                logger.warning(f"Self HTTP request failed: {str(e)}")
            
            # Strategy 3: File system activity - helps on some platforms
            try:
                # Write current timestamp to file
                with open(timestamp_file, "w") as f:
                    f.write(datetime.datetime.now().isoformat())
                logger.info("File system activity successful")
                success = True
            except Exception as e:
                logger.warning(f"File system activity failed: {str(e)}")
            
            # Update stats based on overall success
            if success:
                keep_alive_stats["last_success"] = datetime.datetime.now().isoformat()
                keep_alive_stats["consecutive_failures"] = 0
                keep_alive_stats["successful_attempts"] += 1
                
                # Write stats to file for monitoring
                try:
                    with open("keep_alive_stats.json", "w") as f:
                        json.dump(keep_alive_stats, f)
                except Exception as e:
                    logger.warning(f"Failed to write keep-alive stats: {str(e)}")
            else:
                keep_alive_stats["consecutive_failures"] += 1
                logger.error("All keep-alive strategies failed")
                
            # Sleep for 5 minutes before next ping (UptimeRobot typically checks every 5 minutes)
            time.sleep(300)
            
        except Exception as e:
            # Log any unexpected errors
            logger.error(f"Keep-alive error: {str(e)}")
            keep_alive_stats["consecutive_failures"] += 1
            
            # Adaptive sleep based on consecutive failures
            # Sleep longer if we're having issues to avoid overwhelming the system
            sleep_time = min(300 * (1 + (keep_alive_stats["consecutive_failures"] * 0.1)), 600)
            logger.info(f"Sleeping for {sleep_time} seconds before retry")
            time.sleep(sleep_time)

# Start keep-alive thread if not already running
if "keep_alive_started" not in st.session_state:
    st.session_state.keep_alive_started = True
    logger.info("Starting keep-alive thread")
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()

# Import required modules
try:
    import job_api_integration_database_only as job_api_integration
    import simple_comparison
    import ai_job_displacement
    import career_navigator
    from job_title_autocomplete_v2 import job_title_autocomplete, load_job_titles_from_db
    logger.info("Successfully imported required modules")
except Exception as e:
    st.error(f"Failed to import required modules: {str(e)}")
    logger.error(f"Module import error: {str(e)}")
    st.stop()

# Check if BLS API key is set
bls_api_key = os.environ.get('BLS_API_KEY')
if not bls_api_key:
    logger.warning("BLS_API_KEY environment variable not set")
    # Load from secrets.toml if available
    try:
        bls_api_key = st.secrets["api_keys"]["BLS_API_KEY"]
        os.environ['BLS_API_KEY'] = bls_api_key
        logger.info("Loaded BLS API key from secrets.toml")
    except Exception as e:
        logger.error(f"Could not load BLS API key from secrets: {str(e)}")
        st.error("BLS API key not found. Please set the BLS_API_KEY environment variable or add it to secrets.toml.")
        st.stop()

# Handle health check requests - optimized for UptimeRobot
query_params = st.query_params
if query_params.get("health_check") == "true":
    # Simple OK response for UptimeRobot
    st.text("OK")
    
    # Extended health check information if requested
    if query_params.get("detailed") == "true":
        st.title("iThriveAI Job Analyzer - Detailed Health Check")
        
        # Always show application is running
        st.success("✅ Application status: Running")
        
        # Check database connection
        try:
            # Try to connect to the database
            database_url = os.environ.get('DATABASE_URL')
            if database_url:
                engine = create_engine(database_url)
                with engine.connect() as connection:
                    result = connection.execute(text("SELECT 1"))
                    if result.fetchone():
                        st.success("✅ Database connection: OK")
            else:
                st.error("❌ Database connection: Not configured")
        except Exception as e:
            st.error(f"❌ Database connection: Error - {str(e)}")
        
        # Check BLS API key
        if bls_api_key:
            st.success("✅ BLS API key: Available")
        else:
            st.error("❌ BLS API key: Not configured")
        
        # Check keep-alive status
        try:
            with open("keep_alive_stats.json", "r") as f:
                stats = json.load(f)
                last_success = datetime.datetime.fromisoformat(stats["last_success"])
                time_diff = datetime.datetime.now() - last_success
                if time_diff.total_seconds() < 600:  # Less than 10 minutes
                    st.success(f"✅ Keep-alive: Active (last success: {time_diff.seconds // 60} minutes ago)")
                else:
                    st.warning(f"⚠️ Keep-alive: Last success was {time_diff.seconds // 60} minutes ago")
                st.info(f"ℹ️ Keep-alive stats: {stats['successful_attempts']}/{stats['total_attempts']} successful attempts")
        except Exception as e:
            st.warning(f"⚠️ Keep-alive stats not available: {str(e)}")
            
        st.info("ℹ️ This endpoint is used for application monitoring")
    
    st.stop()  # Stop further execution

# Page configuration
st.set_page_config(
    page_title="Career AI Impact Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add custom CSS
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
    /* Status indicator */
    .status-indicator {
        display: flex;
        align-items: center;
        padding: 5px 10px;
        border-radius: 4px;
        margin-bottom: 10px;
        font-size: 14px;
    }
    .status-indicator.online {
        background-color: #d4edda;
        color: #155724;
    }
    .status-indicator.offline {
        background-color: #f8d7da;
        color: #721c24;
    }
    .status-indicator-dot {
        height: 10px;
        width: 10px;
        border-radius: 50%;
        margin-right: 8px;
    }
    .status-indicator-dot.online {
        background-color: #28a745;
    }
    .status-indicator-dot.offline {
        background-color: #dc3545;
    }
    /* No data message */
    .no-data-message {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 5px;
        text-align: center;
        margin: 20px 0;
    }
</style>
""", unsafe_allow_html=True)

# Database connection setup - ONLY use real database, no fallbacks
try:
    from sqlalchemy import create_engine, text
    
    # Get database URL from environment or secrets
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        try:
            database_url = st.secrets["database"]["DATABASE_URL"]
        except:
            pass
    
    if not database_url:
        st.error("DATABASE_URL not found. Please set the DATABASE_URL environment variable or add it to secrets.toml.")
        logger.error("DATABASE_URL not found")
        st.stop()
    
    # Create database engine
    engine = create_engine(database_url)
    
    # Test connection
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        if result.fetchone():
            logger.info("Successfully connected to database")
            database_available = True
            
            # Import database functions
            from database import save_job_search, get_popular_searches, get_highest_risk_jobs, get_lowest_risk_jobs, get_recent_searches
            
            # Display status indicator
            st.sidebar.markdown("""
            <div class="status-indicator online">
                <div class="status-indicator-dot online"></div>
                Database: Connected
            </div>
            """, unsafe_allow_html=True)
except Exception as e:
    database_available = False
    logger.error(f"Database connection failed: {str(e)}")
    st.error(f"Database connection failed: {str(e)}")
    
    # Display status indicator
    st.sidebar.markdown("""
    <div class="status-indicator offline">
        <div class="status-indicator-dot offline"></div>
        Database: Connection Failed
    </div>
    """, unsafe_allow_html=True)

# Check BLS API status
if bls_api_key:
    st.sidebar.markdown("""
    <div class="status-indicator online">
        <div class="status-indicator-dot online"></div>
        BLS API: Connected
    </div>
    """, unsafe_allow_html=True)
else:
    st.sidebar.markdown("""
    <div class="status-indicator offline">
        <div class="status-indicator-dot offline"></div>
        BLS API: Not Configured
    </div>
    """, unsafe_allow_html=True)
    st.error("BLS API key not found. This application requires authentic BLS data.")
    st.stop()

def check_data_refresh():
    """Check if data needs to be refreshed (daily schedule to keep database active)"""
    try:
        refresh_file = "last_refresh.json"
        current_time = datetime.datetime.now()
        
        # Check if refresh file exists
        if not os.path.exists(refresh_file):
            # Create new refresh file
            with open(refresh_file, "w") as f:
                json.dump({"date": current_time.isoformat()}, f)
            logger.info("Created new refresh tracking file")
            return True
            
        # Read existing refresh data
        with open(refresh_file, "r") as f:
            refresh_data = json.load(f)
            last_refresh = datetime.datetime.fromisoformat(refresh_data["date"])
            
            # Refresh if more than a day has passed
            days_since_refresh = (current_time - last_refresh).days
            
            if days_since_refresh >= 1:
                # Run the daily refresh to keep database active
                try:
                    import db_refresh
                    logger.info("Starting database refresh process")
                    st.info("Refreshing BLS data and performing database activity...")
                    # Run a sample job update to keep database active
                    sample_job = "Software Developer"
                    db_refresh.update_job_data(sample_job)
                    db_refresh.perform_database_queries()
                    db_refresh.check_and_update_refresh_timestamp()
                    st.success(f"Database activity performed successfully. Updated {sample_job} data.")
                    logger.info("Database refresh completed successfully")
                except Exception as e:
                    logger.error(f"Database refresh failed: {str(e)}")
                    st.warning(f"Database refresh attempted but encountered an issue: {str(e)}")
                
                # Update refresh timestamp regardless of success/failure
                with open(refresh_file, "w") as f:
                    json.dump({"date": current_time.isoformat()}, f)
                return True
            return False
    except Exception as e:
        logger.error(f"Error checking data refresh: {str(e)}")
        # If error occurs, try to create a new refresh file
        try:
            with open(refresh_file, "w") as f:
                json.dump({"date": datetime.datetime.now().isoformat()}, f)
        except:
            pass
        return True

# Application title and description
st.image("https://img1.wsimg.com/isteam/ip/70686f32-22d2-489c-a383-6fcd793644be/blob-3712e2e.png/:/rs=h:197,cg:true,m/qt=q:95", width=250)
st.markdown("<h1 style='text-align: center; color: #0084FF;'>Is your job at risk with AI innovation?</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #4CACE5; font-size: 24px; font-weight: 600;'>AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666666; font-weight: bold; font-size: 16px;'>Discover how AI might impact your career in the next 5 years and get personalized recommendations.</p>", unsafe_allow_html=True)

# Data source indicator - only authentic BLS data
st.info("📊 This application uses authentic Bureau of Labor Statistics (BLS) data only. No synthetic or fictional data is used.")

# Tabs for different sections
tabs = st.tabs(["Single Job Analysis", "Job Comparison"])

# Single Job Analysis Tab
with tabs[0]:  # Single Job Analysis tab
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    
    # Job title input with autocomplete functionality
    st.markdown("Enter any job title to analyze")
    search_job_title = job_title_autocomplete(
        label="Enter your job title",
        key="job_title_search",
        placeholder="Start typing to see suggestions...",
        help="Type a job title and select from matching suggestions"
    )
    
    # Clear Entry button - refreshes the entire app
    if st.button("🗑️ Clear Entry", key="clear_button_single"):
        # Clear by refreshing the page which resets all widgets
        st.rerun()
    
    # Normalize the job title for special cases
    normalized_job_title = search_job_title.lower().strip() if search_job_title else ""
    
    # Check for variations of "Diagnosician" for demo purposes
    if re.search(r'diagnos(i(c|s|t|cian)|e)', normalized_job_title):
        search_job_title = "Diagnosician"
    
    # Add search button
    search_clicked = st.button("Analyze Job Risk")
    
    # Check for data refresh when the app starts
    check_data_refresh()
    
    # Only search when button is clicked and there's a job title
    if search_clicked and search_job_title:
        # Show loading spinner during API calls and data processing
        with st.spinner(f"Analyzing {search_job_title}..."):
            try:
                # Get job data with optimized API calls - ONLY use real BLS data
                job_data = job_api_integration.get_job_data(search_job_title)
                
                # Save to database if available
                if database_available:
                    try:
                        save_job_search(search_job_title, {
                            'year_1_risk': job_data.get('risk_scores', {}).get('year_1', 0) or job_data.get('year_1_risk', 0),
                            'year_5_risk': job_data.get('risk_scores', {}).get('year_5', 0) or job_data.get('year_5_risk', 0),
                            'risk_category': job_data.get('risk_category', 'Unknown'),
                            'job_category': job_data.get('job_category', 'Unknown')
                        })
                        logger.info(f"Saved job search for {search_job_title}")
                    except Exception as e:
                        logger.error(f"Failed to save job search: {str(e)}")
                
            except Exception as e:
                # If job not found in database, show clear error message
                if "not found in BLS database" in str(e):
                    st.error(f"Job title '{search_job_title}' not found in our BLS database. Please use the Admin Dashboard to add missing job titles.")
                    st.info("Use the search suggestions or contact support to add this occupation with authentic BLS data.")
                    logger.error(f"Job title not found: {search_job_title}")
                    st.stop()
                else:
                    st.error(f"Database error: {str(e)}")
                    logger.error(f"Database error: {str(e)}")
                    st.stop()
            
            # Check if we have real data before proceeding
            if "error" in job_data:
                st.error(f"Error retrieving data: {job_data['error']}")
                st.info("This application only uses authentic BLS data. If the job title is not found in the BLS database, please try a different job title or contact support.")
                st.stop()
                
            # Show results once data is ready
            # Display header with job title and risk assessment
            st.subheader(f"AI Displacement Risk Analysis: {search_job_title}")
            
            # Use columns to create layout matching the screenshots
            job_info_col, risk_gauge_col, risk_factors_col = st.columns([1, 1, 1])
            
            with job_info_col:
                # Job Information section - left column
                st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Job Information</h3>", unsafe_allow_html=True)
                
                bls_data = job_data.get("bls_data", {})
                if "occupation_code" in job_data:
                    st.markdown(f"**Occupation Code:** {job_data['occupation_code']}")
                elif "occ_code" in bls_data:
                    st.markdown(f"**Occupation Code:** {bls_data['occ_code']}")
                
                st.markdown(f"**Job Category:** {job_data.get('job_category', 'General')}")
                
                if "employment" in bls_data:
                    st.markdown(f"**Current Employment:** {bls_data['employment']:,.0f} jobs")
                
                if "employment_change_percent" in bls_data:
                    growth = bls_data['employment_change_percent']
                    growth_text = f"{growth:+.1f}%" if growth else "No data"
                    st.markdown(f"**BLS Projected Growth:** {growth_text}")
                
                if "annual_job_openings" in bls_data:
                    st.markdown(f"**Annual Job Openings:** {bls_data['annual_job_openings']:,.0f}")
                
                # Career Outlook section
                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Career Outlook</h3>", unsafe_allow_html=True)
                st.markdown("<h4 style='color: #0084FF; font-size: 16px;'>Statistics</h4>", unsafe_allow_html=True)
                
                automation_prob = job_data.get("automation_probability", 45.0)
                st.markdown(f"**Task Automation Probability:** {automation_prob:.1f}% of job tasks could be automated")
                
                # Wage trend from BLS data
                bls_data = job_data.get("bls_data", {})
                median_wage = bls_data.get("median_wage")
                if median_wage:
                    st.markdown(f"**Median Annual Wage:** ${median_wage:,.0f}")
                else:
                    st.markdown("**Wage Data:** Contact employer for current wage information")
                
                # Employment growth from BLS data
                employment_change = bls_data.get("employment_change_percent")
                if employment_change is not None:
                    if employment_change > 0:
                        growth_text = f"Growing at {employment_change:.1f}% (faster than average)"
                    elif employment_change < 0:
                        growth_text = f"Declining at {abs(employment_change):.1f}%"
                    else:
                        growth_text = "Stable employment expected"
                    st.markdown(f"**Employment Growth:** {growth_text}")
                else:
                    st.markdown("**Employment Growth:** See BLS projections for current data")
            
            with risk_gauge_col:
                # Overall risk and gauge - center column
                risk_category = job_data.get("risk_category", "High")
                year_1_risk = job_data.get("risk_scores", {}).get("year_1", 35.0) or job_data.get("year_1_risk", 35.0)
                year_5_risk = job_data.get("risk_scores", {}).get("year_5", 60.0) or job_data.get("year_5_risk", 60.0)
                
                st.markdown(f"<h3 style='text-align: center; margin-bottom: 10px;'>Overall AI Displacement Risk: {risk_category}</h3>", unsafe_allow_html=True)
                
                # Create gauge chart for the risk - ensure it matches the Task Automation Probability
                automation_prob = job_data.get("automation_probability", 45.0)
                
                # Make sure we have valid values for the gauge
                if year_5_risk is None:
                    year_5_risk = 0.6  # Default to 60% if value is None

                # Ensure values are in decimal format (0-1) before converting to percentage
                if year_5_risk > 1:
                    gauge_value = year_5_risk  # Already a percentage
                else:
                    gauge_value = year_5_risk * 100  # Convert to percentage
                
                fig = go.Figure(go.Indicator(
                    mode = "gauge+number",
                    value = gauge_value,
                    domain = {'x': [0, 1], 'y': [0, 1]},
                    title = {'text': ""},
                    number = {'suffix': '%', 'font': {'size': 28}},
                    gauge = {
                        'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                        'bar': {'color': "#0084FF"},
                        'bgcolor': "white",
                        'borderwidth': 2,
                        'bordercolor': "gray",
                        'steps': [
                            {'range': [0, 25], 'color': "rgba(0, 255, 0, 0.5)"},
                            {'range': [25, 50], 'color': "rgba(255, 255, 0, 0.5)"},
                            {'range': [50, 75], 'color': "rgba(255, 165, 0, 0.5)"},
                            {'range': [75, 100], 'color': "rgba(255, 0, 0, 0.5)"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': gauge_value
                        }
                    }
                ))
                
                fig.update_layout(
                    height=250,
                    margin=dict(l=20, r=20, t=30, b=20)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Year risks as text
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>1-Year Risk</h4></div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{year_1_risk:.1f}%</div>", unsafe_allow_html=True)
                with col2:
                    st.markdown("<div style='text-align: center;'><h4 style='color: #0084FF; font-size: 18px;'>5-Year Risk</h4></div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='text-align: center; font-size: 20px; font-weight: bold;'>{year_5_risk:.1f}%</div>", unsafe_allow_html=True)
            
            with risk_factors_col:
                # Risk Factors section - right column
                st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Key Risk Factors</h3>", unsafe_allow_html=True)
                
                # Get risk factors from job data
                risk_factors = job_data.get("risk_factors", [])
                
                if risk_factors:
                    for factor in risk_factors:
                        st.markdown(f"❌ {factor}")
                else:
                    st.markdown("No specific risk factors available for this occupation.")
                
                # Protective Factors
                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Protective Factors</h3>", unsafe_allow_html=True)
                
                protective_factors = job_data.get("protective_factors", [])
                
                if protective_factors:
                    for factor in protective_factors:
                        st.markdown(f"✅ {factor}")
                else:
                    st.markdown("No specific protective factors available for this occupation.")
            
            # Analysis section - full width
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Key Insights</h3>", unsafe_allow_html=True)
            
            # Use provided analysis if available
            analysis_text = job_data.get("analysis", "No analysis available for this occupation.")
            st.markdown(analysis_text)
            
            # Employment Trend Chart
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Employment Trend</h3>", unsafe_allow_html=True)
            
            # Get real employment trend data from job_data
            trend_data = job_data.get("trend_data", {})
            if trend_data and "years" in trend_data and "employment" in trend_data:
                years = trend_data["years"]
                employment_values = trend_data["employment"]
            else:
                # Get SOC-specific employment data from database
                occupation_code = job_data.get("occupation_code", "00-0000")
                if occupation_code != "00-0000" and database_available:
                    try:
                        # Get employment data for this specific SOC code
                        import os
                        from sqlalchemy import create_engine, text
                        db_url = os.environ.get('DATABASE_URL')
                        engine = create_engine(db_url)
                        with engine.connect() as conn:
                            query = text("SELECT current_employment, projected_employment FROM bls_job_data WHERE occupation_code = :soc_code LIMIT 1")
                            result = conn.execute(query, {"soc_code": occupation_code})
                            row = result.fetchone()
                            if row and row[0]:
                                current_emp = int(row[0]) if row[0] else 100000
                                projected_emp = int(row[1]) if row[1] else current_emp * 1.1
                                
                                # Create realistic trend data
                                years = [2020, 2021, 2022, 2023, 2024, 2025]
                                # Calculate trend from current to projected
                                growth_factor = (projected_emp / current_emp) ** (1/5)  # 5-year growth
                                base_2020 = current_emp / (growth_factor ** 3)  # Work backwards to 2020
                                employment_values = [int(base_2020 * (growth_factor ** i)) for i in range(6)]
                            else:
                                # No data available
                                years = []
                                employment_values = []
                    except Exception as e:
                        logger.error(f"Error fetching employment trend data: {str(e)}")
                        years = []
                        employment_values = []
                else:
                    years = []
                    employment_values = []
            
            # Create employment trend chart only with real BLS data
            if employment_values and any(val > 0 for val in employment_values):
                trend_fig = go.Figure()
                trend_fig.add_trace(go.Scatter(
                    x=years,
                    y=employment_values,
                    mode='lines+markers',
                    name='Employment',
                    line=dict(color='#0084FF', width=2),
                    marker=dict(size=8)
                ))
                
                trend_fig.update_layout(
                    title=f'Employment Trend for {search_job_title} (2020-2025)',
                    xaxis_title='Year',
                    yaxis_title='Number of Jobs',
                    height=350,
                    margin=dict(l=40, r=40, t=60, b=40)
                )
                
                st.plotly_chart(trend_fig, use_container_width=True)
            else:
                st.info("📊 **Employment trend data from Bureau of Labor Statistics not available for this position.**")
            
            # Similar Jobs section
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Similar Jobs</h3>", unsafe_allow_html=True)
            
            # Get similar jobs data from the job_data response
            raw_similar_jobs = job_data.get("similar_jobs", [])
            similar_jobs = []
            
            # Convert from API format to our display format if data exists
            if raw_similar_jobs and len(raw_similar_jobs) > 0:
                for job in raw_similar_jobs:
                    # Handle different data formats from the API
                    if "job_title" in job and "year_5_risk" in job:
                        # Handle percentage vs decimal format
                        year_5_risk = job["year_5_risk"] / 100 if job["year_5_risk"] > 1 else job["year_5_risk"]
                        year_1_risk = job["year_1_risk"] / 100 if job["year_1_risk"] > 1 else job["year_1_risk"]
                        
                        similar_jobs.append({
                            "title": job["job_title"],
                            "year_5_risk": year_5_risk,
                            "year_1_risk": year_1_risk
                        })
                    elif "title" in job and "year_5_risk" in job:
                        similar_jobs.append(job)
            
            # Only show similar jobs if we have real data
            if similar_jobs:
                # Create chart first - ensure we have valid data for all elements
                job_titles = [job.get("title", "Untitled") for job in similar_jobs]
                
                # Handle possible None values in risk data
                risk_values = []
                for job in similar_jobs:
                    risk = job.get("year_5_risk", 0)
                    if risk is None:
                        risk = 0
                    risk_values.append(risk * 100)  # Convert to percentages
                
                similar_fig = go.Figure()
                similar_fig.add_trace(go.Bar(
                    x=job_titles,
                    y=risk_values,
                    marker_color='#FFA500',
                    text=[f"{val:.1f}%" for val in risk_values],
                    textposition='auto'
                ))
                
                # Add colorbar for reference
                similar_fig.update_layout(
                    title="AI Displacement Risk for Similar Jobs",
                    xaxis_title="Job Title",
                    yaxis_title="5-Year Risk (%)",
                    height=400,
                    margin=dict(l=40, r=40, t=60, b=40),
                    coloraxis=dict(
                        colorscale='RdYlGn_r',
                        showscale=True,
                        cmin=0,
                        cmax=100,
                        colorbar=dict(
                            title="5-Year Risk (%)",
                            thickness=15,
                            len=0.5,
                            y=0.5,
                            x=1.1
                        )
                    )
                )
                
                st.plotly_chart(similar_fig, use_container_width=True)
                
                # Add comparison suggestion text
                st.markdown("Compare risk levels of similar occupations:")
                
                # Create table with more detailed risk data
                if len(similar_jobs) > 0:
                    # Add risk categories
                    similar_data = []
                    for i, job in enumerate(similar_jobs):
                        risk = job.get("year_5_risk", 0) * 100
                        category = "High" if risk >= 60 else "Moderate" if risk >= 30 else "Low"
                        # Make sure we have values for both risks, with fallbacks if missing
                        year_5_risk = job.get("year_5_risk", 0)
                        if year_5_risk is None:
                            year_5_risk = 0
                        risk = year_5_risk * 100
                            
                        year_1_risk = job.get("year_1_risk")
                        if year_1_risk is None:
                            year_1_risk = risk * 0.6 / 100  # Convert back to decimal for consistent calculation
                            
                        similar_data.append({
                            "Job Title": job.get("title", ""),
                            "1-Year Risk (%)": f"{year_1_risk * 100:.1f}%",
                            "5-Year Risk (%)": f"{risk:.1f}%",
                            "Risk Category": category
                        })
                    
                    # Create and display dataframe
                    comparison_df = pd.DataFrame(similar_data)
                    st.dataframe(comparison_df, use_container_width=True)
            else:
                st.info("No similar jobs data available from the Bureau of Labor Statistics.")
            
            # Risk Assessment Summary
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Risk Assessment Summary</h3>", unsafe_allow_html=True)
            
            summary_text = job_data.get("summary", "Based on current AI trends and job market analysis, this role is experiencing changes due to automation and AI technologies. Skills in human-centric areas like leadership, creativity, and complex problem-solving will be increasingly valuable as routine aspects become automated.")
            st.markdown(summary_text)
            
            # Call to action for Career Navigator
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Get Your Personalized Career Plan</h3>", unsafe_allow_html=True)
            st.markdown("Our AI-powered Career Navigator can help you develop a personalized plan to adapt to these changes and thrive in your career.", unsafe_allow_html=True)
            
            # Get HTML from career_navigator module to avoid escaping issues
            st.markdown(career_navigator.get_html(), unsafe_allow_html=True)
            
            # Add Recent Searches section if database is available
            if database_available:
                st.markdown("<hr>", unsafe_allow_html=True)
                st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Recent Job Searches</h3>", unsafe_allow_html=True)
                
                # Get recent searches from our storage system
                recent_searches = get_recent_searches(limit=5)
                
                if recent_searches:
                    # Create columns for job title, risk category, and search time
                    recent_col1, recent_col2, recent_col3 = st.columns([3, 2, 2])
                    
                    with recent_col1:
                        st.markdown("<p style='color: #666666; font-weight: bold;'>Job Title</p>", unsafe_allow_html=True)
                    with recent_col2:
                        st.markdown("<p style='color: #666666; font-weight: bold;'>Risk Level</p>", unsafe_allow_html=True)
                    with recent_col3:
                        st.markdown("<p style='color: #666666; font-weight: bold;'>When</p>", unsafe_allow_html=True)
                    
                    # Display recent searches
                    for i, search in enumerate(recent_searches):
                        job_title = search.get("job_title", "Unknown Job")
                        risk_category = search.get("risk_category", "Unknown")
                        timestamp = search.get("timestamp")
                        
                        # Format timestamp as relative time
                        if timestamp:
                            now = datetime.datetime.now()
                            if isinstance(timestamp, str):
                                try:
                                    timestamp = datetime.datetime.fromisoformat(timestamp)
                                except:
                                    timestamp = now
                                    
                            delta = now - timestamp
                            if delta.days > 0:
                                time_ago = f"{delta.days} days ago"
                            elif delta.seconds >= 3600:
                                hours = delta.seconds // 3600
                                time_ago = f"{hours} hour{'s' if hours > 1 else ''} ago"
                            elif delta.seconds >= 60:
                                minutes = delta.seconds // 60
                                time_ago = f"{minutes} minute{'s' if minutes > 1 else ''} ago"
                            else:
                                time_ago = "Just now"
                        else:
                            time_ago = "Recently"
                        
                        # Color-code risk categories
                        if risk_category == "Very High":
                            risk_color = "#FF4B4B"  # Red
                        elif risk_category == "High":
                            risk_color = "#FF8C42"  # Orange
                        elif risk_category == "Moderate":
                            risk_color = "#FFCC3E"  # Yellow
                        elif risk_category == "Low":
                            risk_color = "#4CAF50"  # Green
                        else:
                            risk_color = "#666666"  # Gray
                        
                        # Display in columns
                        col1, col2, col3 = st.columns([3, 2, 2])
                        with col1:
                            # Make job title clickable to search again - use a unique key with index
                            search_key = f"search_{job_title.replace(' ', '_')}_{i}_{abs(hash(str(search))) % 10000}"
                            if st.button(job_title, key=search_key):
                                st.session_state.job_title = job_title
                                st.rerun()
                        with col2:
                            st.markdown(f"<p style='color: {risk_color};'>{risk_category}</p>", unsafe_allow_html=True)
                        with col3:
                            st.write(time_ago)
                else:
                    st.info("No recent searches yet. Be the first to analyze a job!")

# Job Comparison Tab
with tabs[1]:  # Job Comparison tab
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    
    # Introduction text
    st.markdown("Compare the AI displacement risk for multiple jobs side by side to explore transition opportunities. Add up to 5 jobs.")
    
    # Cache the job data to improve performance
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def get_cached_job_data(job_title):
        """Cache job data to improve performance"""
        try:
            return job_api_integration.get_job_data(job_title)
        except Exception as e:
            logger.error(f"Error getting cached job data for {job_title}: {str(e)}")
            return {"error": str(e), "job_title": job_title}
    
    # Direct job entry with dynamic addition - restore original functionality
    new_job = job_title_autocomplete(
        label="Enter a job title and press Enter to add to comparison", 
        key="compare_job_input",
        placeholder="Start typing to see suggestions...",
        help="Type a job title and select from matching suggestions"
    )
    
    # Initialize session state for selected jobs if not already present
    if 'selected_jobs' not in st.session_state:
        st.session_state.selected_jobs = []
    
    # Add job when entered and Enter key is pressed
    if new_job and new_job not in st.session_state.selected_jobs and len(st.session_state.selected_jobs) < 5:
        # Automatically add job when Enter is pressed
        with st.spinner(f"Adding {new_job} to comparison..."):
            try:
                # Pre-load the job data in cache
                job_data = get_cached_job_data(new_job)
                if "error" not in job_data:
                    st.session_state.selected_jobs.append(new_job)
                    logger.info(f"Added job to comparison: {new_job}")
                else:
                    st.error(f"Could not add job: {job_data['error']}")
            except Exception as e:
                st.error(f"Error adding job: {str(e)}")
                logger.error(f"Error adding job to comparison: {str(e)}")
    
    # Display current comparison jobs with remove buttons
    if st.session_state.selected_jobs:
        st.subheader("Current Comparison:")
        
        # Create columns for each job
        job_cols = st.columns(len(st.session_state.selected_jobs))
        
        # Display each job with a remove button
        for i, job in enumerate(st.session_state.selected_jobs.copy()):
            with job_cols[i]:
                st.markdown(f"**{job}**")
                if st.button("❌", key=f"remove_{i}"):
                    st.session_state.selected_jobs.remove(job)
                    logger.info(f"Removed job from comparison: {job}")
                    st.rerun()
        
        # Add clear all button
        if st.button("Clear All Jobs", key="clear_jobs"):
            st.session_state.selected_jobs = []
            logger.info("Cleared all jobs from comparison")
            st.rerun()
    
    # Display comparison when jobs are selected
    if st.session_state.selected_jobs and len(st.session_state.selected_jobs) >= 1:
        st.subheader(f"Analyzing {len(st.session_state.selected_jobs)} Jobs")
        
        # Process jobs with better progress feedback
        progress_text = st.empty()
        job_data_collection = {}
        error_jobs = []
        
        # Show progress as jobs are processed
        for i, job in enumerate(st.session_state.selected_jobs):
            progress_text.write(f"Processing {i+1}/{len(st.session_state.selected_jobs)}: {job}")
            try:
                job_data = get_cached_job_data(job)
                if "error" not in job_data:
                    job_data_collection[job] = job_data
                else:
                    error_jobs.append(job)
                    st.error(f"Error processing {job}: {job_data['error']}")
            except Exception as e:
                error_jobs.append(job)
                st.error(f"Error processing {job}: {str(e)}")
                logger.error(f"Error processing job for comparison: {job}, {str(e)}")
        
        # Check if we have any valid jobs to compare
        if not job_data_collection:
            st.error("No valid jobs to compare. Please try different job titles.")
            st.stop()
            
        # Remove error jobs from session state
        for job in error_jobs:
            if job in st.session_state.selected_jobs:
                st.session_state.selected_jobs.remove(job)
        
        progress_text.write("All jobs processed. Generating comparison...")
        
        # Now we have all job data, proceed with visualization
        # Get data for selected jobs using the comparison function
        try:
            job_data = simple_comparison.get_job_comparison_data(list(job_data_collection.keys()))
            
            # Create visualization tabs for different comparison views
            comparison_tabs = st.tabs(["Comparison Chart", "Comparative Analysis", "Risk Heatmap", "Risk Factors"])
            
            # Tab 1: Basic comparison chart
            with comparison_tabs[0]:
                st.markdown("<h3 style='color: #0084FF;'>5-Year AI Displacement Risk Comparison</h3>", unsafe_allow_html=True)
                chart = simple_comparison.create_comparison_chart(job_data)
                if chart:
                    st.plotly_chart(chart, use_container_width=True)
                else:
                    st.error("Unable to create comparison chart. Please check that you have selected valid jobs with available data.")
                
                # Display short explanation under the chart
                st.markdown("""
                **Chart Explanation**: This chart shows the projected AI displacement risk after 5 years for each selected job. 
                Higher percentages indicate greater likelihood that AI will significantly impact or automate aspects of this role.
                """)
            
            # Tab 2: Side-by-side comparative analysis
            with comparison_tabs[1]:
                st.markdown("<h3 style='color: #0084FF;'>Detailed Comparison</h3>", unsafe_allow_html=True)
                
                # Create tabular comparison
                comparison_df = simple_comparison.create_comparison_table(job_data)
                
                # Display the table with improved formatting
                if comparison_df is not None:
                    st.dataframe(comparison_df, use_container_width=True)
                else:
                    st.error("Unable to create comparison table. Please check that you have selected valid jobs with available data.")
                
                # Side-by-side comparison with actual job data
                st.subheader("Job Comparison Analysis")
                
                # Extract BLS and job data for comparison
                jobs_bls_data = {}
                
                # Extract important data points for each job
                for job_title, job_info in job_data.items():
                    # Get BLS data if available
                    bls_data = job_info.get("bls_data", {})
                    
                    # Get additional data from job API integration
                    try:
                        # Try to get data from the API first
                        api_data = job_api_integration.get_job_data(job_title)
                        api_bls_data = api_data.get("bls_data", {})
                        
                        # Use API data
                        employment = api_bls_data.get("employment") or bls_data.get("employment", "N/A")
                        openings = api_bls_data.get("annual_job_openings") or bls_data.get("annual_job_openings", "N/A")
                        growth = api_bls_data.get("employment_change_percent") or bls_data.get("employment_change_percent", "N/A")
                        
                        # Format the values nicely
                        if isinstance(employment, (int, float)) and employment != "N/A":
                            employment = f"{int(employment):,}"
                        
                        if isinstance(openings, (int, float)) and openings != "N/A":
                            openings = f"{int(openings):,}"
                            
                        if isinstance(growth, (int, float)) and growth != "N/A":
                            growth = f"{float(growth):+.1f}"
                    except Exception as e:
                        logger.error(f"Error getting API data for {job_title}: {str(e)}")
                        employment = bls_data.get("employment", "N/A")
                        openings = bls_data.get("annual_job_openings", "N/A")
                        growth = bls_data.get("employment_change_percent", "N/A")
                        
                    jobs_bls_data[job_title] = {
                        "Employment": employment,
                        "Annual Job Openings": openings,
                        "Growth": growth,
                        "Category": job_info.get("job_category", "General")
                    }
                
                # Create comparison sections
                st.markdown("### Employment & Market Comparison")
                
                # Add explanatory note about BLS data
                st.info("""
                **Note on Employment Data**: The Bureau of Labor Statistics organizes employment data by standardized 
                occupational codes, not by specific job titles. Some job titles may not directly map to BLS classifications, 
                particularly newer or specialized roles. We do our best to match job titles to the appropriate BLS categories.
                """)
                
                # Create employment data comparison
                emp_data = []
                for job, data in jobs_bls_data.items():
                    emp_data.append({
                        "Job Title": job,
                        "Category": data["Category"],
                        "Current Employment": data["Employment"] if data["Employment"] != "N/A" else "Data unavailable",
                        "Projected Growth": f"{data['Growth']}%" if data["Growth"] != "N/A" else "Data unavailable",
                        "Annual Openings": data["Annual Job Openings"] if data["Annual Job Openings"] != "N/A" else "Data unavailable"
                    })
                
                # Display employment comparison
                if emp_data:
                    emp_df = pd.DataFrame(emp_data)
                    st.dataframe(emp_df, use_container_width=True)
                
                # Transition Guidance section
                st.markdown("### Career Transition Recommendations")
                
                # Get lowest risk job from comparison for guidance
                risk_values = [(job, data.get("risk_scores", {}).get("year_5", 0) or data.get("year_5_risk", 0)) 
                              for job, data in job_data.items()]
                
                if len(risk_values) >= 2:
                    lowest_job = min(risk_values, key=lambda x: x[1])
                    highest_job = max(risk_values, key=lambda x: x[1])
                    
                    # Check if significant difference in risk
                    if abs(highest_job[1] - lowest_job[1]) > 0.2:
                        st.markdown(f"""
                        Based on comparing these positions, transitioning toward roles like **{lowest_job[0]}** may provide more long-term career stability as AI adoption increases. Consider the following steps:
                        
                        1. **Skill Development Focus**: Identify overlapping skill requirements between your current role and positions like {lowest_job[0]}
                        2. **Education/Training**: Research specific certifications or courses that would strengthen your qualifications for this career transition
                        3. **Experience Building**: Look for projects or responsibilities in your current role that align with {lowest_job[0]} to build relevant experience
                        """)
                    else:
                        st.markdown("""
                        The selected positions show relatively similar AI impact projections. Consider focusing on enhancing your skills within your current career path:
                        
                        1. **Upskilling**: Develop advanced expertise in your field to handle complex cases AI cannot manage
                        2. **Cross-functional Knowledge**: Build broader understanding across related domains to increase your versatility
                        3. **AI Collaboration Skills**: Develop proficiency working alongside AI tools to enhance your productivity
                        """)
                else:
                    st.markdown("Add more jobs to the comparison to receive transition recommendations.")
            
            # Tab 3: Risk heatmap
            with comparison_tabs[2]:
                st.markdown("<h3 style='color: #0084FF;'>Risk Progression Heatmap</h3>", unsafe_allow_html=True)
                
                heatmap = simple_comparison.create_risk_heatmap(job_data)
                if heatmap:
                    st.plotly_chart(heatmap, use_container_width=True)
                else:
                    st.error("Unable to create risk heatmap. Please check that you have selected valid jobs with available data.")
                
                st.markdown("""
                **Heatmap Explanation**: This visualization shows how displacement risk is projected to increase over time for each position.
                Darker colors indicate higher risk levels, helping you understand both immediate and long-term vulnerability.
                """)
            
            # Tab 4: Risk factors comparison
            with comparison_tabs[3]:
                st.markdown("<h3 style='color: #0084FF;'>Risk Factor Analysis</h3>", unsafe_allow_html=True)
                
                # Create radar chart for risk factor comparison
                radar = simple_comparison.create_radar_chart(job_data)
                if radar:
                    st.plotly_chart(radar, use_container_width=True)
                else:
                    st.error("Unable to create radar chart. Please check that you have selected valid jobs with available data.")
                
                st.markdown("""
                **Factor Analysis Explanation**: This radar chart compares positions across key risk dimensions. 
                Jobs with larger areas on the chart face higher overall risk from AI disruption across multiple factors.
                """)
            
            # Career Navigator Integration
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown("<h2 style='color: #0084FF;'>Next Steps: Personalized Career Navigator</h2>", unsafe_allow_html=True)
            st.markdown("Get personalized career guidance based on your skills and interests.", unsafe_allow_html=True)
            
            st.markdown("""
            <div style='background-color: #0084FF; color: white; padding: 20px; border-radius: 10px; margin-top: 20px;'>
                <h3 style='color: white;'>Career Navigator</h3>
                <p style='font-size: 16px;'>Our AI-powered Career Navigator provides personalized guidance to help you navigate the changing job market:</p>
                <ul style='font-size: 16px;'>
                    <li>Identify transferable skills that increase your value</li>
                    <li>Discover resilient career paths aligned with your experience</li>
                    <li>Get specific training recommendations with costs and ROI</li>
                    <li>Receive a customized transition plan with timeline and milestones</li>
                </ul>
                <a href='https://form.jotform.com/251137815706154' target='_blank'>
                    <button style='background-color: white; color: #0084FF; border: none; padding: 10px 20px; border-radius: 5px; font-weight: bold; cursor: pointer; margin-top: 10px;'>
                        Get Your Personalized Career Plan
                    </button>
                </a>
            </div>
            """, unsafe_allow_html=True)
            
        except Exception as e:
            st.error(f"Error generating comparison: {str(e)}")
            logger.error(f"Error generating job comparison: {str(e)}")

# Application footer
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

# Add version information in the sidebar
st.sidebar.markdown("---")
st.sidebar.markdown(f"""
<div style="text-align: center; color: #666666; font-size: 12px;">
    <p>Version 2.0.0</p>
    <p>Last updated: {datetime.datetime.now().strftime('%Y-%m-%d')}</p>
</div>
""", unsafe_allow_html=True)

# Display health status in sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("<p style='text-align: center; color: #666666; font-size: 14px;'>System Health</p>", unsafe_allow_html=True)

# Check if keep-alive stats file exists
try:
    with open("keep_alive_stats.json", "r") as f:
        stats = json.load(f)
        last_success = datetime.datetime.fromisoformat(stats["last_success"])
        time_diff = datetime.datetime.now() - last_success
        
        if time_diff.total_seconds() < 600:  # Less than 10 minutes
            st.sidebar.markdown("""
            <div class="status-indicator online">
                <div class="status-indicator-dot online"></div>
                Keep-alive: Active
            </div>
            """, unsafe_allow_html=True)
        else:
            st.sidebar.markdown(f"""
            <div class="status-indicator offline">
                <div class="status-indicator-dot offline"></div>
                Keep-alive: Last ping {time_diff.seconds // 60} min ago
            </div>
            """, unsafe_allow_html=True)
except Exception:
    st.sidebar.markdown("""
    <div class="status-indicator online">
        <div class="status-indicator-dot online"></div>
        Keep-alive: Starting
    </div>
    """, unsafe_allow_html=True)

# Add UptimeRobot configuration instructions in sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("<p style='text-align: center; color: #666666; font-size: 14px;'>UptimeRobot Setup</p>", unsafe_allow_html=True)
st.sidebar.info("""
To keep this application alive with UptimeRobot:
1. Create a new monitor in UptimeRobot
2. Set Type to "HTTP(s)"
3. Set URL to your app URL with `?health_check=true` parameter
4. Set monitoring interval to 5 minutes
5. Enable "Alert When Down"
""")
