import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
import datetime
import os
import sys
import threading
import time
import re
import career_navigator
from sqlalchemy import create_engine, text

# Import updated modules that will handle DB/API logic
import job_api_integration_v2 as job_api_integration
import simple_comparison_v2 as simple_comparison
from job_title_autocomplete_v2 import job_title_autocomplete, load_job_titles_from_db

# --- System Status Check ---
@st.cache_data(ttl=300)  # Cache status for 5 minutes
def check_system_status():
    """Checks the status of the database and BLS API."""
    db_status = "Error"
    db_error = "Not configured"
    api_status = "Not Configured"

    # Check DB connection
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        try:
            engine = create_engine(database_url, connect_args={"connect_timeout": 5})
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            db_status = "Connected"
            db_error = None
        except Exception as e:
            db_error = str(e)
    
    # Check BLS API key and connectivity
    if os.environ.get('BLS_API_KEY'):
        try:
            from bls_connector import check_api_connectivity
            if check_api_connectivity():
                api_status = "Accessible"
            else:
                api_status = "Not Accessible"
        except Exception as e:
            api_status = f"Error: {e}"
            
    return {"db_status": db_status, "api_status": api_status, "db_error": db_error}

# Run system status check once per session
if 'system_status' not in st.session_state:
    st.session_state.system_status = check_system_status()
system_status = st.session_state.system_status

# --- Health Check Endpoint ---
query_params = st.query_params
if query_params.get("health_check") == "true":
    st.title("iThriveAI Job Analyzer - Health Check")
    st.success("✅ Application status: Running")
    
    if system_status["db_status"] == "Connected":
        st.success("✅ Database connection: OK")
    else:
        st.error(f"❌ Database connection: {system_status['db_status']} - {system_status['db_error']}")
        
    if system_status["api_status"] == "Accessible":
        st.success("✅ BLS API: Accessible")
    else:
        st.error(f"❌ BLS API: {system_status['api_status']}")
        
    st.info("This is the detailed health check endpoint for monitoring.")
    st.stop()

# --- Page Config and CSS ---
st.set_page_config(
    page_title="Career AI Impact Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { background-color: #FFFFFF; }
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        height: 60px; width: 250px; white-space: pre-wrap; background-color: #F0F8FF;
        border-radius: 4px 4px 0 0; gap: 10px; padding-top: 15px; padding-bottom: 15px;
        font-size: 18px; font-weight: 600; text-align: center;
    }
    .stTabs [aria-selected="true"] { background-color: #0084FF; color: white; }
    h1, h2, h3, h4, h5, h6 { color: #0084FF; }
</style>
""", unsafe_allow_html=True)

# --- Sidebar ---
with st.sidebar:
    st.header("System Status")
    
    if system_status["db_status"] == "Connected":
        st.success(f"Database: {system_status['db_status']}")
    else:
        st.error(f"Database: {system_status['db_status']}")

    if system_status["api_status"] == "Accessible":
        st.success(f"BLS API: {system_status['api_status']}")
    else:
        st.warning(f"BLS API: {system_status['api_status']}")

    st.markdown("---")
    st.info(f"App Version: 2.2.0 (DB/API Fallback)")
    st.info(f"Last Load: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    st.markdown("---")
    # Admin Controls moved to sidebar to prevent layout conflicts
    with st.expander("⚙️ ADMIN CONTROLS", expanded=False):
        admin_password = st.text_input("Enter Admin Password", type="password", key="admin_pass")
        if admin_password == "admin123":
            st.success("Admin Access Granted")
            st.subheader("Admin Dashboard: BLS Data Management")
            
            # Placeholder for admin dashboard components
            st.write("Dashboard and population tools would be here.")
            st.slider("Batch Size (SOCs per run)", 1, 10, 5, key="batch_size")
            if st.button("▶️ Start/Resume Batch"):
                st.toast("Batch processing started...")

# --- Main App Body ---
st.image("https://img1.wsimg.com/isteam/ip/70686f32-22d2-489c-a383-6fcd793644be/blob-3712e2e.png/:/rs=h:197,cg:true,m/qt=q:95", width=250)
st.markdown("<h1 style='text-align: center; color: #0084FF;'>Is your job at risk with AI innovation?</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #4CACE5; font-size: 24px; font-weight: 600;'>AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666666; font-weight: bold; font-size: 16px;'>Discover how AI might impact your career in the next 5 years and get personalized recommendations.</p>", unsafe_allow_html=True)

tabs = st.tabs(["Single Job Analysis", "Job Comparison"])

with tabs[0]:
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    if system_status['api_status'] == "Accessible":
        st.info("📊 Using real-time data from the Bureau of Labor Statistics API")
    else:
        st.warning("📊 Using cached data. BLS API is not accessible.")

    search_job_title = job_title_autocomplete(
        label="Enter any job title to analyze",
        key="job_title_search",
        placeholder="Start typing to see suggestions...",
        help="Type a job title and select from matching suggestions"
    )
    
    if st.button("🗑️ Clear Entry", key="clear_button_single"):
        st.rerun()
    
    if st.button("Analyze Job Risk"):
        if search_job_title:
            with st.spinner(f"Analyzing {search_job_title}..."):
                try:
                    # This function now handles DB lookup and API fallback
                    job_data = job_api_integration.get_job_data(search_job_title)
                    
                    if "error" in job_data:
                        st.error(f"Could not retrieve data for '{search_job_title}'. Reason: {job_data['error']}")
                        st.info("Please try a different job title. If the issue persists, an administrator may need to add this job to the database.")
                        st.stop()
                except Exception as e:
                    st.error(f"An unexpected error occurred: {str(e)}")
                    st.stop()

                # Display results
                st.subheader(f"AI Displacement Risk Analysis: {job_data.get('job_title', search_job_title)}")
                
                job_info_col, risk_gauge_col, risk_factors_col = st.columns([1, 1, 1])
                
                with job_info_col:
                    st.markdown("<h3 style='font-size: 20px;'>Job Information</h3>", unsafe_allow_html=True)
                    st.markdown(f"**Occupation Code:** {job_data.get('occupation_code', 'N/A')}")
                    st.markdown(f"**Job Category:** {job_data.get('job_category', 'General')}")
                    
                    bls_data = job_data.get("projections", {})
                    if bls_data.get("current_employment"):
                        st.markdown(f"**Current Employment:** {bls_data['current_employment']:,.0f} jobs")
                    if bls_data.get("percent_change") is not None:
                        st.markdown(f"**BLS Projected Growth:** {bls_data['percent_change']:+.1f}%")

                with risk_gauge_col:
                    risk_category = job_data.get("risk_category", "High")
                    year_5_risk = job_data.get("year_5_risk", 60.0)
                    
                    st.markdown(f"<h3 style='text-align: center; margin-bottom: 10px;'>Overall AI Risk: {risk_category}</h3>", unsafe_allow_html=True)
                    
                    fig = go.Figure(go.Indicator(
                        mode="gauge+number", value=year_5_risk,
                        gauge={'axis': {'range': [0, 100]}, 'bar': {'color': "#0084FF"}}
                    ))
                    fig.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=20))
                    st.plotly_chart(fig, use_container_width=True)

                with risk_factors_col:
                    st.markdown("<h3 style='font-size: 20px;'>Key Risk Factors</h3>", unsafe_allow_html=True)
                    for factor in job_data.get("risk_factors", []):
                        st.markdown(f"❌ {factor}")
                    
                    st.markdown("<h3 style='font-size: 20px; margin-top: 20px;'>Protective Factors</h3>", unsafe_allow_html=True)
                    for factor in job_data.get("protective_factors", []):
                        st.markdown(f"✅ {factor}")

                st.markdown("<h3 style='font-size: 20px; margin-top: 20px;'>Key Insights</h3>", unsafe_allow_html=True)
                st.markdown(job_data.get("analysis", "No analysis available."))
                
                st.markdown("<hr>")
                st.markdown(career_navigator.get_html(), unsafe_allow_html=True)
        else:
            st.warning("Please enter a job title to analyze.")

with tabs[1]:
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    st.markdown("Compare the AI displacement risk for multiple jobs side by side.")
    
    if 'selected_jobs' not in st.session_state:
        st.session_state.selected_jobs = []
    
    new_job = job_title_autocomplete(
        label="Enter a job title to add to comparison",
        key="compare_job_input",
        placeholder="Start typing...",
        help="Add up to 5 jobs for comparison."
    )
    
    if new_job and new_job not in st.session_state.selected_jobs and len(st.session_state.selected_jobs) < 5:
        st.session_state.selected_jobs.append(new_job)
        st.rerun()

    if st.session_state.selected_jobs:
        st.subheader("Jobs to Compare:")
        job_cols = st.columns(len(st.session_state.selected_jobs))
        for i, job in enumerate(st.session_state.selected_jobs):
            with job_cols[i]:
                st.markdown(f"**{job}**")
                if st.button("❌", key=f"remove_{i}"):
                    st.session_state.selected_jobs.pop(i)
                    st.rerun()
        
        if st.button("Clear All Jobs"):
            st.session_state.selected_jobs = []
            st.rerun()
    
    if len(st.session_state.selected_jobs) > 1:
        with st.spinner("Fetching comparison data..."):
            comparison_data = simple_comparison.get_job_comparison_data(st.session_state.selected_jobs)
            
            st.markdown("<h3 style='color: #0084FF;'>5-Year AI Displacement Risk</h3>", unsafe_allow_html=True)
            chart = simple_comparison.create_comparison_chart(comparison_data)
            if chart:
                st.plotly_chart(chart, use_container_width=True)
            
            st.markdown("<h3 style='color: #0084FF;'>Detailed Comparison</h3>", unsafe_allow_html=True)
            table = simple_comparison.create_comparison_table(comparison_data)
            if table is not None:
                st.dataframe(table, use_container_width=True)
