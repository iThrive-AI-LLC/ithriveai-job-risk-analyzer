import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
import datetime
import os
import sys
import threading

# Keep-alive functionality to prevent app sleeping
def keep_alive():
    """Background thread to keep the app active"""
    import time
    while True:
        time.sleep(300)  # Ping every 5 minutes
        try:
            # Simple database query to keep connection alive
            from database import get_recent_searches
            get_recent_searches(limit=1)
        except:
            pass

# Start keep-alive thread
if "keep_alive_started" not in st.session_state:
    st.session_state.keep_alive_started = True
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
# Force reload: Court Reporter special case added
from job_api_integration_database_only import get_job_data
import simple_comparison
import ai_job_displacement
import time
import re
import career_navigator
from sqlalchemy import create_engine, text

# Import the autocomplete functionality
from job_title_autocomplete_v2 import job_title_autocomplete, load_job_titles_from_db

# Check if BLS API key is set
bls_api_key = os.environ.get('BLS_API_KEY')

# Handle health check requests
query_params = st.query_params
if query_params.get("health_check") == "true":
    st.text("OK")
    st.stop()

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
</style>
""", unsafe_allow_html=True)

# Very simple health check endpoint for reliable monitoring
if 'health' in st.query_params:
    st.write("OK")  # Just return a simple OK response
    st.stop()

# Detailed health check endpoint for troubleshooting
if 'health_check' in st.query_params:
    st.title("iThriveAI Job Analyzer - Health Check")
    
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
            st.warning("⚠️ Database connection: Not configured")
    except Exception as e:
        st.warning("⚠️ Database connection: Using fallback data")
        st.info("ℹ️ The application is running in fallback mode with built-in sample data")
    
    # Check BLS API key
    if bls_api_key:
        st.success("✅ BLS API key: Available")
    else:
        st.warning("⚠️ BLS API key: Not configured")
        
    st.info("ℹ️ This endpoint is used for application monitoring")
    st.stop()  # Stop further execution

# Application title and description
st.image("https://img1.wsimg.com/isteam/ip/70686f32-22d2-489c-a383-6fcd793644be/blob-3712e2e.png/:/rs=h:197,cg:true,m/qt=q:95", width=250)
st.markdown("<h1 style='text-align: center; color: #0084FF;'>Is your job at risk with AI innovation?</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #4CACE5; font-size: 24px; font-weight: 600;'>AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #666666; font-weight: bold; font-size: 16px;'>Discover how AI might impact your career in the next 5 years and get personalized recommendations.</p>", unsafe_allow_html=True)

# Database connection setup (with fallback to in-memory data if not available)
try:
    from database import save_job_search, get_popular_searches, get_highest_risk_jobs, get_lowest_risk_jobs, get_recent_searches
    database_available = True
except:
    from db_fallback import save_job_search, get_popular_searches, get_highest_risk_jobs, get_lowest_risk_jobs, get_recent_searches
    database_available = False

def check_data_refresh():
    """Check if data needs to be refreshed (daily schedule to keep Supabase active)"""
    try:
        with open("last_refresh.json", "r") as f:
            refresh_data = json.load(f)
            last_refresh = datetime.datetime.fromisoformat(refresh_data["date"])
            
            # Refresh if more than a day has passed
            days_since_refresh = (datetime.datetime.now() - last_refresh).days
            
            if days_since_refresh >= 1:
                # Run the daily refresh to keep database active
                try:
                    import db_refresh
                    st.info("Refreshing BLS data and performing database activity...")
                    # Run a sample job update to keep database active
                    sample_job = "Software Developer"
                    db_refresh.update_job_data(sample_job)
                    db_refresh.perform_database_queries()
                    db_refresh.check_and_update_refresh_timestamp()
                    st.success(f"Database activity performed successfully. Updated {sample_job} data.")
                except Exception as e:
                    st.warning(f"Database refresh attempted but encountered an issue: {str(e)}")
                return True
            return False
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        # If file doesn't exist or is invalid, trigger refresh
        with open("last_refresh.json", "w") as f:
            json.dump({"date": datetime.datetime.now().isoformat()}, f)
        return True

# Tabs for different sections - use original tab names from screenshots
tabs = st.tabs(["Single Job Analysis", "Job Comparison"])

# Single Job Analysis Tab - Matching original layout
with tabs[0]:  # Single Job Analysis tab
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    
    # Display API source information
    if bls_api_key:
        st.info("📊 Using real-time data from the Bureau of Labor Statistics API")
    
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
    
    # Only search when button is clicked and there's a job title
    # Check for data refresh when the app starts
    check_data_refresh()
    
    if search_clicked and search_job_title:
        # Show loading spinner during API calls and data processing
        with st.spinner(f"Analyzing {search_job_title}..."):
            try:
                # Get job data with optimized API calls
                job_data = get_job_data(search_job_title)

                # ------------------------------------------------------------------
                # Derive missing job category if mapper returned the generic "General"
                # but we do have a valid SOC code.  This improves titles such as
                # “Teacher” / “Kindergarten Teachers, Except Special Education”.
                # ------------------------------------------------------------------
                from bls_job_mapper import get_job_category  # local import to avoid circulars

                if (
                    isinstance(job_data, dict)
                    and job_data.get("job_category", "General") == "General"
                    and job_data.get("occupation_code")
                ):
                    derived_cat = get_job_category(job_data["occupation_code"])
                    if derived_cat:
                        job_data["job_category"] = derived_cat
            except Exception as e:
                # If job not found in database, show clear error message
                if "not found in BLS database" in str(e):
                    st.error(f"Job title '{search_job_title}' not found in our BLS database. Please use the Admin Dashboard to add missing job titles.")
                    st.info("Use the search suggestions or contact support to add this occupation with authentic BLS data.")
                    st.stop()
                else:
                    st.error(f"Database error: {str(e)}")
                    st.stop()
            
            # Save to database
            if database_available:
                save_job_search(search_job_title, {
                    'year_1_risk': job_data.get('risk_scores', {}).get('year_1', 0),
                    'year_5_risk': job_data.get('risk_scores', {}).get('year_5', 0),
                    'risk_category': job_data.get('risk_category', 'Unknown'),
                    'job_category': job_data.get('job_category', 'Unknown')
                })
            
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
                year_1_risk = job_data.get("risk_scores", {}).get("year_1", 35.0)
                year_5_risk = job_data.get("risk_scores", {}).get("year_5", 60.0)
                
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
                
                # Get risk factors from job data or provide job-specific defaults based on searched job title
                job_category = job_data.get('job_category', 'General').lower()
                
                if 'legal' in job_category or 'legal' in search_job_title.lower():
                    default_risk_factors = [
                        "AI legal research tools automate document review",
                        "Contract analysis software can identify key clauses",
                        "Legal document generation reduces routine paperwork",
                        "Basic legal research increasingly automated"
                    ]
                elif 'healthcare' in job_category or any(term in search_job_title.lower() for term in ['nurse', 'doctor', 'medical']):
                    default_risk_factors = [
                        "Diagnostic AI assists with pattern recognition",
                        "Administrative tasks increasingly automated",
                        "Electronic health records reduce manual documentation",
                        "Basic scheduling and routine tasks automated"
                    ]
                else:
                    default_risk_factors = [
                        "Routine administrative tasks increasingly automated",
                        "Basic data entry and processing can be automated",
                        "Standardized procedures require less human oversight",
                        "Simple decision-making processes automated"
                    ]
                
                # Set different default risk factors based on job category
                if "developer" in search_job_title.lower() or "programmer" in search_job_title.lower():
                    default_risk_factors = [
                        "Automated code generation reduces need for routine coding",
                        "AI tools can debug and optimize existing code",
                        "Low-code/no-code platforms replace basic development tasks",
                        "Standardized development work increasingly automated"
                    ]
                elif "analyst" in search_job_title.lower():
                    default_risk_factors = [
                        "AI tools automate data collection and cleaning",
                        "Automated report generation reduces manual work",
                        "Pattern recognition algorithms identify insights faster",
                        "Dashboard automation reduces need for routine analysis"
                    ]
                elif "designer" in search_job_title.lower():
                    default_risk_factors = [
                        "AI design tools can generate layouts and compositions",
                        "Style transfer algorithms automate visual consistency",
                        "Template-based design reduces need for custom work",
                        "Generative design tools create multiple options quickly"
                    ]
                    
                risk_factors = job_data.get("risk_factors", default_risk_factors)
                
                for factor in risk_factors:
                    st.markdown(f"❌ {factor}")
                
                # Protective Factors
                st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Protective Factors</h3>", unsafe_allow_html=True)
                
                # Set different default protective factors based on job category
                job_category = job_data.get('job_category', 'General').lower()
                
                if 'legal' in job_category or 'legal' in search_job_title.lower():
                    default_protective_factors = [
                        "Complex legal reasoning requires human judgment",
                        "Client relationships and advocacy need human empathy",
                        "Ethical decision-making in ambiguous situations",
                        "Courtroom presence and persuasion remain human skills"
                    ]
                elif 'healthcare' in job_category or any(term in search_job_title.lower() for term in ['nurse', 'doctor', 'medical']):
                    default_protective_factors = [
                        "Direct patient care requires human touch and empathy",
                        "Complex medical decision-making needs human judgment",
                        "Emergency situations require quick human adaptation",
                        "Emotional support and bedside manner remain human-centered"
                    ]
                else:
                    default_protective_factors = [
                        "Complex problem-solving requires human creativity",
                        "Interpersonal relationships and communication skills",
                        "Adaptability to unexpected situations",
                        "Strategic thinking and contextual understanding"
                    ]
                
                if "developer" in search_job_title.lower() or "programmer" in search_job_title.lower():
                    default_protective_factors = [
                        "Complex system architecture requires human judgment",
                        "User-centered design needs human empathy and creativity",
                        "Novel problem-solving is difficult to automate",
                        "Client collaboration and requirement gathering need human skills"
                    ]
                elif "analyst" in search_job_title.lower():
                    default_protective_factors = [
                        "Strategic insight requires business context and judgment",
                        "Complex problem definition needs human framing",
                        "Interpreting findings within broader context requires experience",
                        "Communicating insights to stakeholders needs human skills"
                    ]
                elif "designer" in search_job_title.lower():
                    default_protective_factors = [
                        "Creative direction and concept development need human creativity",
                        "Understanding emotional impact requires human empathy",
                        "Cultural context and sensitivity need human judgment",
                        "Client relationship management requires human connections"
                    ]
                
                protective_factors = job_data.get("protective_factors", default_protective_factors)
                
                for factor in protective_factors:
                    st.markdown(f"✅ {factor}")
            
            # Analysis section - full width
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Key Insights</h3>", unsafe_allow_html=True)
            
            # Create job-specific analysis text based on search term
            job_title_cleaned = search_job_title.strip()
            
            # Define a default analysis for any job
            default_analysis = f"{job_title_cleaned} faces changes due to advancing AI technologies. Roles requiring human judgment, creativity, and complex social interactions will remain most protected from automation. Professionals who develop skills in AI collaboration, strategic thinking, and specialized expertise will be best positioned for the changing job market."
            
            # Job-specific analysis templates
            job_analyses = {
                "project manager": f"Project Managers face moderate to high displacement risk as AI tools advance. While routine project tracking and documentation are increasingly automated, roles requiring complex stakeholder management, strategic thinking, and leadership will remain valuable. Project managers who develop skills in AI oversight, strategic leadership, and change management will be more resilient to automation.",
                
                "developer": f"Software Developers are experiencing significant transformation due to AI advancements in code generation and optimization. While routine coding tasks are increasingly automated, developers who specialize in complex architecture, novel problem-solving, and human-centered design will remain valuable. Focus on developing skills in AI integration, system architecture, and specialized domain knowledge to remain competitive.",
                
                "programmer": f"Programmers face significant transformation due to AI advancements in code generation and optimization. While routine coding tasks are increasingly automated, programmers who specialize in complex architecture, novel problem-solving, and human-centered design will remain valuable. Focus on developing skills in AI integration, system architecture, and specialized domain knowledge to remain competitive.",
                
                "analyst": f"Analysts are being transformed by automated data processing and insight generation tools. While data collection and basic analysis are increasingly automated, analysts who can define complex problems, provide strategic context to findings, and communicate effectively with stakeholders will remain essential. Developing skills in advanced analytics, business strategy, and AI-assisted analysis will enhance career resilience.",
                
                "designer": f"Designers are evolving as AI tools enhance creative workflows. While basic design tasks and template-based work face automation, designers who excel at concept development, emotional connection, and creative direction will continue to be valued. Focus on developing skills in design strategy, creative direction, and human experience design to stay ahead of automation trends.",
                
                "manager": f"Managers face moderate disruption as AI tools automate routine management tasks. While administrative aspects of management are increasingly handled by software, managers who excel at leadership, strategic thinking, and complex stakeholder relationships will remain essential. Developing skills in AI-enhanced decision making, change management, and strategic leadership will strengthen career resilience.",
                
                "teacher": f"Teachers face a changing landscape as AI tools automate content creation and basic assessment. However, the core aspects of teaching—mentorship, emotional support, individualized guidance, and inspiring curiosity—remain deeply human. Teachers who integrate AI tools while focusing on relationship-building and higher-order thinking skills will thrive in the evolving educational environment.",
                
                "nurse": f"Nurses remain relatively protected from AI displacement due to the high degree of human care, emotional intelligence, and complex decision-making required. While some diagnostic and administrative tasks may be automated, the hands-on patient care, clinical assessment, and compassionate support that nurses provide cannot be easily replicated by AI systems.",
                
                "doctor": f"Doctors face partial automation of routine diagnostic tasks, but the core aspects of medicine—complex reasoning, ethical judgment, and patient relationships—remain highly resistant to automation. Physicians who learn to effectively collaborate with AI diagnostic tools while focusing on complex cases and patient-centered care will be most successful in the changing healthcare landscape.",
            }
            
            # Check if the job title contains any of our predefined analyses
            selected_analysis = default_analysis
            for key, analysis in job_analyses.items():
                if key in job_title_cleaned.lower():
                    selected_analysis = analysis
                    break
            
            # Use provided analysis if available, otherwise use our job-specific default
            analysis_text = job_data.get("analysis", selected_analysis)
            st.markdown(analysis_text)
            
            # Employment Trend Chart
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Employment Trend</h3>", unsafe_allow_html=True)
            
            # Get real employment trend data from job_data
            trend_data = job_data.get("trend_data", {})
            if trend_data and "years" in trend_data and "employment" in trend_data:
                years = trend_data["years"]
                employment_values = trend_data["employment"]
            else:
                # Get SOC-specific employment data from database or API
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
                                # No fallback data - show message if no real BLS data
                                years = [2020, 2021, 2022, 2023, 2024, 2025]
                                employment_values = [0, 0, 0, 0, 0, 0]  # Will show as "Data not available"
                    except Exception as e:
                        # No fallback - only real BLS data
                        years = [2020, 2021, 2022, 2023, 2024, 2025]
                        employment_values = [0, 0, 0, 0, 0, 0]
                else:
                    # No fallback - only show real BLS data
                    years = [2020, 2021, 2022, 2023, 2024, 2025]
                    employment_values = [0, 0, 0, 0, 0, 0]
            
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
                st.info("📊 **Employment trend data from Bureau of Labor Statistics not yet available for this position.** Analysis shows current risk factors and projections based on job category research.")
            
            # Similar Jobs section
            st.markdown("<h3 style='color: #0084FF; font-size: 20px; margin-top: 20px;'>Similar Jobs</h3>", unsafe_allow_html=True)
            
            # Get similar jobs data from the job_data response
            # The API returns similar_jobs in a specific format that we need to adapt
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
            
            # Ensure similar jobs data is always available for comparison
            if not similar_jobs or len(similar_jobs) == 0:
                # Provide default similar jobs data if none exists
                if "project manager" in search_job_title.lower():
                    similar_jobs = [
                        {"title": "Program Manager", "year_5_risk": 0.55, "year_1_risk": 0.30},
                        {"title": "Product Manager", "year_5_risk": 0.45, "year_1_risk": 0.25},
                        {"title": "Construction Manager", "year_5_risk": 0.40, "year_1_risk": 0.20},
                        {"title": "Operations Manager", "year_5_risk": 0.65, "year_1_risk": 0.40}
                    ]
                elif "developer" in search_job_title.lower() or "programmer" in search_job_title.lower():
                    similar_jobs = [
                        {"title": "Frontend Developer", "year_5_risk": 0.60, "year_1_risk": 0.35},
                        {"title": "Backend Developer", "year_5_risk": 0.45, "year_1_risk": 0.25},
                        {"title": "DevOps Engineer", "year_5_risk": 0.40, "year_1_risk": 0.20},
                        {"title": "Software Architect", "year_5_risk": 0.35, "year_1_risk": 0.15}
                    ]
                elif "analyst" in search_job_title.lower():
                    similar_jobs = [
                        {"title": "Data Analyst", "year_5_risk": 0.65, "year_1_risk": 0.40},
                        {"title": "Business Analyst", "year_5_risk": 0.60, "year_1_risk": 0.35},
                        {"title": "Financial Analyst", "year_5_risk": 0.50, "year_1_risk": 0.30},
                        {"title": "Research Analyst", "year_5_risk": 0.45, "year_1_risk": 0.25}
                    ]
                else:
                    # Generic similar jobs for any other job title
                    similar_jobs = [
                        {"title": "Team Lead", "year_5_risk": 0.50, "year_1_risk": 0.25},
                        {"title": "Department Manager", "year_5_risk": 0.40, "year_1_risk": 0.20},
                        {"title": "Director", "year_5_risk": 0.35, "year_1_risk": 0.15},
                        {"title": "Individual Contributor", "year_5_risk": 0.60, "year_1_risk": 0.35}
                    ]
                    
            if similar_jobs:
                # Create dataframe for table
                similar_df = pd.DataFrame(similar_jobs)
                
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
            
            # Risk Assessment Summary
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Risk Assessment Summary</h3>", unsafe_allow_html=True)
            
            summary_text = job_data.get("summary", "Based on current AI trends and job market analysis, this role is experiencing significant changes due to automation and AI technologies. Skills in human-centric areas like leadership, creativity, and complex problem-solving will be increasingly valuable as routine aspects become automated.")
            st.markdown(summary_text)
            
            # Call to action for Career Navigator
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown("<h3 style='color: #0084FF; font-size: 20px;'>Get Your Personalized Career Plan</h3>", unsafe_allow_html=True)
            st.markdown("Our AI-powered Career Navigator can help you develop a personalized plan to adapt to these changes and thrive in your career.", unsafe_allow_html=True)
            
            # Get HTML from career_navigator module to avoid escaping issues
            st.markdown(career_navigator.get_html(), unsafe_allow_html=True)
            
            # Add Recent Searches section
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
                for search in recent_searches:
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

# Job Comparison Tab - Match original functionality from screenshots
with tabs[1]:  # Job Comparison tab
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    
    # Introduction text
    st.markdown("Compare the AI displacement risk for multiple jobs side by side to explore transition opportunities. Add up to 5 jobs.")
    
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
            # Quick fetch to ensure the title can be resolved to a SOC code
            if "error" not in get_job_data(new_job):
                st.session_state.selected_jobs.append(new_job)
            else:
                st.warning(f"⚠️  '{new_job}' could not be resolved to a valid SOC code and was not added.")
    
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
                    st.rerun()
        
        # Add clear all button
        if st.button("Clear All Jobs", key="clear_jobs"):
            st.session_state.selected_jobs = []
            st.rerun()
    
    # Display comparison when jobs are selected
    if st.session_state.selected_jobs and len(st.session_state.selected_jobs) >= 1:
        st.subheader(f"Analyzing {len(st.session_state.selected_jobs)} Jobs")
        
        # Process jobs with better progress feedback
        progress_text = st.empty()
        job_data_collection = {}
        
        # Show progress as jobs are processed
        for i, job in enumerate(st.session_state.selected_jobs):
            progress_text.write(f"Processing {i+1}/{len(st.session_state.selected_jobs)}: {job}")
            data = get_job_data(job)
            if isinstance(data, dict) and "error" not in data:
                job_data_collection[job] = data
            else:
                st.warning(f"Skipping '{job}' – no valid BLS data available.")
        
        progress_text.write("All jobs processed. Generating comparison...")
        
        # Now we have all job data, proceed with visualization
        # Filter out jobs that failed earlier
        valid_job_titles = list(job_data_collection.keys())
        if not valid_job_titles:
            st.error("No valid jobs to compare. Please add jobs with available BLS data.")
            st.stop()

        job_data = simple_comparison.get_job_comparison_data(valid_job_titles)
        
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
            st.dataframe(comparison_df, use_container_width=True)
            
            # Side-by-side comparison with actual job data
            st.subheader("Job Comparison Analysis")
            
            # Extract BLS and job data for comparison
            jobs_bls_data = {}
            jobs_skill_data = {}
            
            # Extract important data points for each job
            for job_title, job_info in job_data.items():
                # Get BLS data if available
                bls_data = job_info.get("bls_data", {})
                
                # Get additional data from job API integration and our hardcoded BLS data
                try:
                    # Get up-to-date BLS data (via Neon cache / API)
                    api_data = get_job_data(job_title)
                    api_bls_data = api_data.get("bls_data", {})
                    
                    # Use API data if available, otherwise use job_info data
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
                    # If any error occurs fall back to already-loaded row data
                    print(f"Error getting API data for {job_title}: {str(e)}")
                    employment = bls_data.get("employment", "N/A")
                    openings = bls_data.get("annual_job_openings", "N/A")
                    growth = bls_data.get("employment_change_percent", "N/A")
                    
                jobs_bls_data[job_title] = {
                    "Employment": employment,
                    "Annual Job Openings": openings,
                    "Growth": growth,
                    "Category": job_info.get("job_category", "General")
                }
                
                # Get skill data from our job_comparison module
                import job_comparison
                
                # Define default skills first so it's always available
                default_skills = {
                    'technical_skills': ['Data analysis', 'Industry knowledge', 'Computer proficiency'],
                    'soft_skills': ['Communication', 'Problem-solving', 'Adaptability'],
                    'emerging_skills': ['AI collaboration', 'Digital literacy', 'Remote work skills']
                }
                
                # First try an exact case match
                if job_title in job_comparison.JOB_SKILLS:
                    skills = job_comparison.JOB_SKILLS[job_title]
                else:
                    # Try case-insensitive match
                    found = False
                    for skill_job, skill_data in job_comparison.JOB_SKILLS.items():
                        if job_title.lower() == skill_job.lower():
                            skills = skill_data
                            found = True
                            break
                    
                    if not found:
                        # Use default skills if no match found
                        skills = default_skills
                
                jobs_skill_data[job_title] = {
                    "Technical Skills": skills.get('technical_skills', ["N/A"]),
                    "Soft Skills": skills.get('soft_skills', ["N/A"]),
                    "Emerging Skills": skills.get('emerging_skills', ["N/A"])
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
                    "Annual Openings": data["Annual Openings"] if data["Annual Openings"] != "N/A" else "Data unavailable"
                })
            
            # Display employment comparison
            if emp_data:
                emp_df = pd.DataFrame(emp_data)
                st.dataframe(emp_df, use_container_width=True)
            
            # Display skill comparison
            st.markdown("### Skill Comparison")
            
            # Create side-by-side skill comparison
            skill_cols = st.columns(len(jobs_skill_data))
            
            for i, (job, skills) in enumerate(jobs_skill_data.items()):
                with skill_cols[i]:
                    st.markdown(f"#### {job}")
                    
                    st.markdown("**Technical Skills:**")
                    for skill in skills["Technical Skills"]:
                        st.markdown(f"- {skill}")
                    
                    st.markdown("**Soft Skills:**")
                    for skill in skills["Soft Skills"]:
                        st.markdown(f"- {skill}")
                    
                    st.markdown("**Emerging Skills:**")
                    for skill in skills["Emerging Skills"]:
                        st.markdown(f"- {skill}")
            
            # Transition Guidance section
            st.markdown("### Career Transition Recommendations")
            
            # Get lowest risk job from comparison for guidance
            risk_values = [(job, data.get("risk_scores", {}).get("year_5", 0)) 
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

# ------------------------------------------------------------------
# Sidebar Admin Controls (minimal re-introduction)
# ------------------------------------------------------------------

# Place the admin tools in a sidebar expander so normal users don’t see
# them unless explicitly opened.  If admins need the full batch-loading
# dashboard, they should import `admin_dashboard` which can hold all of
# the heavy logic; otherwise we at least surface DB health stats here.

with st.sidebar.expander("⚙️  ADMIN CONTROLS", expanded=False):
    st.subheader("Administrative Tools")

    # Quick DB health indicator
    if database_available:
        from sqlalchemy import text

        status = "❌  Error"
        try:
            db_url = os.environ.get("DATABASE_URL")
            if db_url:
                _tmp_engine = create_engine(db_url)
                with _tmp_engine.connect() as _conn:
                    _conn.execute(text("SELECT 1"))
                status = "✅  Connected"
        except Exception as _admin_db_exc:
            status = f"⚠️  {type(_admin_db_exc).__name__}"

        st.write(f"Database status: **{status}**")
    else:
        st.warning("Database not configured – running in limited mode.")

    # Attempt to import a dedicated admin dashboard (if it exists)
    import importlib
    try:
        admin_dashboard = importlib.import_module("admin_dashboard")  # type: ignore

        # Safely attempt to call render() if it exists
        if callable(getattr(admin_dashboard, "render", None)):
            try:
                admin_dashboard.render()  # type: ignore[attr-defined]
            except st.errors.StreamlitAPIException as api_exc:  # noqa: E501
                # Catch nested element violations or any Streamlit-specific
                # issues coming from within the admin dashboard and show an
                # informative message instead of crashing the main app.
                st.error(
                    "Admin dashboard could not be rendered inside this expander "
                    "because of a Streamlit layout conflict.\n\n"
                    f"Details: {api_exc.__class__.__name__}"
                )
        else:
            st.info(
                "Admin dashboard module is available but does **not** expose a "
                "`render()` function.  Please update `admin_dashboard.py` to "
                "wrap UI code inside a callable `render()`."
            )
    except ModuleNotFoundError:
        st.info(
            "Admin dashboard module (`admin_dashboard.py`) is not deployed in "
            "this environment.  Deploy the module and ensure it is on the "
            "Python path if you wish to enable advanced data-management tools."
        )
