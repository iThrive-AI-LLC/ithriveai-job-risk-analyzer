import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import time

# Import all needed modules for job risk data and comparison
from simple_comparison import (
    get_job_data, get_job_categories, get_jobs_by_category, 
    create_comparison_table, create_comparison_chart, 
    create_risk_heatmap, create_radar_chart, create_factor_comparison
)

# Import skill recommendations
from skill_recommendations import get_skill_recommendations, get_adaptation_strategies

# Set page config
st.set_page_config(
    page_title="AI Job Displacement Risk Analyzer",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
.main-header {
    font-size: 3.2rem;
    color: #1E88E5;
    font-weight: bold;
    text-align: center;
    margin-bottom: 0.5rem;
    line-height: 1.2;
}
.sub-header {
    font-size: 1.5rem;
    color: #424242;
    text-align: center;
    margin-bottom: 2rem;
}
.risk-high {
    color: #D32F2F;
    font-weight: bold;
}
.risk-moderate {
    color: #FF9800;
    font-weight: bold;
}
.risk-low {
    color: #4CAF50;
    font-weight: bold;
}
.highlight {
    background-color: #f0f7ff;
    padding: 10px;
    border-radius: 5px;
    border-left: 5px solid #1E88E5;
}
</style>
""", unsafe_allow_html=True)

# Header with larger, centered styling
st.markdown('<h1 class="main-header">HOW IS YOUR JOB IMPACTED BY AI?</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Use our AI Job Displacement Risk Analyzer to assess your career outlook over the next 5 years</p>', unsafe_allow_html=True)

# Add app description in sidebar
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/6295/6295417.png", width=100)
st.sidebar.title("About This Tool")
st.sidebar.markdown("""
This tool uses real-time data from:
- Oxford Martin School research
- Brookings Institution studies
- McKinsey Global Institute forecasts
- World Economic Forum reports
- Bureau of Labor Statistics data

The analysis evaluates your job's displacement risk based on:
- Task automation potential
- AI capability development
- Industry adoption trends
- Historical patterns
""")

# Add custom CSS for more prominent tabs
st.markdown("""
<style>
.stTabs [data-baseweb="tab-list"] {
    gap: 24px;
}
.stTabs [data-baseweb="tab"] {
    font-size: 18px;
    font-weight: 600;
    color: #777777;
}
.stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
    color: #1E88E5;
    border-bottom-color: #1E88E5;
    border-bottom-width: 3px;
}
</style>
""", unsafe_allow_html=True)

# Create tabs for different features
tab1, tab2 = st.tabs(["Single Job Analysis", "Job Comparison"])

# Tab 1: Single Job Analysis
with tab1:
    st.write("### Analyze a Job")
    
    # Instructions
    st.markdown("""
    <div style="background-color: #f0f7ff; padding: 10px; border-radius: 5px; margin-bottom: 15px;">
    <p><strong>Instructions:</strong> Either select a job from our database using the dropdown menus or enter any job title in the text field. 
    Click the "Analyze Risk" button to get a comprehensive AI displacement risk assessment, including visualizations, 
    skill recommendations, and adaptation strategies.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Create two columns for better layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Option 1: Select a job from our database**")
        # Category selection outside the form
        job_categories = get_job_categories()
        single_category = st.selectbox("Choose a job category", job_categories, key="tab1_category")
    
    # Use exact list of jobs for each category
    job_list_by_category = {
        "technical": ['Software Engineer', 'Data Scientist', 'Web Developer', 'DevOps Engineer', 
                    'Database Administrator', 'IT Support Technician', 'Cybersecurity Analyst', 'AI Engineer'],
        "management": ['Project Manager', 'Product Manager', 'Operations Manager', 'Human Resources Manager', 'Chief Executive Officer'],
        "healthcare": ['Nurse', 'Physician', 'Medical Technician', 'Pharmacist', 'Physical Therapist', 'Radiologist'],
        "education": ['Teacher', 'College Professor', 'School Administrator', 'School Counselor'],
        "service": ['Cook', 'Waiter/Waitress', 'Bartender', 'Hotel Receptionist', 'Retail Salesperson', 'Customer Service Representative'],
        "restaurant": ['Chef', 'Restaurant Manager', 'Hostess', 'Dishwasher', 'Line Cook', 'Food Delivery Driver'],
        "finance": ['Accountant', 'Financial Analyst', 'Investment Banker', 'Insurance Underwriter', 'Bank Teller'],
        "legal": ['Lawyer', 'Paralegal', 'Court Reporter', 'Judge', 'Legal Secretary', 'Claims Adjuster'],
        "emergency": ['Police Officer', 'Firefighter', 'EMT/Paramedic', 'Emergency Room Nurse', 'Air Traffic Controller', 'Dispatcher'],
        "creative": ['Graphic Designer', 'Writer', 'Marketing Specialist', 'Photographer', 'Film Director'],
        "transportation": ['Truck Driver', 'Taxi Driver', 'Delivery Driver', 'Airline Pilot'],
        "manufacturing": ['Factory Worker', 'Construction Worker', 'Electrician', 'Architect']
    }
    
    # Get the list of jobs for the selected category
    selected_category_jobs = job_list_by_category.get(single_category, [])
    
    with col1:
        # Job selection for that category
        single_selected_job = st.selectbox(
            "Select a job from this category", 
            [""] + selected_category_jobs,
            key="tab1_job"
        )
        
        # Add clear button
        if st.button("Clear Selection", key="clear_job_button"):
            # Use a safer approach to clear selections
            if 'tab1_job' in st.session_state:
                del st.session_state['tab1_job']
            if 'tab1_manual_job' in st.session_state:
                del st.session_state['tab1_manual_job']
            st.rerun()
    
    with col2:
        st.write("**Option 2: Enter any job title**")
        manual_job_title = st.text_input(
            "Enter your job title:",
            placeholder="e.g. Software Developer, Nurse, Marketing Manager",
            key="tab1_manual_job"
        )
    
    # Form just for the submit button
    with st.form("job_analysis_form"):
        # Use selected job if provided, otherwise use manually entered job title
        job_title = single_selected_job if single_selected_job else manual_job_title
        
        # Display the job that will be analyzed
        if job_title:
            st.write(f"Job to analyze: **{job_title}**")
        else:
            st.warning("Please select a job from the dropdown or enter a job title")
            
        submit_button = st.form_submit_button("Analyze Risk")

# Tab 2: Job Comparison
with tab2:
    st.subheader("Job Risk Comparison Tool")
    st.markdown("""
    Compare AI displacement risks across different jobs to understand relative vulnerabilities.
    """)
    
    # Instructions
    st.markdown("""
    <div style="background-color: #f0f7ff; padding: 10px; border-radius: 5px; margin-bottom: 15px;">
    <p><strong>Instructions:</strong> Select jobs from the categories on the left or enter custom job titles on the right. 
    You can add up to 5 jobs to compare. Click "Add Selected Jobs" or "Add Custom Job" to add them to your comparison list, 
    then click "Compare Selected Jobs" to generate visualizations showing relative displacement risks.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize session state for selected jobs
    if 'selected_jobs' not in st.session_state:
        st.session_state.selected_jobs = []
    
    # Create two columns for job selection
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Select from Categories")
        
        # Category selection
        job_categories = get_job_categories()
        selected_category = st.selectbox("Choose a job category", job_categories)
        
        # Jobs in that category
        category_jobs = get_jobs_by_category(selected_category)
        selected_jobs = st.multiselect("Select jobs from this category", category_jobs)
        
        # Add button
        if st.button("Add Selected Jobs"):
            for job in selected_jobs:
                if job not in st.session_state.selected_jobs and len(st.session_state.selected_jobs) < 5:
                    st.session_state.selected_jobs.append(job)
    
    with col2:
        st.markdown("### Add Custom Job")
        
        # Custom job entry
        custom_job = st.text_input("Enter a custom job title")
        
        # Add button
        if st.button("Add Custom Job"):
            if custom_job and custom_job not in st.session_state.selected_jobs and len(st.session_state.selected_jobs) < 5:
                st.session_state.selected_jobs.append(custom_job)
    
    # Show currently selected jobs
    st.markdown("### Selected Jobs for Comparison")
    if not st.session_state.selected_jobs:
        st.info("No jobs selected. Please add jobs from the options above.")
    else:
        # Display selected jobs with option to remove
        selected_jobs_str = ", ".join(st.session_state.selected_jobs)
        st.write(f"Currently comparing: {selected_jobs_str}")
        
        # Clear button
        if st.button("Clear All Jobs"):
            st.session_state.selected_jobs = []
            st.rerun()
    
    # Run comparison if jobs are selected
    if st.session_state.selected_jobs:
        # Run comparison button
        if st.button("Compare Selected Jobs"):
            with st.spinner("Generating comparison..."):
                # Get data for selected jobs
                job_data_comparison = get_job_data(st.session_state.selected_jobs)
                
                # Show which jobs have data
                available_jobs = list(job_data_comparison.keys())
                missing_jobs = set(st.session_state.selected_jobs) - set(job_data_comparison.keys())
                
                if missing_jobs:
                    st.warning(f"Unable to analyze these jobs: {', '.join(missing_jobs)}")
                
                if available_jobs:
                    # Create comparison table
                    st.subheader("Job Risk Comparison Table")
                    comparison_df = create_comparison_table(job_data_comparison)
                    st.dataframe(comparison_df, hide_index=True)
                    
                    # Create visualization tabs
                    st.subheader("Job Risk Visualizations")
                    
                    # Add custom CSS for the visualization tabs
                    st.markdown("""
                    <style>
                    .stTabs [data-baseweb="tab-panel"] .stTabs [data-baseweb="tab-list"] {
                        gap: 16px;
                    }
                    .stTabs [data-baseweb="tab-panel"] .stTabs [data-baseweb="tab"] {
                        font-size: 16px;
                        font-weight: 500;
                        color: #777777;
                    }
                    .stTabs [data-baseweb="tab-panel"] .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
                        color: #1E88E5;
                        border-bottom-color: #1E88E5;
                        border-bottom-width: 2px;
                    }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    vis_tabs = st.tabs(["Bar Chart", "Heatmap", "Radar Chart", "Risk Factors"])
                    
                    # Bar chart
                    with vis_tabs[0]:
                        st.markdown("#### Risk Comparison Bar Chart")
                        bar_chart = create_comparison_chart(job_data_comparison)
                        if bar_chart:
                            st.plotly_chart(bar_chart, use_container_width=True)
                        else:
                            st.warning("Unable to create bar chart.")
                    
                    # Heatmap
                    with vis_tabs[1]:
                        st.markdown("#### Risk Timeline Heatmap")
                        heatmap = create_risk_heatmap(job_data_comparison)
                        if heatmap:
                            st.plotly_chart(heatmap, use_container_width=True)
                        else:
                            st.warning("Unable to create heatmap.")
                    
                    # Radar chart
                    with vis_tabs[2]:
                        st.markdown("#### Job Risk Radar Chart")
                        if len(available_jobs) <= 5:  # Limit for readability
                            radar = create_radar_chart(job_data_comparison)
                            if radar:
                                st.plotly_chart(radar, use_container_width=True)
                            else:
                                st.warning("Unable to create radar chart.")
                        else:
                            st.info("Please select 5 or fewer jobs for the radar chart visualization.")
                    
                    # Risk factors comparison
                    with vis_tabs[3]:
                        st.markdown("#### Risk Factors Comparison")
                        if len(available_jobs) <= 3:  # Limit for readability
                            factors = create_factor_comparison(job_data_comparison)
                            if factors:
                                st.plotly_chart(factors, use_container_width=True)
                            else:
                                st.warning("Unable to create factor comparison chart.")
                        else:
                            st.info("Please select 3 or fewer jobs for the detailed risk factors visualization.")
                else:
                    st.error("No data available for the selected jobs. Please try different job titles.")

# Process when form is submitted in Tab 1
if submit_button and job_title:
    # Show loading message
    with tab1, st.spinner(f"Analyzing AI displacement risk for '{job_title}'..."):
        try:
            # Get displacement risk data directly from simple_comparison.py which has reliable data
            job_data = get_job_data([job_title])
            
            # Check if job data was found
            if job_title in job_data:
                processed_data = job_data[job_title]
                
                # Display results
                st.markdown("---")
                st.subheader(f"AI Displacement Risk Analysis: {job_title}")
                
                # Central risk gauge and overview
                col1, col2, col3 = st.columns([1, 2, 1])
                
                # Get average risk (weighted more toward 5-year) for central gauge
                avg_risk = (processed_data.get('year_1_risk', 0) * 0.3) + (processed_data.get('year_5_risk', 0) * 0.7)
                overall_risk_level = "Low"
                if avg_risk >= 70:
                    overall_risk_level = "Very High"
                    gauge_color = "#D32F2F"  # Red
                elif avg_risk >= 50:
                    overall_risk_level = "High"
                    gauge_color = "#F44336"  # Light Red
                elif avg_risk >= 30:
                    overall_risk_level = "Moderate"
                    gauge_color = "#FF9800"  # Orange
                else:
                    gauge_color = "#4CAF50"  # Green
                
                # Job category in first column
                with col1:
                    st.metric("Job Category", processed_data.get('job_category', 'Unknown'))
                
                # Central risk gauge
                with col2:
                    # Create a gauge visualization for overall risk
                    st.markdown(f"""
                    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center;">
                        <h3 style="margin-bottom: 5px;">Overall AI Displacement Risk</h3>
                        <div style="width: 150px; height: 150px; border-radius: 50%; background: conic-gradient(
                            {gauge_color} 0% {avg_risk}%, 
                            #E0E0E0 {avg_risk}% 100%
                        ); display: flex; justify-content: center; align-items: center; margin: 10px;">
                            <div style="width: 120px; height: 120px; border-radius: 50%; background-color: white; 
                                display: flex; justify-content: center; align-items: center; flex-direction: column;">
                                <span style="font-size: 2rem; font-weight: bold; color: {gauge_color};">{int(avg_risk)}%</span>
                                <span style="font-weight: bold; color: {gauge_color};">{overall_risk_level}</span>
                            </div>
                        </div>
                        <p style="text-align: center; margin-top: 5px; font-size: 0.9em;">
                            Weighted average based on 1-year and 5-year projections
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Original metrics for 5-year risk
                with col3:
                    st.metric("5-Year Risk", f"{processed_data.get('year_5_risk', 0):.1f}%")
                
                # Risk level indicators
                col1, col2 = st.columns(2)
                
                year_1_risk_level = "Low"
                if processed_data.get('year_1_risk', 0) >= 70:
                    year_1_risk_level = "Very High"
                elif processed_data.get('year_1_risk', 0) >= 50:
                    year_1_risk_level = "High"
                elif processed_data.get('year_1_risk', 0) >= 30:
                    year_1_risk_level = "Moderate"
                
                year_5_risk_level = "Low"
                if processed_data.get('year_5_risk', 0) >= 70:
                    year_5_risk_level = "Very High"
                elif processed_data.get('year_5_risk', 0) >= 50:
                    year_5_risk_level = "High"
                elif processed_data.get('year_5_risk', 0) >= 30:
                    year_5_risk_level = "Moderate"
                
                with col1:
                    # Enhanced 1-Year Risk Indicator
                    color_1yr = "#4CAF50" if year_1_risk_level == "Low" else "#FF9800" if year_1_risk_level == "Moderate" else "#F44336"
                    st.markdown(f"""
                    <div style="background-color: rgba(0,0,0,0.05); padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                        <h4 style="margin:0; padding-bottom:5px;">1-Year Outlook</h4>
                        <div style="display: flex; align-items: center;">
                            <div style="width: 50px; height: 50px; border-radius: 50%; background-color: {color_1yr}; display: flex; 
                                justify-content: center; align-items: center; color: white; font-weight: bold; margin-right: 15px;">
                                {int(processed_data.get('year_1_risk', 0))}%
                            </div>
                            <div>
                                <span style="font-weight: bold; color: {color_1yr};">{year_1_risk_level} Risk</span>
                                <br><span style="font-size: 0.9em;">Short-term impact</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    # Enhanced 5-Year Risk Indicator
                    color_5yr = "#4CAF50" if year_5_risk_level == "Low" else "#FF9800" if year_5_risk_level == "Moderate" else "#F44336"
                    st.markdown(f"""
                    <div style="background-color: rgba(0,0,0,0.05); padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                        <h4 style="margin:0; padding-bottom:5px;">5-Year Outlook</h4>
                        <div style="display: flex; align-items: center;">
                            <div style="width: 50px; height: 50px; border-radius: 50%; background-color: {color_5yr}; display: flex; 
                                justify-content: center; align-items: center; color: white; font-weight: bold; margin-right: 15px;">
                                {int(processed_data.get('year_5_risk', 0))}%
                            </div>
                            <div>
                                <span style="font-weight: bold; color: {color_5yr};">{year_5_risk_level} Risk</span>
                                <br><span style="font-size: 0.9em;">Long-term impact</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Call to Action Section
                st.markdown("---")
                st.subheader("Ready for a Personalized Career Transition Plan?")
                st.markdown("""
                <div style="background-color: #f0f7ff; padding: 20px; border-radius: 10px; border-left: 5px solid #1E88E5;">
                <h4>Get Expert Guidance for Your Career Transition</h4>
                <p>Based on your job's AI displacement risk, you may benefit from personalized career guidance.</p>
                <ul>
                    <li><strong>Premium Plan:</strong> Actionable Detailed Execution Plans (On-Demand PDFs): For each recommended role, receive a separate, step-by-step plan detailing phased actions, resources, application strategies, and investment considerations to guide your career transition.</li>
                    <li><strong>Pro and Premium Plan:</strong> Flexibility to Refine & Regenerate: Access a secure link to edit your initial information (including resume and military details) and regenerate your Career Navigation Package a limited number of times to explore updated scenarios.</li>
                    <li><strong>Basic Plan:</strong> Navigation Report (PDF): A comprehensive, AI-generated guide tailored to your unique inputs, skills, resume, and military background (if applicable), identifying 3 aligned job roles with in-depth analysis, training recommendations, and next steps.</li>
                    <li><strong>Basic Plan:</strong> Convenient Email Delivery: All personalized reports and plans are delivered directly to your email as easy-to-access PDF documents.</li>
                </ul>
                </div>
                """, unsafe_allow_html=True)
                
                # CTA Button
                cta_button = st.button("Get Your Personalized Career Transition Package", type="primary", use_container_width=True)
                
                # Button click handler
                if cta_button:
                    # Open URL in a new tab
                    st.markdown(f'<meta http-equiv="refresh" content="0;url=https://form.jotform.com/251137815706154">', unsafe_allow_html=True)
                    st.markdown("Opening enrollment form in a new tab...")
                    # Additional message after click
                    st.success("Taking you to the enrollment form for the Personalized Career Navigator package.")
                
                # Risk factors
                st.markdown("### Key Risk Factors")
                risk_factors = processed_data.get('risk_factors', {})
                
                if risk_factors:
                    # Convert risk_factors to a format suitable for display
                    risk_factor_list = []
                    for factor, value in risk_factors.items():
                        if isinstance(value, (int, float)):
                            impact_level = "Very High" if value >= 70 else "High" if value >= 50 else "Moderate" if value >= 30 else "Low"
                            risk_factor_list.append({"Risk Factor": factor, "Impact Level": impact_level, "Score": value})
                        else:
                            risk_factor_list.append({"Risk Factor": factor, "Impact Level": value, "Score": 0})
                    
                    # Create dataframe for display
                    risk_factor_df = pd.DataFrame(risk_factor_list)
                    st.dataframe(risk_factor_df[["Risk Factor", "Impact Level"]], hide_index=True, width=None)
                else:
                    st.warning("No specific risk factors available for this job.")
                
                # Create year-by-year risk table
                st.markdown("### Year-by-Year Risk Analysis")
                
                # Calculate risks for years 1-5
                year_1_risk = processed_data.get('year_1_risk', 0)
                year_5_risk = processed_data.get('year_5_risk', 0)
                years = range(1, 6)
                
                risk_values = [
                    year_1_risk,
                    (year_5_risk - year_1_risk) * 0.25 + year_1_risk,
                    (year_5_risk - year_1_risk) * 0.5 + year_1_risk,
                    (year_5_risk - year_1_risk) * 0.75 + year_1_risk,
                    year_5_risk
                ]
                
                # Determine risk level for each year
                risk_levels = []
                for risk in risk_values:
                    if risk < 30:
                        risk_levels.append("Low")
                    elif risk < 50:
                        risk_levels.append("Moderate")
                    elif risk < 70:
                        risk_levels.append("High")
                    else:
                        risk_levels.append("Very High")
                
                # Create risk progression dataframe
                risk_df = pd.DataFrame({
                    "Year": [f"Year {year}" for year in years],
                    "Risk %": [f"{risk:.1f}%" for risk in risk_values],
                    "Risk Level": risk_levels
                })
                
                st.dataframe(risk_df, hide_index=True, width=None)
                
                # Create a Job Risk Visualizations section with tabs
                st.markdown("### Job Risk Visualizations")
                
                # Add custom CSS for the visualization tabs in the single job analysis
                st.markdown("""
                <style>
                .stTabs [data-baseweb="tab-panel"] .stTabs [data-baseweb="tab-list"] {
                    gap: 16px;
                }
                .stTabs [data-baseweb="tab-panel"] .stTabs [data-baseweb="tab"] {
                    font-size: 16px;
                    font-weight: 500;
                    color: #777777;
                }
                .stTabs [data-baseweb="tab-panel"] .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
                    color: #1E88E5;
                    border-bottom-color: #1E88E5;
                    border-bottom-width: 2px;
                }
                </style>
                """, unsafe_allow_html=True)
                
                vis_tabs = st.tabs(["Risk Progression", "Risk Factors Radar", "Impact Timeline"])
                
                # Tab 1: Risk Progression Chart
                with vis_tabs[0]:
                    # Create risk progression chart
                    fig = px.line(
                        x=[f"Year {year}" for year in years],
                        y=risk_values,
                        markers=True,
                        title="AI Displacement Risk Progression",
                        labels={"x": "Timeline", "y": "Risk Percentage"}
                    )
                    
                    # Set y-axis range
                    fig.update_layout(yaxis_range=[0, 100])
                    
                    # Add threshold lines
                    fig.add_shape(type="line", x0="Year 1", x1="Year 5", y0=30, y1=30,
                                 line=dict(color="green", width=2, dash="dash"))
                    
                    fig.add_shape(type="line", x0="Year 1", x1="Year 5", y0=50, y1=50,
                                 line=dict(color="orange", width=2, dash="dash"))
                    
                    fig.add_shape(type="line", x0="Year 1", x1="Year 5", y0=70, y1=70,
                                 line=dict(color="red", width=2, dash="dash"))
                    
                    # Add annotations for threshold lines
                    fig.add_annotation(x="Year 5", y=30, text="Low Risk Threshold",
                                      showarrow=False, yshift=10, xshift=70)
                    
                    fig.add_annotation(x="Year 5", y=50, text="Moderate Risk Threshold",
                                      showarrow=False, yshift=10, xshift=70)
                    
                    fig.add_annotation(x="Year 5", y=70, text="High Risk Threshold",
                                      showarrow=False, yshift=10, xshift=70)
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                # Tab 2: Risk Factors Radar Chart
                with vis_tabs[1]:
                    # Create radar chart for risk factors
                    risk_factors = processed_data.get('risk_factors', {})
                    
                    if risk_factors:
                        # Extract risk factors and values
                        categories = list(risk_factors.keys())
                        values = list(risk_factors.values())
                        
                        # Create radar chart
                        fig = go.Figure()
                        
                        fig.add_trace(go.Scatterpolar(
                            r=values,
                            theta=categories,
                            fill='toself',
                            name=job_title,
                            line_color='rgb(31, 119, 180)'
                        ))
                        
                        fig.update_layout(
                            polar=dict(
                                radialaxis=dict(
                                    visible=True,
                                    range=[0, 100]
                                )
                            ),
                            showlegend=False,
                            title=f"Risk Factor Analysis for {job_title}"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Add description for risk factors
                        st.markdown("#### Risk Factor Definitions")
                        risk_descriptions = {
                            "Task Automation": "How easily routine tasks can be automated",
                            "AI Capability": "Current and projected AI ability to perform job functions",
                            "Industry Adoption": "Rate at which your industry is adopting AI solutions",
                            "Digital Transformation": "How quickly your field is being digitized",
                            "Specialized Knowledge": "Degree to which the role requires expertise AI can't easily replicate"
                        }
                        
                        for factor in categories:
                            if factor in risk_descriptions:
                                st.markdown(f"**{factor}**: {risk_descriptions[factor]}")
                    else:
                        st.warning("Risk factor data not available for this job.")
                
                # Tab 3: Impact Timeline
                with vis_tabs[2]:
                    # Create a timeline of potential AI impact
                    st.markdown("#### AI Impact Timeline")
                    
                    # Create dataframe for timeline
                    milestones = [
                        {"Year": "Now", "Milestone": "Current AI capabilities affecting your role", "Impact": f"{year_1_risk:.1f}% displacement risk"},
                        {"Year": "Year 1-2", "Milestone": "Near-term AI advancements", "Impact": f"{(risk_values[1] + risk_values[2])/2:.1f}% displacement risk"},
                        {"Year": "Year 3-5", "Milestone": "Medium-term AI capabilities", "Impact": f"{year_5_risk:.1f}% displacement risk"},
                        {"Year": "Beyond 5 Years", "Milestone": "Long-term outlook", "Impact": "Requires continual skill development"}
                    ]
                    
                    # Create a colored timeline
                    for i, milestone in enumerate(milestones):
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.markdown(f"**{milestone['Year']}**")
                        with col2:
                            color = "red" if "High" in year_5_risk_level else "orange" if year_5_risk_level == "Moderate" else "green"
                            st.markdown(f"<div style='background-color: rgba(0,0,0,0.05); padding: 10px; border-left: 5px solid {color};'><strong>{milestone['Milestone']}</strong><br>{milestone['Impact']}</div>", unsafe_allow_html=True)
                            if i < len(milestones) - 1:
                                st.markdown("↓")
                
                # Analysis explanation
                st.markdown("### Analysis Summary")
                
                # Generate analysis based on risk level
                if year_5_risk_level in ["High", "Very High"]:
                    analysis = f"The {job_title} role faces a {year_5_risk_level.lower()} risk of AI displacement within 5 years. Key factors driving this risk include advancements in automation technologies, machine learning algorithms capable of handling tasks traditionally performed by {job_title}s, and industry trends toward AI adoption. Consider developing specialized skills in areas AI struggles with, such as creative problem-solving, emotional intelligence, and complex decision-making."
                elif year_5_risk_level == "Moderate":
                    analysis = f"The {job_title} role faces a moderate risk of AI displacement within 5 years. While some aspects of this job may be automated, the role likely requires human skills that AI currently struggles to replicate. Consider upskilling in areas that complement AI technologies and focus on aspects of the job that require human judgment, creativity, or interpersonal skills."
                else:
                    analysis = f"The {job_title} role faces a low risk of AI displacement within 5 years. This job likely requires skills that are difficult to automate, such as emotional intelligence, creative thinking, complex problem-solving, or physical dexterity. While AI may augment aspects of this job, it's unlikely to replace human workers entirely in this field in the near future."
                
                st.markdown(f"<div class='highlight'>{analysis}</div>", unsafe_allow_html=True)
                
                # Adaptation strategies
                st.markdown("---")
                st.subheader("Adaptation Strategies")
                
                # Generate strategies based on risk level
                if year_5_risk_level in ["High", "Very High"]:
                    strategies = [
                        "Focus on developing skills that AI struggles with, such as creative thinking and emotional intelligence",
                        "Consider transitioning to roles that supervise or work alongside AI systems",
                        "Explore adjacent career paths that leverage your experience but face lower automation risk",
                        "Invest in specialized knowledge that's difficult to automate",
                        "Develop skills in AI implementation and maintenance - become the handler rather than the handled"
                    ]
                elif year_5_risk_level == "Moderate":
                    strategies = [
                        "Identify which aspects of your role are most vulnerable to automation and develop complementary skills",
                        "Focus on building expertise in areas requiring human judgment and creativity",
                        "Learn to work effectively with AI tools to enhance your productivity",
                        "Develop stronger interpersonal and leadership skills",
                        "Consider specializing in niches where human expertise remains highly valued"
                    ]
                else:
                    strategies = [
                        "Stay current with AI developments in your field to identify emerging trends",
                        "Focus on continuous learning to maintain your competitive edge",
                        "Identify ways to leverage AI tools to enhance your productivity",
                        "Develop strong collaboration and communication skills",
                        "Consider how your role might evolve as AI becomes more prevalent in your industry"
                    ]
                
                for strategy in strategies:
                    st.markdown(f"- {strategy}")
                
                # Skill recommendations section
                st.markdown("---")
                st.subheader("Recommended Skills to Develop")
                
                # Get job category from the processed data
                job_category = processed_data.get('job_category', 'technical')
                
                # Get skill recommendations based on job category and risk level
                skill_recs = get_skill_recommendations(job_category, year_5_risk_level, job_title)
                
                # Display skill recommendations
                if skill_recs:
                    st.markdown(f"<div class='highlight'><strong>Priority:</strong> {skill_recs['urgency']}<br>{skill_recs['approach']}</div>", unsafe_allow_html=True)
                    
                    # Create columns for different skill types
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown("#### Core Professional Skills")
                        for skill in skill_recs.get('core_skills', []):
                            st.markdown(f"- {skill}")
                    
                    with col2:
                        st.markdown("#### Emerging Tech Skills")
                        for skill in skill_recs.get('emerging_skills', []):
                            st.markdown(f"- {skill}")
                    
                    with col3:
                        st.markdown("#### Human/Soft Skills")
                        for skill in skill_recs.get('soft_skills', []):
                            st.markdown(f"- {skill}")
                    
                    # Additional adaptation strategies
                    adaptation_strategies = get_adaptation_strategies(job_category, year_5_risk_level, job_title)
                    if adaptation_strategies:
                        st.markdown("#### Specific Adaptation Actions")
                        for strategy in adaptation_strategies[:5]:  # Limit to 5 strategies
                            st.markdown(f"- {strategy}")
                else:
                    st.warning("No skill recommendations available for this job category.")
                
                # Research sources
                st.markdown("---")
                st.subheader("Research Sources")
                st.markdown("""
                This analysis is based on aggregated data from:
                - Oxford Martin School research on automation probabilities
                - Brookings Institution studies on AI and future of work
                - McKinsey Global Institute forecasts on automation and work
                - World Economic Forum reports on the future of jobs
                - Bureau of Labor Statistics data on job outlook
                """)
                
            else:
                st.warning(f"Sorry, we don't have data for '{job_title}'. Please try a different job title such as 'Software Engineer', 'Teacher', 'Nurse', or 'Project Manager'.")
                
        except Exception as e:
            st.error(f"Error analyzing job: {str(e)}")
            st.markdown("Please try another job title or try again later.")