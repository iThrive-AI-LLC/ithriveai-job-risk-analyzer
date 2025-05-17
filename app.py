import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import time

# Set page config first - this must be the first Streamlit command
st.set_page_config(
    page_title="iThriveAI Job Risk Analyzer",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed"  # Start with sidebar collapsed for faster load
)

# Custom CSS for light theme matching the godaddy button page
st.markdown("""
<style>
    /* Main app background */
    .stApp {
        background-color: #FFFFFF;
    }
    
    /* Headers */
    h1, h2, h3, h4, h5, h6 {
        color: #0084FF !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
        background-color: #F8FBFF;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: #F0F7FF;
        border-radius: 4px 4px 0 0;
        color: #333333;
        padding: 10px 20px;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #4CACE5 !important;
        color: white !important;
    }
    
    /* Remove white space after Analyze a Job */
    [data-testid="stVerticalBlock"] {
        gap: 0 !important;
    }
    
    .element-container:empty {
        display: none !important;
    }
    
    /* General spacing */
    .css-1y0tads, .css-1544g2n {
        padding: 0rem 1rem 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Display logo and header
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.image("https://img1.wsimg.com/isteam/ip/70686f32-22d2-489c-a383-6fcd793644be/blob-3712e2e.png/:/rs=h:197,cg:true,m/qt=q:95", width=250)
    st.markdown("<h1 style='text-align: center; color: #0084FF;'>Is your job at risk with AI innovation?</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #4CACE5;'>AI Job Displacement Risk Analyzer</p>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #666666;'>Discover how AI might impact your career in the next 5 years and get personalized recommendations.</p>", unsafe_allow_html=True)

# Create tabs for single job analysis and comparison
tabs = st.tabs(["Single Job Analysis", "Job Comparison"])

with tabs[0]:  # Single Job Analysis tab
    st.markdown("<h2 style='color: #0084FF;'>Analyze a Job</h2>", unsafe_allow_html=True)
    # Remove that empty space by not having any blank lines or elements here
    
    # Basic job data for common occupations
    JOB_DATA = {
        'Software Engineer': {
            'year_1_risk': 15.0,
            'year_5_risk': 35.0,
            'job_category': 'technical',
            'risk_factors': [
                'Automated code generation is improving rapidly',
                'AI code assistants can handle routine programming tasks',
                'Standardized development processes can be automated'
            ],
            'protective_factors': [
                'Complex problem-solving still requires human insight',
                'System architecture and design needs human creativity',
                'Collaboration and code review require human judgment'
            ],
            'analysis': 'While AI will automate routine coding tasks, software engineers who focus on complex problem-solving, system architecture, and collaboration will remain valuable.'
        },
        'Teacher': {
            'year_1_risk': 12.0,
            'year_5_risk': 25.0,
            'job_category': 'education',
            'risk_factors': [
                'Online learning platforms can deliver standard curriculum',
                'AI tutors can provide personalized learning paths',
                'Administrative tasks can be automated'
            ],
            'protective_factors': [
                'Social-emotional learning requires human connection',
                'Classroom management needs human presence',
                'Mentorship and inspiration aspects are difficult to automate'
            ],
            'analysis': 'Teaching roles will evolve with more technology integration, but the human connection, mentorship, and classroom management aspects keep displacement risk moderate.'
        },
        'Nurse': {
            'year_1_risk': 8.0,
            'year_5_risk': 18.0,
            'job_category': 'healthcare',
            'risk_factors': [
                'Remote monitoring systems reduce some bedside checks',
                'AI diagnostic tools assist with routine assessments',
                'Electronic health records automate documentation'
            ],
            'protective_factors': [
                'Physical care requires human dexterity and empathy',
                'Complex patient situations need human judgment',
                'Patient comfort and emotional support need human touch'
            ],
            'analysis': 'Nursing has low displacement risk due to the physical care, emotional support, and complex judgment required. Technology will augment rather than replace nurses.'
        },
        'Truck Driver': {
            'year_1_risk': 20.0,
            'year_5_risk': 65.0,
            'job_category': 'transportation',
            'risk_factors': [
                'Self-driving technology is advancing rapidly',
                'Long-haul routes are often predictable and mappable',
                'Economic pressure to reduce transportation costs'
            ],
            'protective_factors': [
                'Complex urban environments still challenge automation',
                'Loading/unloading often requires human intervention',
                'Maintenance and troubleshooting need human skills'
            ],
            'analysis': 'Truck driving faces significant displacement risk from autonomous vehicles, particularly for long-haul routes. However, local delivery, specialized transport, and logistics roles may be more resilient.'
        },
        'Accountant': {
            'year_1_risk': 25.0,
            'year_5_risk': 45.0,
            'job_category': 'finance',
            'risk_factors': [
                'Automated bookkeeping software handles routine tasks',
                'Tax preparation software becomes more sophisticated',
                'AI can analyze financial data and generate reports'
            ],
            'protective_factors': [
                'Complex tax planning requires human expertise',
                'Financial strategy and advising need human judgment',
                'Regulatory compliance benefits from human oversight'
            ],
            'analysis': 'While basic accounting tasks face automation, roles focusing on financial strategy, complex tax planning, and personalized financial advice remain valuable.'
        }
    }

    # Function to get job data (simplified for fast loading)
    def get_quick_job_data(job_title):
        # Check if we have exact data for this job
        if job_title in JOB_DATA:
            return JOB_DATA[job_title]
        
        # Look for similar jobs (very simple matching)
        lower_title = job_title.lower()
        for known_job, data in JOB_DATA.items():
            if known_job.lower() in lower_title or lower_title in known_job.lower():
                # Return with adjusted values and note
                result = data.copy()
                result['analysis'] = f"Note: Using data from similar role ({known_job}). " + result['analysis'] 
                return result
        
        # Default values if no match found
        return {
            'year_1_risk': 30.0,
            'year_5_risk': 50.0,
            'job_category': 'general',
            'risk_factors': [
                'AI and automation are affecting most industries',
                'Routine aspects of most jobs can be automated',
                'Economic pressure to increase efficiency'
            ],
            'protective_factors': [
                'Complex decision-making often requires human judgment',
                'Social intelligence and emotional aspects are difficult to automate',
                'Adaptability and creativity remain human strengths'
            ],
            'analysis': 'We don\'t have specific data for this job title, but most roles face some level of disruption from AI and automation. Jobs that require adaptability, creativity, and complex problem-solving tend to be more resilient.'
        }

    # Create the two options side by side
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<h3 style='color: #333333;'>Option 1: Select a job from our database</h3>", unsafe_allow_html=True)
        
        # Job category selection
        job_categories = ["technical", "healthcare", "education", "finance", "transportation", "marketing", "legal", "creative"]
        job_category = st.selectbox("Choose a job category", options=job_categories)
        
        # Create a filtered list of jobs for the selected category
        category_jobs = [job for job, data in JOB_DATA.items() if data.get('job_category') == job_category]
        category_jobs = category_jobs if category_jobs else list(JOB_DATA.keys())
        
        selected_job = st.selectbox("Select a job from this category", options=category_jobs)
        
        # Add a button to analyze the selected job
        analyze_selected = st.button("Analyze Selected Job", type="primary")
    
    with col2:
        st.markdown("<h3 style='color: #333333;'>Option 2: Enter any job title</h3>", unsafe_allow_html=True)
        
        # Text input for custom job
        custom_job = st.text_input("Enter your job title:", placeholder="e.g. Software Developer, Nurse, Marketing Manager")
        
        # Add a button to analyze the custom job
        analyze_custom = st.button("Analyze Custom Job", type="primary")
    
    # Determine which job to analyze
    job_to_analyze = None
    if analyze_selected:
        job_to_analyze = selected_job
    elif analyze_custom and custom_job:
        job_to_analyze = custom_job
    
    # Process when a job is entered
    if job_to_analyze:
        with st.spinner(f"Analyzing AI displacement risk for '{job_to_analyze}'..."):
            # Small delay for UX - makes it feel like calculation is happening
            time.sleep(0.5)
            
            # Get job data
            job_data = get_quick_job_data(job_to_analyze)
            
            # Display results
            st.markdown("---")
            st.markdown(f"<h2 style='color: #0084FF;'>AI Displacement Risk Analysis: {job_to_analyze}</h2>", unsafe_allow_html=True)
            
            # Create three columns
            col1, col2, col3 = st.columns([1, 2, 1])
            
            # Calculate overall risk (weighted toward 5-year)
            year_1_risk = job_data.get('year_1_risk', 0)
            year_5_risk = job_data.get('year_5_risk', 0)
            avg_risk = (year_1_risk * 0.3) + (year_5_risk * 0.7)
            
            # Determine risk level and color
            risk_level = "Low"
            gauge_color = "#4CAF50"  # Green
            
            if avg_risk >= 70:
                risk_level = "Very High"
                gauge_color = "#D32F2F"  # Red
            elif avg_risk >= 50:
                risk_level = "High"
                gauge_color = "#F44336"  # Light Red
            elif avg_risk >= 30:
                risk_level = "Moderate"
                gauge_color = "#FF9800"  # Orange
            
            with col1:
                job_category = job_data.get('job_category', 'General')
                st.metric("Job Category", job_category.title())
            
            with col2:
                # Create a gauge chart for risk visualization
                fig = go.Figure(go.Indicator(
                    mode = "gauge+number",
                    value = avg_risk,
                    title = {'text': f"Overall AI Displacement Risk: {risk_level}", 'font': {'color': '#333333'}},
                    gauge = {
                        'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': '#333333'},
                        'bar': {'color': gauge_color},
                        'bgcolor': '#F8FBFF',
                        'borderwidth': 2,
                        'bordercolor': '#F0F7FF',
                        'steps': [
                            {'range': [0, 30], 'color': "#E8F5E9"},  # Light green
                            {'range': [30, 50], 'color': "#FFF3E0"},  # Light orange
                            {'range': [50, 70], 'color': "#FFEBEE"},  # Light red
                            {'range': [70, 100], 'color': "#FFCDD2"}  # Lighter red
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': avg_risk
                        }
                    }
                ))
                
                fig.update_layout(
                    height=300,
                    margin=dict(l=30, r=30, t=50, b=30),
                    paper_bgcolor='#FFFFFF',
                    font={'color': '#333333'}
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            with col3:
                # Show 1-year and 5-year risk metrics
                st.metric("1-Year Risk", f"{year_1_risk:.1f}%")
                st.metric("5-Year Risk", f"{year_5_risk:.1f}%")
            
            # Show key findings
            st.markdown("<h2 style='color: #0084FF;'>Key Findings</h2>", unsafe_allow_html=True)
            
            # Risk factors
            st.markdown("<h3 style='color: #F44336;'>Risk Factors</h3>", unsafe_allow_html=True)
            risk_factors = job_data.get('risk_factors', [])
            for i, factor in enumerate(risk_factors):
                st.markdown(f"**{i+1}.** {factor}")
            
            # Protective factors
            st.markdown("<h3 style='color: #4CAF50;'>Protective Factors</h3>", unsafe_allow_html=True)
            protective_factors = job_data.get('protective_factors', [])
            for i, factor in enumerate(protective_factors):
                st.markdown(f"**{i+1}.** {factor}")
            
            # Analysis
            st.markdown("<h3 style='color: #0084FF;'>Analysis</h3>", unsafe_allow_html=True)
            st.markdown(job_data.get('analysis', 'No analysis available.'))
            
            # Timeline visualization
            st.markdown("<h2 style='color: #0084FF;'>Risk Progression Timeline</h2>", unsafe_allow_html=True)
            
            # Create timeline data
            timeline_data = {
                'Year': [1, 2, 3, 4, 5],
                'Risk': [
                    year_1_risk,
                    year_1_risk * 1.2,  # Estimated year 2
                    year_1_risk * 1.4,  # Estimated year 3
                    year_1_risk * 1.6,  # Estimated year 4
                    year_5_risk         # Year 5
                ]
            }
            
            df_timeline = pd.DataFrame(timeline_data)
            
            # Create timeline chart
            fig = px.line(
                df_timeline, 
                x='Year', 
                y='Risk',
                title=f'Projected AI Displacement Risk for {job_to_analyze} Over Time',
                labels={'Risk': 'Risk (%)', 'Year': 'Years from Now'},
                markers=True
            )
            
            fig.update_traces(line=dict(width=3, color=gauge_color), marker=dict(size=10))
            fig.update_layout(
                xaxis=dict(tickmode='linear'),
                yaxis=dict(range=[0, 100]),
                plot_bgcolor='#F8FBFF',
                paper_bgcolor='#FFFFFF'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Add call-to-action
            st.markdown("---")
            st.markdown("<h2 style='color: #0084FF;'>Ready for a Complete Career Transition Plan?</h2>", unsafe_allow_html=True)
            
            cols = st.columns(2)
            with cols[0]:
                st.markdown("""
                Knowing your AI displacement risk is just the first step. Our Career Navigator offers:
                
                - Custom skill development roadmap
                - AI-informed career transition planning
                - Industry-specific opportunity analysis
                - Personalized action steps for your situation
                """)
            
            with cols[1]:
                st.markdown("""
                <div style="background-color: #F0F7FF; padding: 20px; border-radius: 10px; text-align: center;">
                <h3 style="margin-top: 0; color: #0084FF;">Career Transition Package</h3>
                <p>Get a personalized plan tailored to your specific situation.</p>
                <a href="https://form.jotform.com/240636561351150" target="_blank" 
                style="display: inline-block; background-color: #4CACE5; color: white; padding: 10px 20px; 
                text-decoration: none; font-weight: bold; border-radius: 5px; margin-top: 10px;">
                Get Your Career Transition Package Now!</a>
                </div>
                """, unsafe_allow_html=True)

with tabs[1]:  # Job Comparison tab
    st.markdown("<h2 style='color: #0084FF;'>Compare Multiple Jobs</h2>", unsafe_allow_html=True)
    st.write("Select multiple jobs to compare their AI displacement risks side by side.")
    
    # Create multiselect for job comparison
    jobs_to_compare = st.multiselect("Select jobs to compare:", options=list(JOB_DATA.keys()))
    
    # Add custom job option
    custom_job_compare = st.text_input("Add a custom job title:", placeholder="e.g. Data Scientist")
    if st.button("Add Custom Job", type="primary") and custom_job_compare:
        jobs_to_compare.append(custom_job_compare)
    
    # Display comparison when jobs are selected
    if jobs_to_compare and len(jobs_to_compare) > 0:
        # Get data for all selected jobs
        comparison_data = {}
        for job in jobs_to_compare:
            comparison_data[job] = get_quick_job_data(job)
        
        # Create comparison chart
        st.markdown("<h3 style='color: #0084FF;'>5-Year AI Displacement Risk Comparison</h3>", unsafe_allow_html=True)
        
        # Prepare data for chart
        chart_data = {
            'Job': [],
            'Risk (%)': [],
            'Category': []
        }
        
        for job, data in comparison_data.items():
            chart_data['Job'].append(job)
            chart_data['Risk (%)'].append(data['year_5_risk'])
            chart_data['Category'].append(data['job_category'])
        
        df_chart = pd.DataFrame(chart_data)
        
        # Create horizontal bar chart
        fig = px.bar(
            df_chart,
            y='Job',
            x='Risk (%)',
            color='Risk (%)',
            color_continuous_scale=['#4CAF50', '#FF9800', '#F44336', '#D32F2F'],
            orientation='h',
            title='5-Year AI Displacement Risk by Job',
            labels={'Job': '', 'Risk (%)': 'Risk (%)'},
            height=400
        )
        
        fig.update_layout(
            xaxis=dict(range=[0, 100]),
            plot_bgcolor='#F8FBFF',
            paper_bgcolor='#FFFFFF'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Create comparison table
        st.markdown("<h3 style='color: #0084FF;'>Detailed Comparison</h3>", unsafe_allow_html=True)
        
        # Prepare data for table
        table_data = {
            'Job': [],
            'Category': [],
            '1-Year Risk': [],
            '5-Year Risk': [],
            'Key Risk Factor': [],
            'Key Protective Factor': []
        }
        
        for job, data in comparison_data.items():
            table_data['Job'].append(job)
            table_data['Category'].append(data['job_category'].title())
            table_data['1-Year Risk'].append(f"{data['year_1_risk']}%")
            table_data['5-Year Risk'].append(f"{data['year_5_risk']}%")
            table_data['Key Risk Factor'].append(data['risk_factors'][0] if data['risk_factors'] else 'N/A')
            table_data['Key Protective Factor'].append(data['protective_factors'][0] if data['protective_factors'] else 'N/A')
        
        df_table = pd.DataFrame(table_data)
        
        # Style the table
        st.dataframe(df_table, use_container_width=True)
        
        # Add a note about the comparison
        st.info("Note: This comparison shows the projected AI displacement risk based on current technological trends and research. Individual outcomes may vary based on specific skills, experience, and adaptability.")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center;">
    <p style="font-style: italic; color: #666666;">iThriveAI - AI-Driven, Human-Focused</p>
    <p style="color: #666666;">© 2025 iThriveAI | <a href="https://i-thrive-ai.com" target="_blank" style="color: #0084FF;">i-thrive-ai.com</a></p>
</div>
""", unsafe_allow_html=True)