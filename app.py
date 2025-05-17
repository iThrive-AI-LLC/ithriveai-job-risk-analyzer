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
    
    /* Make input fields and dropdowns more visible */
    .stTextInput input, .stSelectbox > div > div {
        border: 1px solid #CCCCCC !important;
        background-color: #FFFFFF !important;
        border-radius: 4px !important;
        padding: 8px 12px !important;
    }
    
    .stTextInput input:focus, .stSelectbox > div > div:focus {
        border-color: #4CACE5 !important;
        box-shadow: 0 0 0 1px #4CACE5 !important;
    }
    
    /* Style buttons */
    .stButton button {
        background-color: #4CACE5 !important;
        color: white !important;
        border: none !important;
        padding: 0.5rem 1rem !important;
        font-weight: 600 !important;
        transition: all 0.2s !important;
    }
    
    .stButton button:hover {
        background-color: #3d8bc9 !important;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1) !important;
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
        # Technical Jobs
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
        'Data Scientist': {
            'year_1_risk': 10.0,
            'year_5_risk': 30.0,
            'job_category': 'technical',
            'risk_factors': [
                'Automated machine learning platforms are becoming more powerful',
                'Data cleaning and preparation can be automated',
                'Standard analysis techniques can be templated'
            ],
            'protective_factors': [
                'Domain expertise and business understanding remain crucial',
                'Novel problem formulation requires human creativity',
                'Interpreting complex results needs human judgment'
            ],
            'analysis': 'Data scientists who focus on novel problem formulation, domain expertise integration, and communicating insights to stakeholders will remain valuable as routine analysis tasks become automated.'
        },
        'IT Support Specialist': {
            'year_1_risk': 25.0,
            'year_5_risk': 55.0,
            'job_category': 'technical',
            'risk_factors': [
                'Self-service troubleshooting systems are improving',
                'Remote diagnostics can identify common issues',
                'Knowledge bases and chatbots handle routine questions'
            ],
            'protective_factors': [
                'Complex troubleshooting requires human problem-solving',
                'Physical hardware issues need human intervention',
                'Emotional support for frustrated users needs human empathy'
            ],
            'analysis': 'Basic IT support faces significant automation risk, but roles requiring complex troubleshooting, hardware expertise, and good customer service skills will evolve rather than disappear.'
        },
        'DevOps Engineer': {
            'year_1_risk': 12.0,
            'year_5_risk': 28.0,
            'job_category': 'technical',
            'risk_factors': [
                'Infrastructure as code automates deployment',
                'Self-healing systems reduce manual intervention',
                'Monitoring tools automate alert responses'
            ],
            'protective_factors': [
                'Complex system architecture requires human expertise',
                'Security considerations need human judgment',
                'Cross-team collaboration requires human communication'
            ],
            'analysis': 'DevOps engineers who focus on complex system architecture, security, and cross-team collaboration will remain valuable as routine operations become increasingly automated.'
        },
        
        # Education Jobs
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
        'School Administrator': {
            'year_1_risk': 18.0,
            'year_5_risk': 35.0,
            'job_category': 'education',
            'risk_factors': [
                'Administrative tasks can be increasingly automated',
                'Data analytics can inform scheduling and resource allocation',
                'Communication systems can be streamlined'
            ],
            'protective_factors': [
                'Leadership and vision setting require human judgment',
                'Conflict resolution needs human empathy',
                'Community relations benefit from human connections'
            ],
            'analysis': 'While administrative aspects may be automated, school administrators who focus on leadership, community building, and strategic decision-making will remain essential.'
        },
        'College Professor': {
            'year_1_risk': 8.0,
            'year_5_risk': 20.0,
            'job_category': 'education',
            'risk_factors': [
                'Recorded lectures can be reused across semesters',
                'Online learning platforms expand reach of top instructors',
                'Grading and assessment can be automated'
            ],
            'protective_factors': [
                'Original research requires human creativity',
                'Mentoring students needs human guidance',
                'In-depth discussions benefit from human expertise'
            ],
            'analysis': 'College professors who focus on original research, mentoring, and facilitating in-depth discussions will remain valuable, though routine teaching tasks will be increasingly augmented by technology.'
        },
        'Educational Counselor': {
            'year_1_risk': 10.0,
            'year_5_risk': 22.0,
            'job_category': 'education',
            'risk_factors': [
                'Information delivery can be automated',
                'Initial assessments can be handled by AI',
                'Standard advice can be systematized'
            ],
            'protective_factors': [
                'Personalized guidance requires human judgment',
                'Emotional support needs human empathy',
                'Complex case management requires human flexibility'
            ],
            'analysis': 'Educational counselors who focus on personalized guidance, emotional support, and handling complex cases will remain essential as information delivery becomes automated.'
        },
        
        # Healthcare Jobs
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
        'Physician': {
            'year_1_risk': 12.0,
            'year_5_risk': 25.0,
            'job_category': 'healthcare',
            'risk_factors': [
                'AI diagnostic systems can identify common conditions',
                'Digital health allows remote consultations',
                'Treatment protocols can be systematized'
            ],
            'protective_factors': [
                'Complex cases require human judgment',
                'Patient rapport needs human empathy',
                'Ethical decisions benefit from human values'
            ],
            'analysis': 'Physicians will increasingly use AI for diagnosis and treatment support, but complex cases, patient relationships, and ethical decisions will keep physicians central to healthcare.'
        },
        'Pharmacist': {
            'year_1_risk': 20.0,
            'year_5_risk': 45.0,
            'job_category': 'healthcare',
            'risk_factors': [
                'Automated dispensing systems reduce manual processing',
                'Drug interaction checks can be automated',
                'Basic patient education can be delivered digitally'
            ],
            'protective_factors': [
                'Complex medication management needs human expertise',
                'Patient counseling benefits from human communication',
                'Medication therapy management requires clinical judgment'
            ],
            'analysis': 'While dispensing tasks face automation, pharmacists who focus on medication therapy management, complex patient counseling, and clinical services will remain essential.'
        },
        'Physical Therapist': {
            'year_1_risk': 5.0,
            'year_5_risk': 15.0,
            'job_category': 'healthcare',
            'risk_factors': [
                'Exercise demonstrations can be delivered digitally',
                'Progress tracking can be automated',
                'Some assessments can be done via telehealth'
            ],
            'protective_factors': [
                'Hands-on treatment requires human touch',
                'Individualized program design needs human expertise',
                'Motivation and emotional support require human connection'
            ],
            'analysis': 'Physical therapy has very low displacement risk due to the hands-on nature of treatment, need for individualized program design, and importance of human motivation and support.'
        },
        'Medical Technologist': {
            'year_1_risk': 25.0,
            'year_5_risk': 50.0,
            'job_category': 'healthcare',
            'risk_factors': [
                'Automated lab equipment reduces manual processing',
                'Image analysis can be performed by AI',
                'Standard procedures can be fully automated'
            ],
            'protective_factors': [
                'Complex sample analysis requires human expertise',
                'Quality control benefits from human oversight',
                'Unusual findings need human investigation'
            ],
            'analysis': 'Routine testing faces significant automation, but medical technologists who specialize in complex analysis, quality control, and investigating unusual findings will remain valuable.'
        },
        'Dentist': {
            'year_1_risk': 10.0,
            'year_5_risk': 25.0,
            'job_category': 'healthcare',
            'risk_factors': [
                'Digital imaging improves diagnosis efficiency',
                'Computer-guided procedures increase precision',
                'Patient education can be delivered digitally'
            ],
            'protective_factors': [
                'Manual dexterity for procedures requires human skills',
                'Patient comfort needs human empathy',
                'Complex treatment planning requires human judgment'
            ],
            'analysis': 'Dentistry has relatively low displacement risk due to the manual dexterity required for procedures, need for patient rapport, and complexity of treatment planning.'
        },
        
        # Transportation Jobs
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
        'Delivery Driver': {
            'year_1_risk': 15.0,
            'year_5_risk': 45.0,
            'job_category': 'transportation',
            'risk_factors': [
                'Route optimization reduces inefficiencies',
                'Autonomous vehicles are being tested for deliveries',
                'Drone delivery is emerging for small packages'
            ],
            'protective_factors': [
                'Last-mile navigation in complex areas needs human judgment',
                'Package handling requires human dexterity',
                'Customer interaction often benefits from human touch'
            ],
            'analysis': 'While autonomous technology will impact delivery services, drivers who handle complex urban routes, require significant package handling, or provide customer service will see slower displacement.'
        },
        'Taxi/Rideshare Driver': {
            'year_1_risk': 25.0,
            'year_5_risk': 70.0,
            'job_category': 'transportation',
            'risk_factors': [
                'Self-driving taxis are being tested in multiple cities',
                'Urban routes are increasingly well-mapped',
                'High economic incentive for automation'
            ],
            'protective_factors': [
                'Complex urban navigation still challenges automation',
                'Customer service aspect benefits from human interaction',
                'Safety concerns may slow full automation'
            ],
            'analysis': 'Taxi and rideshare driving faces high displacement risk as autonomous vehicle technology matures, though regulation and safety concerns may slow the transition in some markets.'
        },
        'Pilot': {
            'year_1_risk': 5.0,
            'year_5_risk': 15.0,
            'job_category': 'transportation',
            'risk_factors': [
                'Autopilot systems handle routine flight phases',
                'Remote piloting technology is advancing',
                'Automated systems can handle emergency procedures'
            ],
            'protective_factors': [
                'Complex decision-making requires human judgment',
                'Passenger confidence relies on human pilots',
                'Regulatory requirements maintain human oversight'
            ],
            'analysis': 'While automation will continue to augment flying, regulatory requirements, passenger expectations, and the need for complex decision-making in emergencies keeps displacement risk low for pilots.'
        },
        
        # Finance Jobs
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
        },
        'Financial Analyst': {
            'year_1_risk': 20.0,
            'year_5_risk': 40.0,
            'job_category': 'finance',
            'risk_factors': [
                'Automated data analysis tools process financial information',
                'Algorithmic trading reduces manual market analysis',
                'Report generation can be automated'
            ],
            'protective_factors': [
                'Strategic insight requires human judgment',
                'Complex market conditions need human interpretation',
                'Client relationships benefit from human interaction'
            ],
            'analysis': 'Financial analysts who focus on strategic insights, complex market interpretation, and client relationships will remain valuable as routine analysis becomes automated.'
        },
        'Bank Teller': {
            'year_1_risk': 35.0,
            'year_5_risk': 60.0,
            'job_category': 'finance',
            'risk_factors': [
                'ATMs and mobile banking reduce need for in-person transactions',
                'Automated systems can handle deposits and withdrawals',
                'Digital identification reduces manual verification'
            ],
            'protective_factors': [
                'Complex transactions benefit from human assistance',
                'Customer service needs human interaction',
                'Financial advice requires human judgment'
            ],
            'analysis': 'Bank teller roles face significant displacement risk as digital banking expands, though roles may evolve toward more complex financial consulting and relationship management.'
        },
        'Insurance Underwriter': {
            'year_1_risk': 30.0,
            'year_5_risk': 65.0,
            'job_category': 'finance',
            'risk_factors': [
                'Automated risk assessment models evaluate applications',
                'Digital data collection reduces manual processing',
                'AI can analyze patterns across large datasets'
            ],
            'protective_factors': [
                'Complex cases require human judgment',
                'New risk categories need human evaluation',
                'Relationship management benefits from human interaction'
            ],
            'analysis': 'Insurance underwriting faces high automation risk for standard policies, though human underwriters will still be needed for complex cases, new risk categories, and relationship management.'
        },
        
        # Marketing Jobs
        'Marketing Manager': {
            'year_1_risk': 12.0,
            'year_5_risk': 30.0,
            'job_category': 'marketing',
            'risk_factors': [
                'Automated campaign management tools optimize spending',
                'AI can analyze customer data and trends',
                'Content generation tools create basic materials'
            ],
            'protective_factors': [
                'Strategic brand positioning needs human creativity',
                'Understanding emerging cultural trends requires human insight',
                'Client relationships benefit from human interaction'
            ],
            'analysis': 'Marketing managers who focus on strategic brand positioning, cultural insight, and relationship management will remain valuable as tactical execution becomes more automated.'
        },
        'Social Media Specialist': {
            'year_1_risk': 25.0,
            'year_5_risk': 45.0,
            'job_category': 'marketing',
            'risk_factors': [
                'Content scheduling can be automated',
                'AI can generate basic social posts',
                'Analytics tools automate performance reporting'
            ],
            'protective_factors': [
                'Authentic brand voice needs human creativity',
                'Crisis management requires human judgment',
                'Community engagement benefits from human interaction'
            ],
            'analysis': 'While basic content creation and scheduling face automation, social media specialists who excel at authentic brand voice, crisis management, and meaningful community engagement will remain valuable.'
        },
        'Market Research Analyst': {
            'year_1_risk': 22.0,
            'year_5_risk': 40.0,
            'job_category': 'marketing',
            'risk_factors': [
                'Data collection can be automated',
                'Analysis tools process information without human intervention',
                'Report generation can be templated'
            ],
            'protective_factors': [
                'Strategic insight requires human judgment',
                'Research design needs human creativity',
                'Contextual understanding benefits from human experience'
            ],
            'analysis': 'Market research analysts who focus on research design, strategic insights, and contextual interpretation will remain valuable as data collection and basic analysis become automated.'
        },
        'Copywriter': {
            'year_1_risk': 25.0,
            'year_5_risk': 50.0,
            'job_category': 'marketing',
            'risk_factors': [
                'AI text generators create basic content',
                'Templates standardize common formats',
                'Editing tools improve writing without human intervention'
            ],
            'protective_factors': [
                'Original, compelling narratives need human creativity',
                'Brand voice consistency benefits from human judgment',
                'Emotional resonance requires human understanding'
            ],
            'analysis': 'While basic content creation faces automation, copywriters who excel at creating original, emotionally resonant content with consistent brand voice will continue to be valued.'
        },
        
        # Legal Jobs
        'Lawyer': {
            'year_1_risk': 15.0,
            'year_5_risk': 35.0,
            'job_category': 'legal',
            'risk_factors': [
                'Document review can be automated',
                'Legal research tools find relevant cases without human search',
                'Contract analysis can be performed by AI'
            ],
            'protective_factors': [
                'Complex legal strategy requires human judgment',
                'Courtroom advocacy needs human persuasion',
                'Client counseling benefits from human empathy'
            ],
            'analysis': 'While document review and basic research face automation, lawyers who focus on complex strategy, advocacy, and client counseling will remain essential to legal practice.'
        },
        'Paralegal': {
            'year_1_risk': 30.0,
            'year_5_risk': 55.0,
            'job_category': 'legal',
            'risk_factors': [
                'Document preparation can be automated',
                'E-discovery tools reduce manual document review',
                'Case management systems automate workflow'
            ],
            'protective_factors': [
                'Client interaction requires human empathy',
                'Complex document preparation needs human judgment',
                'Attorney support benefits from human flexibility'
            ],
            'analysis': 'Paralegal roles face significant automation for routine tasks, though those who specialize in client interaction, complex document preparation, and high-level attorney support will adapt rather than be replaced.'
        },
        'Legal Secretary': {
            'year_1_risk': 35.0,
            'year_5_risk': 65.0,
            'job_category': 'legal',
            'risk_factors': [
                'Scheduling can be automated',
                'Document formatting tools reduce manual preparation',
                'Digital filing systems reduce paper management'
            ],
            'protective_factors': [
                'Complex coordination requires human judgment',
                'Confidentiality benefits from human discretion',
                'Client interaction needs human empathy'
            ],
            'analysis': 'Legal secretary roles face high automation risk for routine tasks, though roles may evolve toward more complex coordination, client interaction, and specialized support functions.'
        },
        
        # Creative Jobs
        'Graphic Designer': {
            'year_1_risk': 15.0,
            'year_5_risk': 35.0,
            'job_category': 'creative',
            'risk_factors': [
                'Template-based design reduces need for custom work',
                'AI-generated imagery creates basic visuals',
                'Automated layout tools arrange elements without human intervention'
            ],
            'protective_factors': [
                'Original concept development needs human creativity',
                'Brand understanding requires human judgment',
                'Client collaboration benefits from human communication'
            ],
            'analysis': 'While basic design production faces automation, graphic designers who excel at original concept development, brand understanding, and client collaboration will remain valuable.'
        },
        'Video Editor': {
            'year_1_risk': 18.0,
            'year_5_risk': 40.0,
            'job_category': 'creative',
            'risk_factors': [
                'Automated editing tools can create rough cuts',
                'AI can identify highlights in footage',
                'Template-based editing reduces custom work'
            ],
            'protective_factors': [
                'Narrative structure requires human creativity',
                'Emotional pacing needs human judgment',
                'Visual storytelling benefits from human aesthetics'
            ],
            'analysis': 'While basic editing tasks face automation, video editors who excel at narrative structure, emotional pacing, and visual storytelling will continue to be valued in the creative industry.'
        },
        'Photographer': {
            'year_1_risk': 20.0,
            'year_5_risk': 45.0,
            'job_category': 'creative',
            'risk_factors': [
                'Stock photography reduces need for custom shoots',
                'AI image generation creates visuals without cameras',
                'Automated editing improves images without human intervention'
            ],
            'protective_factors': [
                'Original aesthetic vision requires human creativity',
                'Subject interaction needs human direction',
                'Technical mastery in challenging conditions requires human skill'
            ],
            'analysis': 'While stock photography and AI-generated images impact the market, photographers with original aesthetic vision, subject interaction skills, and technical mastery in challenging conditions will remain valuable.'
        },
        'Writer/Author': {
            'year_1_risk': 15.0,
            'year_5_risk': 35.0,
            'job_category': 'creative',
            'risk_factors': [
                'AI text generators create basic content',
                'Formulaic genres can be partially automated',
                'Editing tools improve writing without human intervention'
            ],
            'protective_factors': [
                'Original storytelling requires human creativity',
                'Emotional resonance needs human experience',
                'Cultural relevance benefits from human perspective'
            ],
            'analysis': 'While basic content creation faces automation, writers who create original stories with emotional resonance and cultural relevance will continue to be valued in publishing and media.'
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
    
    # Add a Clear Entries button
    if st.button("Clear Entries", type="secondary"):
        # This will trigger a rerun with empty values
        st.session_state.clear()
        st.rerun()
    
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
            
            # Skills Impact Spider Diagram
            st.markdown("<h2 style='color: #0084FF;'>Skills Impact Analysis</h2>", unsafe_allow_html=True)
            
            # Create radar chart data
            radar_categories = ['Technical Skills', 'Creative Thinking', 'Human Interaction', 
                               'Complex Decision Making', 'Physical Skills', 'Specialized Knowledge']
            
            # Determine radar values based on job category and risk level
            job_category = job_data.get('job_category', 'general')
            
            if job_category == 'technical':
                radar_values = [40, 85, 60, 90, 30, 75]  # Technical jobs need creativity and complex decisions
            elif job_category == 'healthcare':
                radar_values = [60, 65, 95, 80, 85, 90]  # Healthcare needs human interaction and specialized knowledge
            elif job_category == 'transportation':
                radar_values = [50, 40, 75, 70, 90, 65]  # Transportation needs physical skills
            elif job_category == 'education':
                radar_values = [50, 80, 95, 75, 50, 80]  # Education needs human interaction and creativity
            elif job_category == 'finance':
                radar_values = [75, 65, 70, 90, 20, 85]  # Finance needs complex decisions
            else:
                # Default balanced values
                radar_values = [65, 70, 75, 75, 60, 70]
            
            # Adjust values based on risk level
            year_5_risk = job_data.get('year_5_risk', 0)
            if year_5_risk > 60:
                # High risk jobs have lower values across the board
                radar_values = [max(20, v - 30) for v in radar_values]
            elif year_5_risk < 25:
                # Low risk jobs have higher values
                radar_values = [min(95, v + 10) for v in radar_values]
            
            # Create radar chart
            fig = go.Figure()
            
            fig.add_trace(go.Scatterpolar(
                r=radar_values,
                theta=radar_categories,
                fill='toself',
                fillcolor='rgba(0, 132, 255, 0.2)',
                line=dict(color='#0084FF', width=2),
                name='AI-Resistant Skills'
            ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 100]
                    )
                ),
                showlegend=False,
                height=400,
                margin=dict(l=80, r=80, t=20, b=20),
                paper_bgcolor='#FFFFFF'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("""
                <div style='background-color: #F0F7FF; padding: 15px; border-radius: 5px; margin-bottom: 20px;'>
                    <p><strong>What this chart means:</strong> The spider diagram shows which skill areas are most protected from AI displacement in this role. 
                    Higher values (further from center) indicate skills that AI will have difficulty replacing.</p>
                </div>
            """, unsafe_allow_html=True)
            
            # Get skill recommendations
            job_cat = job_data.get('job_category', 'general')
            
            if avg_risk >= 70:
                risk_level = "Very High"
            elif avg_risk >= 50:
                risk_level = "High"
            elif avg_risk >= 30:
                risk_level = "Moderate"
            else:
                risk_level = "Low"
                
            # Display skill recommendations
            st.markdown("<h2 style='color: #0084FF;'>Key Skills to Develop</h2>", unsafe_allow_html=True)
            
            # Create three columns for different skill types
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("<h3 style='color: #333333;'>Technical Skills</h3>", unsafe_allow_html=True)
                if job_cat == 'technical':
                    skills = [
                        "Advanced problem-solving",
                        "Systems thinking",
                        "High-level design",
                        "Cross-functional collaboration"
                    ]
                elif job_cat == 'healthcare':
                    skills = [
                        "Advanced patient care",
                        "Specialized clinical expertise",
                        "Integrated care management",
                        "Health tech integration"
                    ]
                elif job_cat == 'education':
                    skills = [
                        "Curriculum development",
                        "Personalized learning design",
                        "Educational technology integration",
                        "Adaptive learning systems"
                    ]
                elif job_cat == 'finance':
                    skills = [
                        "Complex financial analysis",
                        "Risk assessment expertise",
                        "Strategic financial planning",
                        "Fintech integration"
                    ]
                elif job_cat == 'transportation':
                    skills = [
                        "Complex logistics planning",
                        "Transportation systems knowledge",
                        "Route optimization",
                        "Emergency response"
                    ]
                else:
                    skills = [
                        "Complex problem solving",
                        "Critical thinking",
                        "Systems thinking",
                        "Digital tool mastery"
                    ]
                
                for skill in skills:
                    st.markdown(f"• {skill}")
            
            with col2:
                st.markdown("<h3 style='color: #333333;'>Human Skills</h3>", unsafe_allow_html=True)
                
                if job_cat == 'technical':
                    skills = [
                        "Creative thinking",
                        "Interpersonal communication",
                        "Leadership and mentoring",
                        "Stakeholder management"
                    ]
                elif job_cat == 'healthcare':
                    skills = [
                        "Advanced empathy",
                        "Crisis management",
                        "Interdisciplinary collaboration",
                        "Patient advocacy"
                    ]
                elif job_cat == 'education':
                    skills = [
                        "Emotional intelligence",
                        "Mentorship and coaching",
                        "Cultural competence",
                        "Creative engagement"
                    ]
                elif job_cat == 'finance':
                    skills = [
                        "Ethical decision making",
                        "Client relationship management",
                        "Financial communication",
                        "Critical thinking"
                    ]
                elif job_cat == 'transportation':
                    skills = [
                        "Situational awareness",
                        "Customer service excellence",
                        "Communication clarity",
                        "Decision making under pressure"
                    ]
                else:
                    skills = [
                        "Adaptability",
                        "Creativity",
                        "Communication excellence",
                        "Emotional intelligence"
                    ]
                
                for skill in skills:
                    st.markdown(f"• {skill}")
            
            with col3:
                st.markdown("<h3 style='color: #333333;'>Future-Ready Skills</h3>", unsafe_allow_html=True)
                
                if job_cat == 'technical':
                    skills = [
                        "AI/ML ethics",
                        "Human-AI collaboration",
                        "Novel use-case development",
                        "Algorithm auditing"
                    ]
                elif job_cat == 'healthcare':
                    skills = [
                        "AI-assisted diagnostics",
                        "Health tech integration",
                        "AI output verification",
                        "AI-human collaborative care"
                    ]
                elif job_cat == 'education':
                    skills = [
                        "AI-enhanced teaching methods",
                        "Educational technology integration",
                        "Adaptive learning system design",
                        "Digital pedagogy"
                    ]
                elif job_cat == 'finance':
                    skills = [
                        "Fintech integration",
                        "Algorithmic trading oversight",
                        "Automated financial systems",
                        "AI-assisted financial planning"
                    ]
                elif job_cat == 'transportation':
                    skills = [
                        "Autonomous vehicle supervision",
                        "Smart transportation systems",
                        "Advanced telemetry analytics",
                        "Transportation technology integration"
                    ]
                else:
                    skills = [
                        "AI literacy",
                        "Technology adaptation",
                        "Human-AI collaboration",
                        "New technology integration"
                    ]
                
                for skill in skills:
                    st.markdown(f"• {skill}")
            
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
                <a href="https://form.jotform.com/251137815706154" target="_blank" 
                style="display: inline-block; background-color: #4CACE5; color: white; padding: 10px 20px; 
                text-decoration: none; font-weight: bold; border-radius: 5px; margin-top: 10px;">
                Start your personalized Career Navigator package today!</a>
                </div>
                """, unsafe_allow_html=True)

with tabs[1]:  # Job Comparison tab
    st.markdown("<h2 style='color: #0084FF;'>Compare Jobs</h2>", unsafe_allow_html=True)
    
    st.markdown("<p>Compare the AI displacement risk for multiple jobs side by side.</p>", unsafe_allow_html=True)
    
    # Create a more structured selection interface similar to the single job analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<h3 style='color: #333333;'>Select from our database</h3>", unsafe_allow_html=True)
        
        # Job category selection
        job_categories = ["technical", "healthcare", "education", "finance", "transportation", "marketing", "legal", "creative"]
        compare_category = st.selectbox("Choose a job category for comparison", options=job_categories, key="compare_category")
        
        # Create a filtered list of jobs for the selected category
        category_jobs = [job for job, data in JOB_DATA.items() if data.get('job_category') == compare_category]
        category_jobs = category_jobs if category_jobs else list(JOB_DATA.keys())
        
        # Allow multi-select for job comparison from the category
        jobs_to_compare = st.multiselect(
            "Select jobs to compare (2-4 recommended)",
            options=sorted(category_jobs),
            default=category_jobs[:2] if len(category_jobs) >= 2 else None,
            key="category_comparison"
        )
    
    with col2:
        st.markdown("<h3 style='color: #333333;'>Add custom job titles</h3>", unsafe_allow_html=True)
        
        # Custom job input fields (allow up to 3)
        custom_job1 = st.text_input("Custom Job 1:", placeholder="e.g. Interior Designer", key="custom_job1")
        custom_job2 = st.text_input("Custom Job 2:", placeholder="e.g. Web Developer", key="custom_job2")
        custom_job3 = st.text_input("Custom Job 3:", placeholder="e.g. Data Analyst", key="custom_job3")
        
        # Add button for custom jobs
        if st.button("Add Custom Jobs to Comparison", type="primary"):
            for job in [custom_job1, custom_job2, custom_job3]:
                if job and job not in jobs_to_compare:
                    jobs_to_compare.append(job)
    
    # Clear selections button
    if st.button("Clear All Selections", type="secondary"):
        # This will trigger a rerun with empty values
        st.session_state.clear()
        st.rerun()
    
    # Display comparison when at least 2 jobs are selected
    if jobs_to_compare and len(jobs_to_compare) >= 2:
        st.markdown("---")
        st.markdown(f"<h2 style='color: #0084FF;'>Comparing {len(jobs_to_compare)} Jobs</h2>", unsafe_allow_html=True)
        
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