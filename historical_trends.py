"""
Historical AI job displacement trends and predictions module.
This provides data on how AI job displacement risks have evolved over time.
"""

# Historical data on AI displacement risk changes (baseline year 2020)
HISTORICAL_IMPACT_TRENDS = {
    # Technical and computing fields
    'technical': {
        2020: 1.0,  # Baseline (relative risk)
        2021: 1.05,
        2022: 1.12,
        2023: 1.25,
        2024: 1.40,
        2025: 1.60  # Current year projection
    },
    'computing': {
        2020: 1.0,
        2021: 1.08,
        2022: 1.15,
        2023: 1.30,
        2024: 1.45,
        2025: 1.65
    },
    'data_science': {
        2020: 1.0,
        2021: 1.03,
        2022: 1.08,
        2023: 1.20,
        2024: 1.35,
        2025: 1.55
    },
    
    # Administrative and service roles
    'administrative': {
        2020: 1.0,
        2021: 1.15,
        2022: 1.35,
        2023: 1.60,
        2024: 1.90,
        2025: 2.20
    },
    'service': {
        2020: 1.0,
        2021: 1.10,
        2022: 1.25,
        2023: 1.45,
        2024: 1.75,
        2025: 2.10
    },
    'customer_service': {
        2020: 1.0,
        2021: 1.15,
        2022: 1.40,
        2023: 1.70,
        2024: 2.05,
        2025: 2.40
    },
    
    # Human-centered and creative fields
    'management': {
        2020: 1.0,
        2021: 1.02,
        2022: 1.05,
        2023: 1.10,
        2024: 1.18,
        2025: 1.28
    },
    'healthcare': {
        2020: 1.0,
        2021: 1.02,
        2022: 1.06,
        2023: 1.12,
        2024: 1.20,
        2025: 1.30
    },
    'education': {
        2020: 1.0,
        2021: 1.03,
        2022: 1.08,
        2023: 1.15,
        2024: 1.25,
        2025: 1.40
    },
    'creative': {
        2020: 1.0,
        2021: 1.05,
        2022: 1.15,
        2023: 1.30,
        2024: 1.50,
        2025: 1.75
    },
    
    # Other fields
    'legal': {
        2020: 1.0,
        2021: 1.08,
        2022: 1.18,
        2023: 1.35,
        2024: 1.55,
        2025: 1.80
    },
    'financial': {
        2020: 1.0,
        2021: 1.10,
        2022: 1.25,
        2023: 1.45,
        2024: 1.70,
        2025: 2.00
    },
    'transportation': {
        2020: 1.0,
        2021: 1.12,
        2022: 1.28,
        2023: 1.50,
        2024: 1.80,
        2025: 2.15
    },
    'manufacturing': {
        2020: 1.0,
        2021: 1.10,
        2022: 1.25,
        2023: 1.45,
        2024: 1.70,
        2025: 2.00
    },
    'marketing': {
        2020: 1.0,
        2021: 1.08,
        2022: 1.22,
        2023: 1.40,
        2024: 1.65,
        2025: 1.95
    },
    'hr': {
        2020: 1.0,
        2021: 1.05,
        2022: 1.15,
        2023: 1.30,
        2024: 1.50,
        2025: 1.75
    },
    'construction': {
        2020: 1.0,
        2021: 1.03,
        2022: 1.08,
        2023: 1.15,
        2024: 1.25,
        2025: 1.40
    }
}

# Default trend for categories not specifically mapped
DEFAULT_TREND = {
    2020: 1.0,
    2021: 1.08,
    2022: 1.20,
    2023: 1.35,
    2024: 1.55,
    2025: 1.80
}

# Key AI advancement milestones
AI_MILESTONES = [
    {
        'year': 2020,
        'event': "GPT-3 Release",
        'impact': "Advanced text generation capabilities demonstrated"
    },
    {
        'year': 2021,
        'event': "DALL-E and Other Multimodal Models",
        'impact': "AI expanded into image generation from text descriptions"
    },
    {
        'year': 2022,
        'event': "ChatGPT Public Release",
        'impact': "Conversational AI became widely accessible and demonstrated general knowledge capabilities"
    },
    {
        'year': 2023,
        'event': "GPT-4 and Multimodal Large Models",
        'impact': "Advanced reasoning and multi-format processing capabilities emerged"
    },
    {
        'year': 2024,
        'event': "AI Agent Frameworks",
        'impact': "Autonomous AI systems gained capability to perform complex multi-step tasks"
    },
    {
        'year': 2025,
        'event': "Advanced Specialized AI Systems",
        'impact': "Domain-specific AI reached expert-level performance in multiple professional fields"
    }
]

def get_historical_trend(job_category):
    """
    Get historical trend data for a specific job category.
    
    Args:
        job_category (str): The job category
    
    Returns:
        dict: Year-by-year relative risk values
    """
    return HISTORICAL_IMPACT_TRENDS.get(job_category, DEFAULT_TREND)

def get_ai_milestones():
    """
    Get key AI advancement milestones.
    
    Returns:
        list: List of AI milestones by year
    """
    return AI_MILESTONES

def get_historical_analysis(job_category):
    """
    Generate analysis of historical AI impact on the job category.
    
    Args:
        job_category (str): The job category
    
    Returns:
        str: Analysis of historical trends
    """
    trend = get_historical_trend(job_category)
    
    # Calculate acceleration (increase in year-over-year growth)
    yearly_changes = [
        (trend[year] / trend[year-1]) for year in range(2021, 2026)
    ]
    
    # Determine if acceleration is happening
    acceleration = yearly_changes[-1] > yearly_changes[0]
    
    # Calculate total growth from 2020 to 2025
    total_growth = trend[2025] / trend[2020] - 1  # As percentage
    
    # Generate analysis
    if acceleration:
        if total_growth > 1.0:  # More than 100% increase
            pace = "rapidly accelerating"
            outlook = "The pace of AI impact on this field is increasing substantially each year."
        else:
            pace = "steadily increasing"
            outlook = "The field is experiencing a consistent, growing impact from AI advancements."
    else:
        pace = "relatively stable"
        outlook = "While AI is affecting this field, the pace of change has been relatively consistent."
    
    category_insights = {
        'technical': "Technical roles have seen growing impact from AI coding assistants and automated development tools.",
        'computing': "Computing jobs have been affected by AI code generation and automated testing systems.",
        'data_science': "Data science work has been enhanced but also partially automated by AutoML and no-code analytics.",
        'administrative': "Administrative tasks have been increasingly automated through document processing AI and scheduling systems.",
        'service': "Service roles have seen growing automation through chatbots and self-service technologies.",
        'customer_service': "Customer service has experienced significant automation through conversational AI and support bots.",
        'management': "Management roles have been less impacted, though AI now assists with data-driven decision making.",
        'healthcare': "Healthcare has seen AI impact mainly in diagnostics and administrative processes, less in direct care.",
        'education': "Educational roles have been supplemented rather than replaced by AI learning tools.",
        'creative': "Creative fields initially seemed protected but are now seeing increasing AI impact in content generation.",
        'legal': "Legal research and document processing have been increasingly automated while complex advisory work remains human-driven.",
        'financial': "Financial analysis and routine transactions have been increasingly automated.",
        'transportation': "Autonomous vehicle technology has steadily advanced toward greater capabilities.",
        'manufacturing': "Robotics and AI-driven process optimization have steadily increased manufacturing automation.",
        'marketing': "Marketing has seen growing AI impact in content creation, analytics, and campaign optimization.",
        'hr': "HR functions like resume screening and initial interviews have been increasingly automated.",
        'construction': "Physical construction roles have been less impacted, though design and planning are seeing AI adoption."
    }
    
    specific_insight = category_insights.get(
        job_category, 
        "This field has experienced the typical pattern of initial automation of routine tasks, followed by AI assistance for more complex work."
    )
    
    analysis = f"""
    ## Historical AI Impact (2020-2025)
    
    The displacement risk for this job category has shown a **{pace}** pattern over the past five years, 
    with a total increase of **{total_growth*100:.0f}%** from 2020 to 2025.
    
    {outlook}
    
    {specific_insight}
    
    Key turning points have included the advent of large language models (2020-2022), multimodal AI systems (2022-2023),
    and the development of AI agent frameworks (2024-2025) that can perform complex multi-step tasks.
    """
    
    return analysis