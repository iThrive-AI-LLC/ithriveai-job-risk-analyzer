"""
Data Processor Module

This module provides fallback risk analysis for job titles when BLS mapping fails.
It uses heuristic pattern matching to categorize jobs and calculate AI displacement risk.
"""

import re
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Union
import datetime

# Configure logging
logger = logging.getLogger(__name__)

# Dictionary of job categories and associated risk profiles
JOB_CATEGORIES = {
    'education': {
        'pattern': r'teacher|professor|instructor|educator|tutor|lecturer|librarian|media|collections|specialist|school|education|teaching|academic|faculty',
        'base_risk': 25,
        'yearly_increase': 6,
        'variance': 5,
        'risk_factors': [
            "Increasing adoption of online learning platforms",
            "AI tools for content creation and curation",
            "Automated grading and assessment systems",
            "Digital cataloging and information retrieval systems"
        ],
        'protective_factors': [
            "Need for human guidance and mentorship",
            "Social-emotional learning components",
            "Complex information evaluation skills",
            "Community engagement and relationship building"
        ]
    },
    'technical': {
        'pattern': r'developer|engineer|programmer|analyst|scientist|researcher|technician|IT|software|data|system|code|technical|specialist|technology',
        'base_risk': 35,
        'yearly_increase': 8,
        'variance': 7,
        'risk_factors': [
            "Automated code generation tools",
            "Low-code/no-code platforms",
            "AI-powered debugging and testing",
            "Standardization of technical processes"
        ],
        'protective_factors': [
            "Complex problem-solving requirements",
            "Need for novel solutions and innovation",
            "System architecture and design skills",
            "Cross-functional collaboration abilities"
        ]
    },
    'administrative': {
        'pattern': r'assistant|clerk|secretary|administrative|receptionist|office|coordinator|data entry|typist|admin|support|clerical',
        'base_risk': 65,
        'yearly_increase': 7,
        'variance': 4,
        'risk_factors': [
            "Document automation and digital workflows",
            "AI-powered scheduling and organization tools",
            "Natural language processing for correspondence",
            "Automated data entry and form processing"
        ],
        'protective_factors': [
            "Interpersonal communication skills",
            "Adaptability to changing priorities",
            "Organizational knowledge and context",
            "Problem-solving for unique situations"
        ]
    },
    'creative': {
        'pattern': r'artist|writer|designer|musician|actor|director|producer|creative|composer|architect|fashion|content|creator|graphic|media',
        'base_risk': 20,
        'yearly_increase': 7,
        'variance': 8,
        'risk_factors': [
            "AI-generated content and designs",
            "Automated editing and production tools",
            "Template-based creative systems",
            "Generative art and music technologies"
        ],
        'protective_factors': [
            "Original concept development",
            "Cultural context and emotional intelligence",
            "Unique artistic vision and style",
            "Human connection and authenticity"
        ]
    },
    'service': {
        'pattern': r'cashier|retail|sales|server|waiter|waitress|hospitality|customer|service|attendant|clerk|barista|store|shop',
        'base_risk': 55,
        'yearly_increase': 9,
        'variance': 6,
        'risk_factors': [
            "Self-checkout and automated ordering systems",
            "AI-powered customer service chatbots",
            "Automated inventory and stocking systems",
            "Digital payment and transaction processing"
        ],
        'protective_factors': [
            "Personal touch and customer relationships",
            "Complex problem resolution skills",
            "Adaptability to unique customer needs",
            "Emotional intelligence and empathy"
        ]
    },
    'healthcare': {
        'pattern': r'doctor|nurse|physician|therapist|medical|health|healthcare|clinical|dental|pharmacy|pharmacist|patient|care|practitioner',
        'base_risk': 15,
        'yearly_increase': 5,
        'variance': 6,
        'risk_factors': [
            "AI diagnostic and imaging analysis tools",
            "Automated patient monitoring systems",
            "Digital health records and documentation",
            "Telemedicine and remote care platforms"
        ],
        'protective_factors': [
            "Hands-on patient care requirements",
            "Complex diagnostic reasoning",
            "Empathy and bedside manner",
            "Ethical decision-making abilities"
        ]
    },
    'management': {
        'pattern': r'manager|director|supervisor|executive|chief|head|lead|leadership|management|administrator|principal',
        'base_risk': 30,
        'yearly_increase': 6,
        'variance': 7,
        'risk_factors': [
            "Automated decision support systems",
            "AI-powered performance analytics",
            "Project management automation tools",
            "Predictive business intelligence systems"
        ],
        'protective_factors': [
            "Strategic thinking and vision",
            "Team building and motivation skills",
            "Complex stakeholder management",
            "Adaptability to organizational change"
        ]
    }
}

# Default category for when no specific match is found
DEFAULT_CATEGORY = {
    'base_risk': 40,
    'yearly_increase': 7,
    'variance': 6,
    'risk_factors': [
        "Increasing automation across industries",
        "AI tools for routine task completion",
        "Digital transformation of workflows",
        "Standardization of processes"
    ],
    'protective_factors': [
        "Complex problem-solving requirements",
        "Human creativity and innovation",
        "Interpersonal skills and collaboration",
        "Adaptability to changing conditions"
    ]
}

def determine_risk_factors(job_title: str, data_sources: Dict[str, Any]) -> Dict[str, Any]:
    """
    Determine risk factors based on job title and category.
    
    Args:
        job_title: The job title to analyze
        data_sources: Additional data for analysis (optional)
        
    Returns:
        Dictionary with risk factors and category information
    """
    job_title_lower = job_title.lower()
    job_category = None
    category_info = DEFAULT_CATEGORY
    best_match_score = 0
    
    # Determine job category using regex pattern matching
    for category, info in JOB_CATEGORIES.items():
        pattern = info['pattern']
        matches = re.findall(pattern, job_title_lower)
        match_score = len(matches)
        
        if match_score > best_match_score:
            job_category = category
            category_info = info
            best_match_score = match_score
    
    # If no good match found, use default
    if best_match_score == 0:
        job_category = "general"
        category_info = DEFAULT_CATEGORY
    
    logger.info(f"Determined category '{job_category}' for job title '{job_title}' with match score {best_match_score}")
    
    # Extract specific risk factors based on job title keywords
    additional_factors = []
    
    # Add specific factors for librarians and media specialists
    if re.search(r'librarian|media collection|specialist', job_title_lower):
        additional_factors = [
            "Digital cataloging and automated classification systems",
            "AI-powered search and information retrieval tools",
            "Digital content management replacing physical collections",
            "Automated content recommendation systems"
        ]
    
    # Combine category risk factors with any additional ones
    risk_factors = category_info.get('risk_factors', [])[:3]  # Take top 3 from category
    if additional_factors:
        risk_factors = additional_factors + risk_factors[:2]  # Prioritize specific factors
    
    protective_factors = category_info.get('protective_factors', [])[:3]
    
    return {
        'job_category': job_category,
        'base_risk': category_info['base_risk'],
        'yearly_increase': category_info['yearly_increase'],
        'variance': category_info['variance'],
        'risk_factors': risk_factors,
        'protective_factors': protective_factors
    }

def calculate_risk_levels(risk_values: List[float]) -> List[str]:
    """
    Convert numerical risk values to risk level categories.
    
    Args:
        risk_values: List of risk percentages
        
    Returns:
        List of risk level categories (Low, Moderate, High, Very High)
    """
    risk_levels = []
    
    for risk in risk_values:
        if risk < 30:
            risk_levels.append("Low")
        elif risk < 50:
            risk_levels.append("Moderate")
        elif risk < 75:
            risk_levels.append("High")
        else:
            risk_levels.append("Very High")
    
    return risk_levels

def process_job_data(job_title: str, data_sources: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Process job data to determine displacement risk when BLS mapping fails.
    
    Args:
        job_title: The job title to analyze
        data_sources: Additional data for analysis (optional)
        
    Returns:
        Dictionary with complete job risk analysis
    """
    if data_sources is None:
        data_sources = {}
    
    # Get risk factors and category information
    risk_info = determine_risk_factors(job_title, data_sources)
    
    # Extract parameters
    base_risk = risk_info['base_risk']
    yearly_increase = risk_info['yearly_increase']
    variance = risk_info['variance']
    job_category = risk_info['job_category']
    
    # Calculate risk for years 1-5
    risk_values = []
    np.random.seed(hash(job_title) % 10000)  # Consistent randomness for same job title
    
    for year in range(1, 6):
        # Add some randomness but ensure consistent results for same job title
        variation = np.random.normal(0, variance)
        
        # Year factor makes later years slightly less predictable
        year_factor = 1 - (0.1 * (year - 1))
        
        # Calculate risk with diminishing returns for later years
        risk = min(98, base_risk + (yearly_increase * year * year_factor) + variation)
        risk = max(2, risk)  # Ensure minimum risk of 2%
        
        risk_values.append(round(risk, 1))
    
    # Get risk level descriptions
    risk_levels = calculate_risk_levels(risk_values)
    
    # Create summary and analysis
    current_year = datetime.datetime.now().year
    years = [current_year + i for i in range(5)]
    
    # Generate analysis text
    if job_title.lower() == "librarians and media collections specialists":
        analysis = (
            f"Librarians and Media Collections Specialists face {risk_levels[4].lower()} AI displacement risk "
            f"({risk_values[4]}%) over the next 5 years. While traditional library functions like cataloging and "
            f"information retrieval are increasingly automated, the role is evolving toward digital curation, "
            f"information literacy instruction, and community engagement. Specialists who develop skills in "
            f"digital content management, data analysis, and user experience design will be better positioned "
            f"to adapt to technological changes in the field."
        )
    else:
        analysis = (
            f"Analysis for {job_title} shows {risk_levels[0].lower()} initial risk ({risk_values[0]}%) "
            f"growing to {risk_levels[4].lower()} risk ({risk_values[4]}%) over 5 years. "
            f"Jobs in the {job_category} category typically face increasing automation pressure "
            f"as AI technologies mature. Professionals should focus on developing skills that "
            f"complement rather than compete with automation."
        )
    
    # Prepare result dictionary
    result = {
        'job_title': job_title,
        'job_category': job_category,
        'risk_metrics': {
            'year_1_risk': risk_values[0],
            'year_5_risk': risk_values[4],
            'year_1_level': risk_levels[0],
            'year_5_level': risk_levels[4]
        },
        'year_1': risk_values[0],
        'year_2': risk_values[1],
        'year_3': risk_values[2],
        'year_4': risk_values[3],
        'year_5': risk_values[4],
        'risk_level_1': risk_levels[0],
        'risk_level_2': risk_levels[1],
        'risk_level_3': risk_levels[2],
        'risk_level_4': risk_levels[3],
        'risk_level_5': risk_levels[4],
        'summary': f"AI displacement risk for {job_title}: {risk_values[4]}% over 5 years ({risk_levels[4]}).",
        'analysis': analysis,
        'factors': risk_info['risk_factors'],
        'protective_factors': risk_info['protective_factors'],
        'years': years,
        'risk_values': risk_values,
        'source': "data_processor_heuristic",
        'variance': variance  # Include variance to fix the KeyError
    }
    
    return result

if __name__ == "__main__":
    # Test with some example job titles
    test_titles = [
        "Librarians and Media Collections Specialists",
        "Software Engineer",
        "Administrative Assistant",
        "Creative Director",
        "Retail Sales Associate",
        "Nurse Practitioner",
        "Project Manager"
    ]
    
    for title in test_titles:
        result = process_job_data(title)
        print(f"\n--- {title} ---")
        print(f"Category: {result['job_category']}")
        print(f"5-Year Risk: {result['year_5']}% ({result['risk_level_5']})")
        print(f"Risk Factors: {result['factors'][:2]}")
