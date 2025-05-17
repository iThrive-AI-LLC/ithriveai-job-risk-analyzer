import re
import pandas as pd
import numpy as np
from collections import Counter

# Dictionary of job categories and associated risk profiles
# These are derived from research studies on AI impact
JOB_CATEGORIES = {
    # Creative and artistic jobs - generally lower initial risk, increasing over time
    'creative': {
        'pattern': r'artist|writer|designer|musician|actor|director|producer|creative|composer|architect|fashion',
        'base_risk': 20,
        'yearly_increase': 5,
        'variance': 5
    },
    
    # Technical and analytical jobs - moderate initial risk with moderate growth
    'technical': {
        'pattern': r'developer|engineer|programmer|analyst|scientist|researcher|technician|IT|software|data|system|code',
        'base_risk': 30,
        'yearly_increase': 8,
        'variance': 7
    },
    
    # Computing and software development - specialized technical category
    'computing': {
        'pattern': r'software engineer|web developer|frontend|backend|full[-\s]?stack|devops|software developer|coder|programmer',
        'base_risk': 35,
        'yearly_increase': 9,
        'variance': 6
    },
    
    # Data science and AI - specialized technical category
    'data_science': {
        'pattern': r'data scientist|machine learning|AI engineer|data analyst|statistician|data engineer|NLP|computer vision',
        'base_risk': 25,
        'yearly_increase': 7,
        'variance': 8
    },
    
    # Administrative and clerical jobs - higher initial risk with steady growth
    'administrative': {
        'pattern': r'assistant|clerk|secretary|administrative|receptionist|office|coordinator|data entry|typist',
        'base_risk': 65,
        'yearly_increase': 6,
        'variance': 4
    },
    
    # Management and leadership - lower initial risk with slower growth
    'management': {
        'pattern': r'manager|director|executive|CEO|CTO|CFO|president|supervisor|head|leader|chief',
        'base_risk': 15,
        'yearly_increase': 4,
        'variance': 3
    },
    
    # Healthcare and medical - varied but generally lower risk due to human element
    'healthcare': {
        'pattern': r'doctor|nurse|physician|therapist|dentist|pharmacist|medical|health|care|clinical|surgeon',
        'base_risk': 10,
        'yearly_increase': 7,
        'variance': 5
    },
    
    # Education and teaching - moderate risk with steady growth
    'education': {
        'pattern': r'teacher|professor|instructor|educator|tutor|lecturer|coach|trainer|teaching',
        'base_risk': 25,
        'yearly_increase': 7,
        'variance': 5
    },
    
    # Service industry - higher initial risk, faster growth
    'service': {
        'pattern': r'cashier|retail|sales|server|waiter|waitress|hospitality|customer|service|attendant|clerk|barista',
        'base_risk': 55,
        'yearly_increase': 9,
        'variance': 6
    },
    
    # Legal professions - varied risk depending on level, but increasing over time
    'legal': {
        'pattern': r'lawyer|attorney|paralegal|legal|judge|counsel|solicitor|barrister',
        'base_risk': 35,
        'yearly_increase': 8,
        'variance': 7
    },
    
    # Transportation and logistics - high initial risk due to autonomous vehicles
    'transportation': {
        'pattern': r'driver|pilot|captain|operator|courier|delivery|trucker|transportation|logistics|shipping',
        'base_risk': 70,
        'yearly_increase': 8,
        'variance': 5
    },
    
    # Manufacturing and production - high initial risk, steady growth
    'manufacturing': {
        'pattern': r'worker|operator|assembler|machinist|fabricator|production|manufacturing|factory|plant',
        'base_risk': 75,
        'yearly_increase': 5,
        'variance': 4
    },
    
    # Financial services - moderate to high risk, steady growth
    'financial': {
        'pattern': r'accountant|auditor|banker|finance|financial|broker|trader|investment|banking|insurance|advisor',
        'base_risk': 45,
        'yearly_increase': 9,
        'variance': 6
    },
    
    # Marketing and PR - moderate risk, increasing over time
    'marketing': {
        'pattern': r'marketing|advertiser|PR|public relations|brand|communications|media|social media|content',
        'base_risk': 40,
        'yearly_increase': 8,
        'variance': 7
    },
    
    # Customer service - high initial risk, rapid growth
    'customer_service': {
        'pattern': r'customer service|support|helpdesk|call center|representative|agent',
        'base_risk': 60,
        'yearly_increase': 10,
        'variance': 5
    },
    
    # Human resources - moderate risk with steady growth
    'hr': {
        'pattern': r'HR|human resources|recruiter|talent|personnel|benefits|compensation',
        'base_risk': 40,
        'yearly_increase': 7,
        'variance': 5
    },
    
    # Construction and trades - lower initial risk but growing
    'construction': {
        'pattern': r'construction|builder|carpenter|electrician|plumber|roofer|contractor|mason|painter|tradesperson',
        'base_risk': 25,
        'yearly_increase': 6,
        'variance': 4
    }
}

# Default category for unmatched jobs
DEFAULT_CATEGORY = {
    'base_risk': 35,
    'yearly_increase': 7,
    'variance': 6
}

def extract_keywords(text):
    """Extract keywords from a block of text."""
    # Simple keyword extraction based on frequency
    words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())
    # Filter common words
    common_words = {'with', 'that', 'have', 'this', 'will', 'from', 'they', 'their', 'when', 'what', 'more', 'about'}
    keywords = [word for word in words if word not in common_words]
    return Counter(keywords).most_common(10)

def extract_sentiment_indicators(text, job_title):
    """Extract sentiment indicators related to automation and AI impact."""
    positive_pattern = r'(?i)(?:less vulnerable|resistant|immune|safe from|low risk|unlikely|not susceptible|human advantage|creative|interpersonal|complex|judgment)'
    negative_pattern = r'(?i)(?:vulnerable|susceptible|at risk|high risk|likely|threatened|replaced|automated|repetitive|routine|obsolete)'
    
    job_pattern = re.escape(job_title.lower())
    
    # Look for sentences containing the job title
    sentences = re.split(r'(?<=[.!?])\s+', text)
    job_sentences = [s for s in sentences if re.search(job_pattern, s.lower())]
    
    positive_count = 0
    negative_count = 0
    
    for sentence in job_sentences:
        positive_count += len(re.findall(positive_pattern, sentence))
        negative_count += len(re.findall(negative_pattern, sentence))
    
    # Calculate sentiment score (-1 to 1)
    total = positive_count + negative_count
    if total == 0:
        return 0
    return (positive_count - negative_count) / total

def determine_risk_factors(job_title, data_sources):
    """Determine risk factors based on research data."""
    # Handle different formats of data_sources
    all_text = ""
    if isinstance(data_sources, dict):
        # If it's a dictionary, just get all values as text
        all_text = ' '.join(str(value) for value in data_sources.values())
    elif isinstance(data_sources, list):
        # For list format, handle different possible structures
        for item in data_sources:
            if isinstance(item, tuple) and len(item) >= 2:
                # For tuples, use the second element as the data
                all_text += ' ' + str(item[1])
            elif isinstance(item, str):
                # For plain strings, use the string directly
                all_text += ' ' + item
            elif isinstance(item, dict):
                # For dictionaries, join all values
                all_text += ' ' + ' '.join(str(v) for v in item.values())
    else:
        # For any other format, convert to string
        all_text = str(data_sources)
    
    # Extract keywords and sentiment
    keywords = extract_keywords(all_text)
    sentiment = extract_sentiment_indicators(all_text, job_title)
    
    # Determine job category - check for more specific categories first
    job_category = None
    
    # Special handling for software engineer and similar technical roles
    # Check for specific categories first, then fall back to more general categories
    specific_patterns = {
        'software engineer': ['computing', 'technical'],
        'developer': ['computing', 'technical'],
        'programmer': ['computing', 'technical'],
        'data scientist': ['data_science', 'technical'],
        'data analyst': ['data_science', 'technical'],
        'machine learning': ['data_science', 'technical']
    }
    
    # First, try to match specific job titles
    for specific_job, categories_to_check in specific_patterns.items():
        if specific_job in job_title.lower():
            # Check each potential category in order
            for potential_category in categories_to_check:
                if re.search(JOB_CATEGORIES[potential_category]['pattern'], job_title.lower()):
                    job_category = potential_category
                    break
            if job_category:  # If found a match, no need to continue
                break
    
    # If no specific match found, check all categories
    if job_category is None:
        for category, info in JOB_CATEGORIES.items():
            if re.search(info['pattern'], job_title.lower()):
                job_category = category
                break
    
    # Use default if no match
    if job_category is None:
        category_info = DEFAULT_CATEGORY
    else:
        category_info = JOB_CATEGORIES[job_category]
    
    # Adjust base risk based on sentiment
    sentiment_adjustment = -20 * sentiment  # -20 to +20 adjustment
    base_risk = max(5, min(95, category_info['base_risk'] + sentiment_adjustment))
    
    # Determine yearly increase
    yearly_increase = category_info['yearly_increase']
    variance = category_info['variance']
    
    # Additional risk adjustments based on specific job title terms
    if any(term in job_title.lower() for term in ['ai', 'artificial intelligence', 'machine learning', 'ml']):
        # Roles directly working with AI have different dynamics
        base_risk = max(5, base_risk - 10)  # Slightly lower initial risk
        yearly_increase = yearly_increase * 0.8  # Slower increase
    
    if any(term in job_title.lower() for term in ['senior', 'lead', 'principal', 'staff']):
        # Senior roles tend to have higher job security initially
        base_risk = max(5, base_risk - 5)  # Slightly lower initial risk
    
    # For very new/emerging tech roles
    if any(term in job_title.lower() for term in ['blockchain', 'crypto', 'quantum', 'web3']):
        variance = variance + 3  # More uncertainty in projections
    
    return {
        'job_category': job_category,
        'base_risk': base_risk,
        'yearly_increase': yearly_increase,
        'variance': variance,
        'keywords': keywords,
        'sentiment': sentiment
    }

def calculate_risk_levels(risk_values):
    """Convert risk percentages to text descriptions."""
    risk_levels = []
    
    for risk in risk_values:
        if risk < 30:
            risk_levels.append("Low")
        elif risk < 60:
            risk_levels.append("Moderate")
        elif risk < 85:
            risk_levels.append("High")
        else:
            risk_levels.append("Very High")
    
    return risk_levels

def process_job_data(job_title, data_sources):
    """Process job data from multiple sources to determine displacement risk."""
    # Extract risk factors from research data
    risk_factors = determine_risk_factors(job_title, data_sources)
    
    # Calculate risk values for years 1-5
    base_risk = risk_factors['base_risk']
    yearly_increase = risk_factors['yearly_increase']
    variance = risk_factors['variance']
    
    risk_values = []
    for year in range(1, 6):
        # Calculate risk with diminishing returns for higher years
        # Add small random variation to make the graph more natural
        variation = np.random.normal(0, variance)
        year_factor = 1 - (0.1 * (year - 1))  # Diminishing effect of yearly increase
        risk = min(98, base_risk + (yearly_increase * year * year_factor) + variation)
        risk = max(2, risk)  # Ensure at least 2% risk
        risk_values.append(round(risk, 1))
    
    # Get text descriptions of risk levels
    risk_levels = calculate_risk_levels(risk_values)
    
    # Extract sources used
    source_names = ""
    if isinstance(data_sources, list):
        # Get source names safely
        source_list = []
        for item in data_sources:
            if isinstance(item, tuple) and len(item) >= 1:
                source_list.append(str(item[0]))
        source_names = ", ".join(source_list)
    elif isinstance(data_sources, dict):
        source_names = ", ".join(str(k) for k in data_sources.keys())
    
    # Build factors narrative
    job_category = risk_factors['job_category'] or "general"
    sentiment = risk_factors['sentiment']
    keywords = risk_factors['keywords']
    
    factors_narrative = f"""
    **Category Analysis**: This assessment classifies '{job_title}' in the {job_category.replace('_', ' ')} category.
    
    **Key Research Indicators**:
    - Overall sentiment in research: {"Positive (research suggests lower vulnerability)" if sentiment > 0 else "Negative (research suggests higher vulnerability)" if sentiment < 0 else "Neutral"}
    - Most relevant terms from research: {', '.join([kw for kw, _ in keywords[:5]])}
    
    **Primary Risk Factors**:
    """
    
    # Add specific risk factors based on job category and job title
    # Food service and culinary roles
    if 'cook' in job_title.lower() or 'chef' in job_title.lower() or 'culinary' in job_title.lower():
        factors_narrative += "- Routine food preparation tasks vulnerable to automation\n"
        factors_narrative += "- Emerging robotic systems for standardized cooking\n"
        factors_narrative += "- Automated ingredient processing and measurement\n"
        factors_narrative += "- Retention of creative and sensory aspects that resist automation\n"
        factors_narrative += "- Human judgment still required for quality control and adaptation\n"
        
    # Administrative and service roles
    elif job_category in ['administrative', 'service', 'transportation', 'manufacturing', 'customer_service']:
        factors_narrative += "- High proportion of routine, predictable tasks\n"
        factors_narrative += "- Limited requirement for complex decision-making\n"
        factors_narrative += "- Tasks are well-structured and rule-based\n"
        factors_narrative += "- Increasing capabilities of AI in process automation\n"
    
    # Technical and analytical roles (general)
    elif job_category in ['technical', 'financial', 'marketing']:
        factors_narrative += "- Many analytical tasks that can be algorithmic\n"
        factors_narrative += "- Increasing capabilities of AI in data processing\n"
        factors_narrative += "- Growing automation of routine analysis\n"
        factors_narrative += "- Retention of complex strategic and creative elements\n"
    
    # Software and computing specific factors
    elif (job_category in ['computing', 'data_science'] or 
        re.search(r'\bsoftware\s+engineer\b', job_title.lower()) or
        re.search(r'\bdeveloper\b', job_title.lower()) or
        re.search(r'\bprogrammer\b', job_title.lower())):
        
        factors_narrative += "- AI code generation tools impacting routine coding tasks\n"
        factors_narrative += "- Automated testing and quality assurance advancements\n"
        factors_narrative += "- Increasingly sophisticated algorithm development tools\n"
        factors_narrative += "- Growing capabilities in automated system design\n"
        factors_narrative += "- Persistent need for complex problem-solving and system architecture skills\n"
    
    # Transportation roles
    elif 'driver' in job_title.lower() or 'transportation' in job_title.lower() or 'delivery' in job_title.lower():
        factors_narrative += "- Autonomous vehicle technology advancing rapidly\n"
        factors_narrative += "- Route optimization and navigation increasingly automated\n"
        factors_narrative += "- Self-driving technology implementation growing in controlled environments\n"
        factors_narrative += "- Complex urban environments still challenging for full automation\n"
        factors_narrative += "- Regulatory and safety concerns affecting adoption timeline\n"
    
    # Retail and sales
    elif 'retail' in job_title.lower() or 'sales' in job_title.lower() or 'cashier' in job_title.lower():
        factors_narrative += "- Automated checkout and payment systems expanding\n"
        factors_narrative += "- Inventory management increasingly automated\n"
        factors_narrative += "- Customer service aspects requiring human touch\n"
        factors_narrative += "- Complex customer interactions still requiring human empathy\n"
        factors_narrative += "- Specialized product knowledge valuable in certain domains\n"
    
    # Healthcare roles
    elif job_category in ['healthcare'] or any(term in job_title.lower() for term in ['doctor', 'nurse', 'physician', 'medical']):
        factors_narrative += "- AI diagnostic tools enhancing but not replacing human judgment\n"
        factors_narrative += "- Medical image analysis increasingly automated\n"
        factors_narrative += "- Patient care aspects requiring high empathy and human touch\n"
        factors_narrative += "- Complex decision-making in unpredictable medical situations\n"
        factors_narrative += "- Ethical considerations requiring human oversight\n"
    
    # Creative and education roles
    elif job_category in ['creative', 'education']:
        factors_narrative += "- Strong human elements (empathy, creativity, motivation)\n"
        factors_narrative += "- Complex decision-making in unpredictable environments\n"
        factors_narrative += "- High social intelligence requirements\n"
        factors_narrative += "- AI tools enhancing rather than replacing core functions\n"
    
    # Management roles
    elif job_category in ['management']:
        factors_narrative += "- Strategic decision-making requiring human judgment\n"
        factors_narrative += "- Leadership and team development demanding human touch\n"
        factors_narrative += "- AI enhancing data-driven decisions but not replacing leadership\n"
        factors_narrative += "- Complex stakeholder management requiring social intelligence\n"
    
    # Catch-all for other jobs
    else:
        factors_narrative += "- Degree of routine, predictable tasks in the role\n"
        factors_narrative += "- Level of complex decision-making required\n"
        factors_narrative += "- Balance of technical vs. human-centered skills\n"
        factors_narrative += "- Current state of AI advancement in the domain\n"
    
    # Create a summary based on the risk levels
    final_risk = risk_levels[4]  # Year 5 risk level
    initial_risk = risk_levels[0]  # Year 1 risk level
    
    summary = f"Based on current research and industry trends, the role of '{job_title}' shows a {initial_risk.lower()} initial risk of AI displacement, "
    
    if initial_risk == final_risk:
        summary += f"which remains relatively stable over the 5-year projection period. "
    elif risk_values[4] - risk_values[0] > 30:
        summary += f"but this increases significantly to a {final_risk.lower()} risk by year 5. "
    else:
        summary += f"gradually shifting to a {final_risk.lower()} risk by year 5. "
    
    # Add job-specific insights
    job_specific_insights = {
        'software engineer': "While routine coding tasks face automation through AI coding assistants, complex system design, architecture, and novel problem-solving remain human strengths. The role is evolving toward higher-level design and AI oversight rather than being eliminated.",
        'developer': "Software development is seeing automation of routine coding and testing, but requirements analysis, architecture design, and innovative problem-solving continue to require human expertise.",
        'data scientist': "Data scientists will likely shift from routine data preparation and basic modeling to more complex problem formulation, ethical AI development, and strategic insights that require domain expertise and creative thinking.",
        'teacher': "Teaching roles will increasingly incorporate AI tools for personalized learning and administrative tasks, but the human elements of mentorship, motivation, and social-emotional learning remain essential.",
        'nurse': "Nursing combines technical skills with essential human elements like empathy and complex judgment in unpredictable situations, making it relatively resilient to full AI displacement.",
        'doctor': "Medical diagnosis is being enhanced by AI tools, but the complex reasoning, empathy, and ethical judgment of physicians remains essential for patient care.",
        'manager': "Management roles are evolving to incorporate AI-assisted decision making, but leadership, interpersonal skills, and strategic thinking remain distinctly human capabilities.",
        'chef': "Culinary roles combine technical skills with creativity and sensory judgment that remains challenging for AI to replicate fully.",
        'cook': "Food preparation combines routine tasks that could be automated with sensory judgment and adaptability that remains challenging for robotic systems.",
        'driver': "Transportation roles face significant automation pressure from autonomous vehicle technology, though complex urban environments and edge cases still present challenges.",
        'retail': "Retail positions are seeing increasing automation in checkout and inventory, though customer service aspects requiring empathy and problem-solving remain more resistant.",
        'writer': "Content creation is seeing significant AI impact, though human creativity, cultural nuance, and original thinking remain differentiating factors.",
        'lawyer': "Legal research and document preparation are increasingly automated, but complex legal reasoning, negotiation, and courtroom representation continue to require human expertise."
    }
    
    # Check for exact and partial matches
    matched = False
    
    # First try exact job title matches
    for job_pattern, insight in job_specific_insights.items():
        if job_title.lower() == job_pattern:
            summary += " " + insight
            matched = True
            break
    
    # If no exact match, try partial matches within the job title
    if not matched:
        for job_pattern, insight in job_specific_insights.items():
            # Make sure we're not matching substrings of words (e.g., "cook" in "cookbook")
            if re.search(r'\b' + re.escape(job_pattern) + r'\b', job_title.lower()):
                summary += " " + insight
                matched = True
                break
    
    # Add general conclusion based on risk level if no specific insight was added
    if not any(job_pattern in job_title.lower() for job_pattern in job_specific_insights):
        if final_risk in ["High", "Very High"]:
            summary += "The data suggests substantial transformation of this role's responsibilities or potential displacement by AI technologies within 5 years. Upskilling in complementary areas would be advisable."
        elif final_risk == "Moderate":
            summary += "While not facing immediate displacement, this role is likely to see significant changes in required skills and daily tasks as AI capabilities evolve."
        else:
            summary += "This role appears relatively resilient to AI displacement in the near term, though some aspects of the job may still be augmented by AI technologies."
    
    # Construct result dictionary
    result = {
        'job_title': job_title,
        'job_category': job_category,
        # Add risk_metrics that's needed by the application
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
        'summary': summary,
        'factors': factors_narrative,
        'sources': source_names if source_names else "Aggregated research on AI job displacement"
    }
    
    return result
