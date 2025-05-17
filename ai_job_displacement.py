import trafilatura
import requests
from bs4 import BeautifulSoup
import re
import random
import time
from data_processor import process_job_data

# Cache to store already processed job titles
job_risk_cache = {}

def get_website_text_content(url):
    """
    Extract text content from a website using trafilatura.
    """
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded)
            return text
        return None
    except Exception as e:
        print(f"Error fetching content from {url}: {str(e)}")
        return None

def search_oxford_research(job_title):
    """
    Search Oxford Martin School research on automation probabilities.
    """
    try:
        url = "https://www.oxfordmartin.ox.ac.uk/publications/the-future-of-employment/"
        content = get_website_text_content(url)
        
        if not content:
            return None
            
        # Extract relevant sections about job automation
        job_related_content = ""
        lines = content.split('\n')
        
        # Create variations of the job title to improve matching
        job_variations = [
            job_title.lower(),
            job_title.lower().replace("engineer", "engineering"),
            job_title.lower().replace("developer", "development"),
            ' '.join(job_title.lower().split()[1:]) if len(job_title.split()) > 1 else job_title.lower()
        ]
        
        # Try to find exact matches first
        for i, line in enumerate(lines):
            line_lower = line.lower()
            if any(variation in line_lower for variation in job_variations):
                # Get context around the mention (5 lines before and after)
                start = max(0, i-5)
                end = min(len(lines), i+5)
                job_related_content += "\n".join(lines[start:end]) + "\n\n"
        
        # If no exact matches, try to find partial matches
        if not job_related_content:
            job_keywords = job_title.lower().split()
            for i, line in enumerate(lines):
                line_lower = line.lower()
                if any(keyword in line_lower for keyword in job_keywords if len(keyword) > 3):
                    # Get context around the mention (5 lines before and after)
                    start = max(0, i-5)
                    end = min(len(lines), i+5)
                    job_related_content += "\n".join(lines[start:end]) + "\n\n"
        
        # Also include general AI automation content
        ai_keywords = ["automation", "artificial intelligence", "machine learning", "job displacement", "ai impact"]
        for i, line in enumerate(lines):
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in ai_keywords):
                # Get context around the mention (3 lines before and after)
                start = max(0, i-3)
                end = min(len(lines), i+3)
                job_related_content += "\n".join(lines[start:end]) + "\n\n"
        
        return job_related_content if job_related_content else None
    except Exception as e:
        print(f"Error in Oxford research search: {str(e)}")
        return None

def search_brookings_research(job_title):
    """
    Search Brookings Institution research on AI and future of work.
    """
    try:
        url = "https://www.brookings.edu/articles/what-jobs-are-affected-by-ai-better-paid-better-educated-workers-face-the-most-exposure/"
        content = get_website_text_content(url)
        
        if not content:
            return None
            
        # Extract relevant sections about job automation
        job_related_content = ""
        
        # Create variations of the job title to improve matching
        job_variations = [
            job_title.lower(),
            job_title.lower().replace("engineer", "engineering"),
            job_title.lower().replace("developer", "development"),
            ' '.join(job_title.lower().split()[1:]) if len(job_title.split()) > 1 else job_title.lower()
        ]
        
        # Split content into paragraphs and search for job title variations
        paragraphs = content.split('\n\n')
        
        # Try exact matches first
        for i, para in enumerate(paragraphs):
            para_lower = para.lower()
            if any(variation in para_lower for variation in job_variations):
                # Get context around the mention (2 paragraphs before and after)
                start = max(0, i-2)
                end = min(len(paragraphs), i+3)
                job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        # If no exact matches, try to find partial matches using keywords
        if not job_related_content:
            job_keywords = job_title.lower().split()
            for i, para in enumerate(paragraphs):
                para_lower = para.lower()
                if any(keyword in para_lower for keyword in job_keywords if len(keyword) > 3):
                    # Get context around the mention
                    start = max(0, i-2)
                    end = min(len(paragraphs), i+3)
                    job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        # Also include general AI impact content
        ai_keywords = ["automation", "artificial intelligence", "machine learning", "job displacement", 
                       "ai impact", "occupational", "employment", "labor market", "workforce"]
        for i, para in enumerate(paragraphs):
            para_lower = para.lower()
            if any(keyword in para_lower for keyword in ai_keywords):
                # Get context around the mention
                start = max(0, i-1)
                end = min(len(paragraphs), i+2)
                job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        return job_related_content if job_related_content else None
    except Exception as e:
        print(f"Error in Brookings research search: {str(e)}")
        return None

def search_mckinsey_research(job_title):
    """
    Search McKinsey Global Institute research on automation and work.
    """
    try:
        url = "https://www.mckinsey.com/featured-insights/future-of-work/jobs-lost-jobs-gained-what-the-future-of-work-will-mean-for-jobs-skills-and-wages"
        content = get_website_text_content(url)
        
        if not content:
            return None
            
        # Extract relevant sections about job automation
        job_related_content = ""
        paragraphs = content.split('\n\n')
        
        # Create variations of the job title to improve matching
        job_variations = [
            job_title.lower(),
            job_title.lower().replace("engineer", "engineering"),
            job_title.lower().replace("developer", "development"),
            ' '.join(job_title.lower().split()[1:]) if len(job_title.split()) > 1 else job_title.lower()
        ]
        
        # Check for specific related roles that might be in the research
        job_categories = {
            "software engineer": ["programmer", "software", "code", "computing", "tech worker", "IT professional"],
            "developer": ["programmer", "software", "code", "computing", "tech worker", "IT professional"],
            "data scientist": ["analyst", "data", "statistics", "analytical", "tech worker"],
            "marketing": ["advertising", "promotion", "market research", "digital marketing"],
            "healthcare": ["doctor", "nurse", "medical", "health", "patient care"],
            "teacher": ["education", "instructor", "professor", "academic"],
            "finance": ["accounting", "financial", "banker", "investment", "analyst"]
        }
        
        # Add related terms based on job title
        for category, related_terms in job_categories.items():
            if any(term in job_title.lower() for term in category.split()):
                job_variations.extend(related_terms)
        
        # Try exact matches first
        for i, para in enumerate(paragraphs):
            para_lower = para.lower()
            if any(variation in para_lower for variation in job_variations):
                # Get context around the mention
                start = max(0, i-2)
                end = min(len(paragraphs), i+3)
                job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        # If no exact matches, try to find partial matches
        if not job_related_content:
            job_keywords = job_title.lower().split()
            for i, para in enumerate(paragraphs):
                para_lower = para.lower()
                if any(keyword in para_lower for keyword in job_keywords if len(keyword) > 3):
                    # Get context around the mention
                    start = max(0, i-2)
                    end = min(len(paragraphs), i+3)
                    job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        # Include general automation content related to occupations and skills
        ai_keywords = ["automation", "artificial intelligence", "job displacement", "technological change", 
                      "occupations", "jobs", "skills", "workforce", "labor demand", "future of work"]
        for i, para in enumerate(paragraphs):
            para_lower = para.lower()
            if any(keyword in para_lower for keyword in ai_keywords):
                # Get context around the mention
                start = max(0, i-1)
                end = min(len(paragraphs), i+2)
                job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        return job_related_content if job_related_content else None
    except Exception as e:
        print(f"Error in McKinsey research search: {str(e)}")
        return None

def search_world_economic_forum(job_title):
    """
    Search World Economic Forum research on future of jobs.
    """
    try:
        url = "https://www.weforum.org/publications/the-future-of-jobs-report-2023/"
        content = get_website_text_content(url)
        
        if not content:
            return None
        
        # Extract relevant sections about job displacement
        job_related_content = ""
        paragraphs = content.split('\n\n')
        
        # Create variations of the job title to improve matching
        job_variations = [
            job_title.lower(),
            job_title.lower().replace("engineer", "engineering"),
            job_title.lower().replace("developer", "development"),
            ' '.join(job_title.lower().split()[1:]) if len(job_title.split()) > 1 else job_title.lower()
        ]
        
        # Add industry-specific variations
        job_industry_map = {
            "software": ["tech sector", "technology industry", "digital economy", "IT sector"],
            "engineer": ["technical professionals", "technical roles", "engineering roles"],
            "data": ["analytics", "big data", "information processing"],
            "marketing": ["digital marketing", "market specialists", "branding"],
            "finance": ["financial services", "banking sector", "investment"],
            "healthcare": ["health sector", "medical profession", "care economy"]
        }
        
        # Add related industry terms
        for key, terms in job_industry_map.items():
            if key in job_title.lower():
                job_variations.extend(terms)
        
        # Try to find exact matches first
        for i, para in enumerate(paragraphs):
            para_lower = para.lower()
            if any(variation in para_lower for variation in job_variations):
                # Get context
                start = max(0, i-2)
                end = min(len(paragraphs), i+3)
                job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        # If no exact matches, try to find partial matches
        if not job_related_content:
            job_keywords = job_title.lower().split()
            for i, para in enumerate(paragraphs):
                para_lower = para.lower()
                if any(keyword in para_lower for keyword in job_keywords if len(keyword) > 3):
                    # Get context
                    start = max(0, i-2)
                    end = min(len(paragraphs), i+3)
                    job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        # Get general information about future jobs and automation
        ai_keywords = [
            "automation", "ai impact", "artificial intelligence", "machine learning", "job displacement", 
            "skills obsolescence", "future of work", "job transformation", "emerging jobs", 
            "declining roles", "skills gap", "digital transformation", "technology adoption",
            "labor market disruption", "workforce transition"
        ]
        
        for i, para in enumerate(paragraphs):
            para_lower = para.lower()
            if any(keyword in para_lower for keyword in ai_keywords):
                # Get context
                start = max(0, i-1)
                end = min(len(paragraphs), i+2)
                job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        return job_related_content if job_related_content else None
    except Exception as e:
        print(f"Error in WEF research search: {str(e)}")
        return None

def search_bls_data(job_title):
    """
    Search Bureau of Labor Statistics for job outlook data.
    """
    try:
        url = f"https://www.bls.gov/ooh/"
        content = get_website_text_content(url)
        
        if not content:
            return None
        
        # Extract relevant sections about job outlook
        job_related_content = ""
        paragraphs = content.split('\n\n')
        
        # Create variations of the job title to improve matching
        job_variations = [
            job_title.lower(),
            job_title.lower().replace("engineer", "engineering"),
            job_title.lower().replace("developer", "development"),
            ' '.join(job_title.lower().split()[1:]) if len(job_title.split()) > 1 else job_title.lower()
        ]
        
        # Map job titles to BLS occupation categories
        bls_occupation_map = {
            "software engineer": ["software developers", "computer occupations", "information technology"],
            "developer": ["software developers", "web developers", "computer occupations"],
            "data scientist": ["computer and information research scientists", "statisticians", "analysts"],
            "marketing": ["marketing specialists", "market research analysts", "advertising", "promotions"],
            "teacher": ["teachers", "education", "instructors", "professors"],
            "doctor": ["physicians", "healthcare practitioners", "medical"],
            "nurse": ["registered nurses", "healthcare practitioners", "nursing"],
            "accountant": ["accountants", "auditors", "financial specialists"],
            "manager": ["management occupations", "managers", "administrative services managers", "project management"],
            "project manager": ["project management specialists", "management occupations", "construction managers"],
            "program manager": ["management occupations", "computer and information systems managers", "program directors"],
            "driver": ["transportation occupations", "drivers", "delivery"]
        }
        
        # Add BLS occupation categories if applicable
        for key, terms in bls_occupation_map.items():
            if key in job_title.lower():
                job_variations.extend(terms)
        
        # Try to find exact matches first
        for i, para in enumerate(paragraphs):
            para_lower = para.lower()
            if any(variation in para_lower for variation in job_variations):
                # Get context around the mention
                start = max(0, i-3)
                end = min(len(paragraphs), i+4)
                job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        # If no exact matches, try to find partial matches
        if not job_related_content:
            job_keywords = job_title.lower().split()
            for i, para in enumerate(paragraphs):
                para_lower = para.lower()
                if any(keyword in para_lower for keyword in job_keywords if len(keyword) > 3):
                    # Get context around the mention
                    start = max(0, i-3)
                    end = min(len(paragraphs), i+4)
                    job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        # Also include general outlook information, automation trends and job growth data
        outlook_keywords = [
            "job outlook", "employment projections", "fastest growing", "declining occupations",
            "automation", "technology impact", "job growth", "median pay", "employment change",
            "occupational outlook", "career outlook", "employment trends", "future demand"
        ]
        
        for i, para in enumerate(paragraphs):
            para_lower = para.lower()
            if any(keyword in para_lower for keyword in outlook_keywords):
                # Get context around the mention
                start = max(0, i-2)
                end = min(len(paragraphs), i+3)
                job_related_content += "\n\n".join(paragraphs[start:end]) + "\n\n"
        
        return job_related_content if job_related_content else None
    except Exception as e:
        print(f"Error in BLS data search: {str(e)}")
        return None

def get_job_displacement_risk(job_title):
    """
    Get AI displacement risk data for a specific job title.
    """
    # Check if we already have this job in cache
    if job_title.lower() in job_risk_cache:
        return job_risk_cache[job_title.lower()]
    
    # Add fallback data for common jobs
    fallback_job_data = {
        'cook': {
            'job_title': 'Cook',
            'data_sources': [("Fallback Data", "Cook job risk analysis")],
            'job_category': 'service',
            'risk_metrics': {
                'year_1_risk': 25.0,
                'year_5_risk': 45.0,
                'year_1_level': 'Low',
                'year_5_level': 'Moderate'
            },
            'risk_factors': {
                'Routine Task Automation': 'High',
                'AI Recipe Generation': 'Moderate',
                'Food Preparation Robotics': 'Moderate',
                'Customer Preference Analysis': 'Low',
                'Supply Chain Management': 'Low'
            },
            'trend': 'Cooking roles face moderate automation risk as kitchen technologies advance, but creative elements and physical dexterity requirements provide some protection. Specialized culinary skills and food innovation abilities will remain valuable.'
        },
        'teacher': {
            'job_title': 'Teacher',
            'data_sources': [("Fallback Data", "Teacher job risk analysis")],
            'job_category': 'education',
            'risk_metrics': {
                'year_1_risk': 10.0,
                'year_5_risk': 25.0,
                'year_1_level': 'Low',
                'year_5_level': 'Low'
            },
            'risk_factors': {
                'Administrative Tasks': 'High',
                'Grading Automation': 'Moderate',
                'Personalized Learning': 'Low',
                'Student Engagement': 'Very Low',
                'Complex Problem Solving': 'Very Low'
            },
            'trend': 'Teaching positions face low displacement risk due to the essential human connection and mentorship aspects of education. While AI will automate administrative tasks and enhance personalized learning, teachers will remain crucial for student development and guidance.'
        }
    }
    
    # Check if we have fallback data for this job
    if job_title.lower() in fallback_job_data:
        fallback_result = fallback_job_data[job_title.lower()]
        job_risk_cache[job_title.lower()] = fallback_result
        return fallback_result
    
    # Start with an error state
    result = {
        'error': 'Unable to find sufficient data for this job title. Please try a more common or specific job title.'
    }
    
    try:
        # Collect data from multiple sources
        data_sources = []
        
        # Search Oxford research
        oxford_data = search_oxford_research(job_title)
        if oxford_data:
            data_sources.append(("Oxford Martin School", oxford_data))
        
        # Search Brookings research
        brookings_data = search_brookings_research(job_title)
        if brookings_data:
            data_sources.append(("Brookings Institution", brookings_data))
        
        # Search McKinsey research
        mckinsey_data = search_mckinsey_research(job_title)
        if mckinsey_data:
            data_sources.append(("McKinsey Global Institute", mckinsey_data))
        
        # Search World Economic Forum data
        wef_data = search_world_economic_forum(job_title)
        if wef_data:
            data_sources.append(("World Economic Forum", wef_data))
        
        # Search BLS data
        bls_data = search_bls_data(job_title)
        if bls_data:
            data_sources.append(("Bureau of Labor Statistics", bls_data))
        
        # If we have at least some data
        if data_sources:
            # Process the collected data
            result = process_job_data(job_title, data_sources)
            
            # Cache the result
            job_risk_cache[job_title.lower()] = result
            
            return result
        else:
            return result  # Return the error state
            
    except Exception as e:
        return {
            'error': f"An error occurred during data processing: {str(e)}"
        }
