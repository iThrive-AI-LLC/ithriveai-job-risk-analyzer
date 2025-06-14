"""
Database Fallback Module (db_fallback.py)

This module provides fallback functions for database operations when the primary
database connection is unavailable.

It strictly adheres to the requirement of NOT using any fictional or synthetic data.
Instead, it returns empty results or error indicators, logging that the database
is inaccessible.
"""

import logging
import datetime

# Configure logging for this module
logger = logging.getLogger("db_fallback")
logger.setLevel(logging.WARNING)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

DB_UNAVAILABLE_MESSAGE = "Database is unavailable. Returning empty/error state. No data will be saved or retrieved."

def save_job_search(job_title: str, risk_data: dict) -> bool:
    """
    Fallback for saving job search data. Does not save any data.
    
    Args:
        job_title (str): The job title that was searched.
        risk_data (dict): Dictionary containing risk assessment data.
        
    Returns:
        bool: Always False, indicating the save operation failed.
    """
    logger.warning(f"Attempted to save job search for '{job_title}'. {DB_UNAVAILABLE_MESSAGE}")
    return False

def get_popular_searches(limit: int = 5) -> list:
    """
    Fallback for getting popular job searches. Returns an empty list.
    
    Args:
        limit (int): Maximum number of results to return.
        
    Returns:
        list: An empty list.
    """
    logger.warning(f"Attempted to get popular searches. {DB_UNAVAILABLE_MESSAGE}")
    return []

def get_highest_risk_jobs(limit: int = 5) -> list:
    """
    Fallback for getting jobs with the highest average year 5 risk. Returns an empty list.
    
    Args:
        limit (int): Maximum number of results to return.
        
    Returns:
        list: An empty list.
    """
    logger.warning(f"Attempted to get highest risk jobs. {DB_UNAVAILABLE_MESSAGE}")
    return []

def get_lowest_risk_jobs(limit: int = 5) -> list:
    """
    Fallback for getting jobs with the lowest average year 5 risk. Returns an empty list.
    
    Args:
        limit (int): Maximum number of results to return.
        
    Returns:
        list: An empty list.
    """
    logger.warning(f"Attempted to get lowest risk jobs. {DB_UNAVAILABLE_MESSAGE}")
    return []

def get_recent_searches(limit: int = 10) -> list:
    """
    Fallback for getting recent job searches. Returns an empty list.
    
    Args:
        limit (int): Maximum number of results to return.
        
    Returns:
        list: An empty list.
    """
    logger.warning(f"Attempted to get recent searches. {DB_UNAVAILABLE_MESSAGE}")
    return []

# You can add other database-related functions here if needed,
# ensuring they also return empty/error states and log appropriately.

if __name__ == "__main__":
    # Example usage (for testing the fallback module directly)
    logger.info("Testing db_fallback module...")
    
    # Test save_job_search
    save_result = save_job_search("Test Job", {"year_1_risk": 0.1, "year_5_risk": 0.5})
    logger.info(f"save_job_search result: {save_result} (Expected: False)")
    
    # Test get_popular_searches
    popular_searches = get_popular_searches()
    logger.info(f"get_popular_searches result: {popular_searches} (Expected: [])")
    
    # Test get_highest_risk_jobs
    highest_risk = get_highest_risk_jobs()
    logger.info(f"get_highest_risk_jobs result: {highest_risk} (Expected: [])")
    
    # Test get_lowest_risk_jobs
    lowest_risk = get_lowest_risk_jobs()
    logger.info(f"get_lowest_risk_jobs result: {lowest_risk} (Expected: [])")
    
    # Test get_recent_searches
    recent_searches = get_recent_searches()
    logger.info(f"get_recent_searches result: {recent_searches} (Expected: [])")
    
    logger.info("db_fallback module test complete.")
