"""
Job API Integration - Database Only Version
This module ONLY uses the Neon database populated with real BLS data or fetches
directly from the BLS API if data is not in the database.
NO hardcoded job data or synthetic fallback data is used.
"""

import os
import logging
from typing import Dict, Any, List, Optional

# Attempt to import the core data provider module
try:
    import bls_job_mapper
except ImportError as e:
    logging.critical(f"Failed to import bls_job_mapper: {e}. This module is essential.")
    # Define stubs if bls_job_mapper is missing, so the app can at least report this critical error.
    class bls_job_mapper_stub:
        @staticmethod
        def get_complete_job_data(job_title: str) -> Dict[str, Any]:
            return {"error": f"CRITICAL: bls_job_mapper module not found. Cannot fetch data for {job_title}.", "job_title": job_title, "source": "system_error"}
    bls_job_mapper = bls_job_mapper_stub() # type: ignore

logger = logging.getLogger(__name__)

def generate_employment_trend(current_employment: Optional[int], projected_employment: Optional[int], num_years: int = 6) -> Dict[str, List[Any]]:
    """
    Generate a linear employment trend from current to projected employment.
    Uses a default of 6 years (e.g., 2020-2025 or current year + 5 years).
    If data is insufficient, returns empty lists.

    Args:
        current_employment: Current number of people employed.
        projected_employment: Projected number of people employed.
        num_years: The number of years the trend should span.

    Returns:
        A dictionary with 'years' and 'employment' lists.
    """
    if current_employment is None or projected_employment is None or num_years <= 1:
        logger.warning("Insufficient data for generating employment trend. Returning empty trend.")
        return {"years": [], "employment": []}

    years = list(range(datetime.date.today().year, datetime.date.today().year + num_years))
    employment_values: List[int] = []

    try:
        current_emp_val = int(current_employment)
        projected_emp_val = int(projected_employment)
        
        # Calculate annual change (can be negative if employment is declining)
        # Assuming BLS projections are typically over 10 years, adjust for num_years if different.
        # For simplicity, we'll do a linear interpolation over num_years.
        total_change = projected_emp_val - current_emp_val
        # If num_years is 6, there are 5 intervals.
        annual_change = total_change / (num_years -1) if num_years > 1 else 0


        for i in range(num_years):
            employment_values.append(int(current_emp_val + (annual_change * i)))
            
        logger.info(f"Generated employment trend: {employment_values} over {years}")
        return {"years": years, "employment": employment_values}

    except (ValueError, TypeError) as e:
        logger.error(f"Error generating employment trend due to invalid input types: {e}. Current: {current_employment}, Projected: {projected_employment}")
        return {"years": [], "employment": []}


def get_job_data(job_title: str) -> Dict[str, Any]:
    """
    Get job data ONLY from Neon database (via bls_job_mapper) or BLS API.
    No synthetic or fictional data is used.

    Args:
        job_title: The job title to analyze.

    Returns:
        Dictionary with job data or an error message.
    """
    logger.info(f"Fetching job data for: '{job_title}' using only authentic BLS sources.")

    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL environment variable not set. Cannot connect to Neon database.")
        return {
            "error": "Database configuration error. DATABASE_URL not set.",
            "job_title": job_title,
            "source": "system_error"
        }

    try:
        # Delegate data fetching and core processing to bls_job_mapper
        raw_job_data = bls_job_mapper.get_complete_job_data(job_title)

        if not raw_job_data or "error" in raw_job_data:
            error_message = raw_job_data.get("error", "Unknown error from bls_job_mapper")
            logger.warning(f"Failed to get complete job data for '{job_title}': {error_message}")
            return {
                "error": f"Job title '{job_title}' not found in BLS database or insufficient data after API check. {error_message}",
                "job_title": job_title,
                "source": raw_job_data.get("source", "error_bls_mapper")
            }

        # Ensure occupation code is valid (not "00-0000" unless it's a known aggregate)
        # bls_job_mapper.get_complete_job_data should ideally handle this, but an extra check here is fine.
        occupation_code = raw_job_data.get("occupation_code")
        if not occupation_code or occupation_code == "00-0000":
            logger.warning(f"No valid SOC code found for '{job_title}' by bls_job_mapper. Treating as not found.")
            return {
                "error": f"Job title '{job_title}' could not be mapped to a specific BLS occupation.",
                "job_title": job_title,
                "source": "error_soc_mapping"
            }
        
        logger.info(f"Successfully retrieved raw data for '{job_title}' (SOC: {occupation_code}) from bls_job_mapper.")

        # Transform raw_job_data into the structure expected by the Streamlit app
        current_emp = raw_job_data.get("employment")
        projected_emp = raw_job_data.get("projected_employment")
        trend_data = generate_employment_trend(current_emp, projected_emp)

        formatted_data = {
            "job_title": raw_job_data.get("job_title", job_title), # Use standardized title if available
            "occupation_code": occupation_code,
            "job_category": raw_job_data.get("job_category", "General"),
            "source": "bls_database_or_api", # bls_job_mapper handles the distinction
            "bls_data": {
                "employment": current_emp,
                "employment_change_percent": raw_job_data.get("employment_change_percent"),
                "annual_job_openings": raw_job_data.get("annual_job_openings"),
                "median_wage": raw_job_data.get("median_wage"),
                # Add other BLS fields if app_production needs them here
            },
            "risk_scores": { # For compatibility with app_production.py's gauge chart logic
                "year_1": raw_job_data.get("year_1_risk"),
                "year_5": raw_job_data.get("year_5_risk")
            },
            # Direct access for convenience as app_production.py uses both
            "year_1_risk": raw_job_data.get("year_1_risk"),
            "year_5_risk": raw_job_data.get("year_5_risk"),
            "risk_category": raw_job_data.get("risk_category"),
            "risk_factors": raw_job_data.get("risk_factors", []),
            "protective_factors": raw_job_data.get("protective_factors", []),
            "analysis": raw_job_data.get("analysis", "No detailed analysis available for this occupation."),
            "summary": raw_job_data.get("analysis", "Summary based on BLS category analysis."), # Using analysis as summary
            "trend_data": trend_data,
            "similar_jobs": [], # Omitted as per "authentic BLS data only" - no clear way to derive this dynamically
            "automation_probability": None, # Omitted, not directly from BLS for specific jobs
            "last_updated_from_bls": raw_job_data.get("last_updated") # From bls_job_mapper
        }
        
        # Log missing essential fields for debugging
        for key in ["year_1_risk", "year_5_risk", "risk_category", "job_category"]:
            if formatted_data.get(key) is None:
                logger.warning(f"Essential field '{key}' is missing in formatted_data for '{job_title}'. Raw data: {raw_job_data}")
        if not formatted_data["bls_data"].get("median_wage"):
             logger.warning(f"Median wage data missing for '{job_title}'. Raw data: {raw_job_data}")


        logger.info(f"Successfully formatted data for '{job_title}'.")
        return formatted_data

    except Exception as e:
        logger.error(f"Unexpected error in get_job_data for '{job_title}': {e}", exc_info=True)
        return {
            "error": f"An unexpected error occurred while fetching data for '{job_title}'. Details: {str(e)}",
            "job_title": job_title,
            "source": "system_error"
        }


def get_jobs_comparison_data(job_list: List[str]) -> Dict[str, Any]:
    """
    Get comparison data for multiple jobs using ONLY database/BLS data.
    Each job's data is fetched individually using get_job_data.

    Args:
        job_list: A list of job titles to compare.

    Returns:
        A dictionary where keys are job titles and values are their data (or error).
    """
    logger.info(f"Fetching comparison data for jobs: {job_list}")
    results: Dict[str, Any] = {}
    
    if not isinstance(job_list, list):
        logger.error("Invalid job_list provided for comparison. Must be a list.")
        return {"error": "Invalid input: job_list must be a list."}

    for job_title in job_list:
        if not isinstance(job_title, str) or not job_title.strip():
            logger.warning(f"Skipping invalid job title in comparison list: '{job_title}'")
            results[str(job_title)] = {"error": "Invalid job title provided.", "job_title": str(job_title), "source": "input_error"}
            continue
        
        logger.debug(f"Fetching data for comparison: '{job_title}'")
        job_data = get_job_data(job_title)
        
        # Store the result, whether it's data or an error object
        results[job_title] = job_data
        if "error" in job_data:
            logger.warning(f"Error fetching data for '{job_title}' during comparison: {job_data['error']}")
        else:
            logger.info(f"Successfully fetched data for '{job_title}' for comparison.")
            
    logger.info(f"Finished fetching comparison data for {len(job_list)} jobs.")
    return results

if __name__ == '__main__':
    # Example usage for testing this module directly
    # Ensure DATABASE_URL is set in your environment variables for this to work.
    # And bls_job_mapper.py is in the same directory or Python path.
    logging.basicConfig(level=logging.INFO) # Ensure logs are visible for direct run
    
    test_job_titles = [
        "Software Developer", 
        "Registered Nurse", 
        "Truck Driver", 
        "NonExistentJob123",
        "Accountant"
    ]
    
    logger.info(f"\n--- Testing get_job_data for single titles ---")
    for title in test_job_titles:
        logger.info(f"\nFetching data for: {title}")
        data = get_job_data(title)
        if "error" in data:
            logger.error(f"Error for {title}: {data['error']}")
        else:
            logger.info(f"Data for {title} (SOC: {data.get('occupation_code')}):")
            logger.info(f"  Category: {data.get('job_category')}")
            logger.info(f"  5-Year Risk: {data.get('year_5_risk')}% ({data.get('risk_category')})")
            logger.info(f"  Median Wage: {data.get('bls_data', {}).get('median_wage')}")
            logger.info(f"  Employment Trend (first 3 years): {data.get('trend_data', {}).get('employment', [])[:3]}")
        # Basic check for expected keys
        expected_keys = ["job_title", "occupation_code", "year_5_risk", "bls_data", "trend_data"]
        if not all(key in data for key in expected_keys) and "error" not in data:
             logger.warning(f"Data for {title} might be missing some expected keys.")


    logger.info(f"\n--- Testing get_jobs_comparison_data for multiple titles ---")
    comparison_results = get_jobs_comparison_data(test_job_titles)
    for job_title, data_or_error in comparison_results.items():
        logger.info(f"\nResult for comparison: {job_title}")
        if "error" in data_or_error:
            logger.error(f"  Error: {data_or_error['error']}")
        else:
            logger.info(f"  SOC: {data_or_error.get('occupation_code')}, 5-Year Risk: {data_or_error.get('year_5_risk')}%")
    
    logger.info("\n--- Direct module testing complete ---")
