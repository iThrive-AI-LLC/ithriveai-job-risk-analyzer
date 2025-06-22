"""
Job API Integration - Database Only Version
This module ONLY uses the Neon database populated with real BLS data or fetches
directly from the BLS API if data is not in the database.
NO hardcoded job data or synthetic fallback data is used.
"""

import os
import logging
import datetime # Added for employment trend year calculation
from typing import Dict, Any, List, Optional

# Use shared DB engine from the core database module
import database
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

# ------------------------------------------------------------------
# New helper – full data retrieval from BLS DB / API
# This replicates the functionality that used to live in
# bls_job_mapper.get_complete_job_data so that other modules
# which still import that symbol (e.g. app_production.py) do
# not break.  After we define the function, we monkey-patch it
# into the bls_job_mapper namespace for backward compatibility.
# ------------------------------------------------------------------

def get_complete_job_data(job_title: str) -> Dict[str, Any]:
    """
    End-to-end pipeline that maps an arbitrary job title to an SOC code,
    ensures BLS data is present in the Neon database (fetching from the
    BLS API if necessary), and returns a consolidated dictionary that
    includes BLS statistics plus AI-risk estimates.
    """
    try:
        soc_code, standardized_title, job_category = bls_job_mapper.find_occupation_code(job_title)
        if not soc_code:
            return {
                "error": f"No BLS SOC code could be mapped to '{job_title}'. "
                         "Please try a more specific title.",
                "job_title": job_title,
                "source": "soc_mapping_failure"
            }

        # 1) Try cache / DB first
        bls_row = bls_job_mapper.get_bls_data_from_db(soc_code)

        # 2) If missing, attempt to fetch & persist
        if bls_row is None:
            db_eng = bls_job_mapper.get_db_engine()
            if db_eng is None:
                return {
                    "error": "Database engine unavailable while attempting to fetch BLS data.",
                    "job_title": job_title,
                    "source": "db_engine_unavailable"
                }
            success, msg = bls_job_mapper.fetch_and_process_soc_data(soc_code,
                                                                     standardized_title,
                                                                     db_eng)
            if not success:
                return {
                    "error": f"BLS API fetch failed for '{job_title}' (SOC {soc_code}). "
                             f"Details: {msg}",
                    "job_title": job_title,
                    "source": "bls_api_failure"
                }
            bls_row = bls_job_mapper.get_bls_data_from_db(soc_code)

        if bls_row is None:
            return {
                "error": f"No BLS data available after API attempt for '{job_title}'.",
                "job_title": job_title,
                "source": "bls_data_missing"
            }

        # 3) Derive AI-risk metrics
        risk_info = bls_job_mapper.calculate_ai_risk_from_category(job_category, soc_code)

        return {
            # Core identifiers
            "occupation_code": soc_code,
            "job_title": standardized_title,
            "job_category": job_category,
            "source": "bls_database",

            # Raw BLS stats (flatten a few key ones)
            "employment": bls_row.get("current_employment"),
            "projected_employment": bls_row.get("projected_employment"),
            "employment_change_percent": bls_row.get("percent_change"),
            "annual_job_openings": bls_row.get("annual_job_openings"),
            "median_wage": bls_row.get("median_wage"),
            "mean_wage": bls_row.get("mean_wage"),

            # Risk outputs
            "year_1_risk": risk_info.get("year_1_risk"),
            "year_5_risk": risk_info.get("year_5_risk"),
            "risk_category": risk_info.get("risk_category"),
            "risk_factors": risk_info.get("risk_factors"),
            "protective_factors": risk_info.get("protective_factors"),

            # Simple narrative fields – could be enhanced later
            "analysis": (
                f"{standardized_title} – AI displacement risk is "
                f"{risk_info.get('risk_category').lower()} at "
                f"{risk_info.get('year_5_risk')} % over five years."
            ),
            "summary": (
                f"{risk_info.get('year_5_risk')} % five-year AI risk "
                f"({risk_info.get('risk_category')})."
            ),

            # Pass full raw row for downstream use
            "bls_data": bls_row,
            "last_updated": bls_row.get("last_updated")
        }

    except Exception as exc:
        logger.exception("Unhandled error in get_complete_job_data")
        return {
            "error": f"System error when processing '{job_title}': {exc}",
            "job_title": job_title,
            "source": "system_exception"
        }

# Expose for legacy callers (e.g. app_production via bls_job_mapper)
try:
    setattr(bls_job_mapper, "get_complete_job_data", get_complete_job_data)
except Exception:  # bls_job_mapper might be a stub in some failure modes
    pass

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

    # Generate years dynamically, e.g., from 3 years ago to 2 years in the future for a 6-year span
    # This makes the trend somewhat centered around the current time.
    # For a 6-year trend: current_year-3, current_year-2, current_year-1, current_year, current_year+1, current_year+2
    # The app_production.py seems to use a fixed range like 2020-2025.
    # For consistency with that, let's use a fixed recent range if specific BLS projection years aren't available.
    # However, it's better to use actual projection years from BLS if `bls_job_mapper` provides them.
    # For now, let's keep it simple and use a recent fixed range as in app_production.py
    # A more robust solution would involve `bls_job_mapper` providing the base and projection years.
    
    # Using a fixed recent 6-year span as often seen in app examples.
    # This part might need adjustment if BLS data has specific projection year ranges.
    start_year_for_trend = datetime.date.today().year - 3 # Example: 2024-3 = 2021
    years = list(range(start_year_for_trend, start_year_for_trend + num_years)) # e.g., 2021, 2022, 2023, 2024, 2025, 2026

    employment_values: List[int] = []

    try:
        current_emp_val = int(current_employment)
        projected_emp_val = int(projected_employment)
        
        # Calculate annual change (can be negative if employment is declining)
        # This assumes a linear trend over the num_years period.
        # If BLS projections are typically over 10 years, and we show 6, this is an interpolation.
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
    No synthetic or fictional data is used. This function ensures that if "engineer"
    is searched, it does not default to "project manager" unless bls_job_mapper itself
    incorrectly maps it (which would be an issue in bls_job_mapper.py).

    Args:
        job_title: The job title to analyze.

    Returns:
        Dictionary with job data or an error message.
    """
    logger.info(f"Fetching job data for: '{job_title}' using only authentic BLS sources via bls_job_mapper.")

    # Ensure we have an initialised engine; this avoids each module
    # creating its own connection pool and keeps DB handling centralised.
    if database.engine is None:
        logger.error("Shared database engine is not initialised. Cannot fetch BLS data.")
        return {
            "error": "Database configuration error. Engine not initialised.",
            "job_title": job_title,
            "source": "system_error_db_config"
        }

    try:
        # Delegate data fetching and core processing to bls_job_mapper
        # bls_job_mapper is responsible for querying BLS API/DB and all core data processing, including AI risk.
        raw_job_data = bls_job_mapper.get_complete_job_data(job_title)

        if not raw_job_data: # Should not happen if bls_job_mapper is robust, but check
            logger.error(f"bls_job_mapper.get_complete_job_data returned None for '{job_title}'.")
            return {
                "error": f"Failed to retrieve any data for '{job_title}' from the BLS data provider.",
                "job_title": job_title,
                "source": "error_bls_mapper_nodata"
            }

        if "error" in raw_job_data:
            logger.warning(f"Failed to get complete job data for '{job_title}' from bls_job_mapper: {raw_job_data['error']}")
            # Propagate the error from bls_job_mapper
            return {
                "error": raw_job_data['error'], # More specific error from mapper
                "job_title": job_title,
                "source": raw_job_data.get("source", "error_bls_mapper")
            }

        # Ensure occupation code is valid (not "00-0000" unless it's a known aggregate, which bls_job_mapper should clarify)
        occupation_code = raw_job_data.get("occupation_code")
        if not occupation_code or occupation_code == "00-0000":
            # This condition means bls_job_mapper could not find a specific SOC code.
            logger.warning(f"No specific BLS occupation code found for '{job_title}' by bls_job_mapper. Treating as not found.")
            return {
                "error": f"Job title '{job_title}' could not be mapped to a specific BLS occupation. Please try a more standard job title.",
                "job_title": job_title,
                "source": "error_soc_mapping"
            }
        
        logger.info(f"Successfully retrieved raw data for '{job_title}' (SOC: {occupation_code}) from bls_job_mapper.")

        # Transform raw_job_data from bls_job_mapper into the structure expected by the Streamlit app (app_production.py)
        current_emp = raw_job_data.get("employment")
        projected_emp = raw_job_data.get("projected_employment")
        
        # Generate employment trend based on data from bls_job_mapper
        trend_data = generate_employment_trend(current_emp, projected_emp)

        # Prepare the bls_data sub-dictionary as expected by app_production.py
        bls_data_for_app = {
            "employment": current_emp,
            "employment_change_percent": raw_job_data.get("employment_change_percent"),
            "annual_job_openings": raw_job_data.get("annual_job_openings"),
            "median_wage": raw_job_data.get("median_wage"),
            # Include other BLS fields if app_production.py expects them here
            "occupation_code": occupation_code, # Often useful to have it here too
            "standardized_title": raw_job_data.get("job_title", job_title),
            "job_category": raw_job_data.get("job_category", "General"),
        }
        if raw_job_data.get("bls_data"): # If bls_job_mapper provides a nested bls_data, merge it carefully
            bls_data_for_app.update(raw_job_data["bls_data"])


        formatted_data = {
            "job_title": raw_job_data.get("job_title", job_title), # Use standardized title from mapper if available
            "occupation_code": occupation_code,
            "job_category": raw_job_data.get("job_category", "General"),
            "source": raw_job_data.get("source", "bls_database_or_api"), # Source from bls_job_mapper
            
            "bls_data": bls_data_for_app,
            
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
            
            "analysis": raw_job_data.get("analysis", "No detailed analysis available for this occupation from BLS data."),
            # Summary can be same as analysis or a shorter version if mapper provides it
            "summary": raw_job_data.get("summary") or raw_job_data.get("analysis", "Summary based on BLS category analysis."),
            
            "trend_data": trend_data,
            
            "similar_jobs": [], # Per instruction: authentic BLS data only. Deriving similar jobs authentically is complex.
                                # If bls_job_mapper could provide this authentically, it would be passed through.
            
            "automation_probability": None, # Not a direct BLS metric; omit or set to None.
            
            "last_updated_from_bls": raw_job_data.get("last_updated") # Timestamp from bls_job_mapper
        }
        
        # Log missing essential fields for debugging if they weren't caught by bls_job_mapper
        essential_fields_check = {
            "year_1_risk": formatted_data.get("year_1_risk"),
            "year_5_risk": formatted_data.get("year_5_risk"),
            "risk_category": formatted_data.get("risk_category"),
            "job_category": formatted_data.get("job_category"),
            "median_wage": bls_data_for_app.get("median_wage")
        }
        for key, value in essential_fields_check.items():
            if value is None:
                logger.warning(f"Essential field '{key}' is missing or None in final formatted_data for '{job_title}'. This might indicate issues in bls_job_mapper's output or data availability.")

        logger.info(f"Successfully formatted data for '{job_title}' (SOC: {occupation_code}).")
        return formatted_data

    except Exception as e:
        logger.error(f"Unexpected error in get_job_data for '{job_title}': {e}", exc_info=True)
        return {
            "error": f"An unexpected system error occurred while fetching data for '{job_title}'. Details: {str(e)}",
            "job_title": job_title,
            "source": "system_error_integration_module"
        }


def get_jobs_comparison_data(job_list: List[str]) -> Dict[str, Any]:
    """
    Get comparison data for multiple jobs using ONLY database/BLS data.
    Each job's data is fetched individually using get_job_data.

    Args:
        job_list: A list of job titles to compare.

    Returns:
        A dictionary where keys are job titles and values are their data (or error object).
    """
    logger.info(f"Fetching comparison data for jobs: {job_list}")
    results: Dict[str, Any] = {}
    
    if not isinstance(job_list, list):
        logger.error("Invalid job_list provided for comparison. Must be a list.")
        # Return a structure that indicates an error with the input itself
        return {"error_input": "Invalid input: job_list must be a list of strings."}

    for job_title in job_list:
        if not isinstance(job_title, str) or not job_title.strip():
            logger.warning(f"Skipping invalid job title in comparison list: '{job_title}'")
            # Use a consistent key for the job title, even if it's problematic
            error_key = str(job_title) if job_title is not None else "invalid_title_entry"
            results[error_key] = {"error": "Invalid job title provided (empty or not a string).", "job_title": str(job_title), "source": "input_error"}
            continue
        
        logger.debug(f"Fetching data for comparison: '{job_title}'")
        job_data_result = get_job_data(job_title) # This now returns the formatted data or an error object
        
        # Store the result, whether it's data or an error object, under the original job title key
        results[job_title] = job_data_result
        
        if "error" in job_data_result:
            logger.warning(f"Error fetching data for '{job_title}' during comparison: {job_data_result['error']}")
        else:
            logger.info(f"Successfully fetched data for '{job_title}' for comparison.")
            
    logger.info(f"Finished fetching comparison data for {len(job_list)} jobs.")
    return results

if __name__ == '__main__':
    # Example usage for testing this module directly
    # Ensure DATABASE_URL and BLS_API_KEY are set in your environment variables for this to work.
    # And bls_job_mapper.py is in the same directory or Python path.
    
    # Setup basic logging for standalone testing
    if not logger.handlers:
        _test_handler = logging.StreamHandler()
        _test_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
        _test_handler.setFormatter(_test_formatter)
        logger.addHandler(_test_handler)
        logger.setLevel(logging.INFO) # Set to INFO or DEBUG for detailed output
    
    test_job_titles_single = [
        "Software Developer", 
        "Registered Nurse", 
        "Truck Driver", 
        "NonExistentJob123", # Expected to fail gracefully
        "Accountant",
        "Engineer" # Test the "engineer" case specifically
    ]
    
    logger.info(f"\n--- Testing get_job_data for single titles ---")
    for title in test_job_titles_single:
        logger.info(f"\nFetching data for: '{title}'")
        data = get_job_data(title)
        if "error" in data:
            logger.error(f"Error for '{title}': {data['error']} (Source: {data.get('source')})")
        else:
            logger.info(f"Data for '{data.get('job_title', title)}' (SOC: {data.get('occupation_code')}):")
            logger.info(f"  Source: {data.get('source')}")
            logger.info(f"  Category: {data.get('job_category')}")
            logger.info(f"  5-Year Risk: {data.get('year_5_risk')}% ({data.get('risk_category')})")
            logger.info(f"  Median Wage: {data.get('bls_data', {}).get('median_wage')}")
            logger.info(f"  Employment Trend (first 3 years of {len(data.get('trend_data', {}).get('years',[]))} years): {data.get('trend_data', {}).get('employment', [])[:3]}")
            logger.info(f"  Risk Factors (first 2): {data.get('risk_factors', [])[:2]}")
        
        # Basic check for expected keys if no error
        if "error" not in data:
            expected_keys = ["job_title", "occupation_code", "year_5_risk", "bls_data", "trend_data", "risk_factors", "analysis"]
            missing_keys = [key for key in expected_keys if key not in data or data[key] is None]
            if missing_keys:
                 logger.warning(f"Data for '{title}' might be missing some expected keys: {missing_keys}")
        print("-" * 20)


    logger.info(f"\n--- Testing get_jobs_comparison_data for multiple titles ---")
    test_job_titles_comparison = [
        "Software Developer", 
        "Web Developer", 
        "NonExistentJob123",
        "Engineer" # Check engineer in comparison context
    ]
    comparison_results = get_jobs_comparison_data(test_job_titles_comparison)
    for job_title_key, data_or_error_val in comparison_results.items():
        logger.info(f"\nResult for comparison job: '{job_title_key}'")
        if "error" in data_or_error_val:
            logger.error(f"  Error: {data_or_error_val['error']} (Source: {data_or_error_val.get('source')})")
        else:
            logger.info(f"  Standardized Title: {data_or_error_val.get('job_title')}")
            logger.info(f"  SOC: {data_or_error_val.get('occupation_code')}, 5-Year Risk: {data_or_error_val.get('year_5_risk')}%")
            logger.info(f"  Source: {data_or_error_val.get('source')}")
    
    logger.info("\n--- Direct module testing complete ---")
