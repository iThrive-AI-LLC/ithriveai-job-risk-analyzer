"""
AI Job Displacement Module (ai_job_displacement.py)

This module provides a simplified interface for assessing AI job displacement risk,
primarily by acting as a wrapper or adapter for the `bls_job_mapper.py` module.
It ensures that all data is sourced from authentic BLS information processed by `bls_job_mapper`.
Web scraping and non-BLS data processing functionalities from any previous versions
have been removed to align with the "real BLS data only" requirement.
"""

import logging

# Attempt to import the core data provider module
try:
    import bls_job_mapper
    MODULE_IMPORT_SUCCESS = True
except ImportError as e:
    MODULE_IMPORT_SUCCESS = False
    # This error will be logged, and functions will return error states.
    logging.basicConfig(level=logging.ERROR) # Basic logging if main logger isn't set up
    logging.critical(f"ai_job_displacement: CRITICAL IMPORT ERROR: Failed to import bls_job_mapper: {e}. This module is essential.")

logger = logging.getLogger(__name__)

def _calculate_risk_level_text(risk_percentage: float | None) -> str:
    """
    Converts a risk percentage to a textual description (Low, Moderate, High, Very High).
    """
    if risk_percentage is None:
        return "Unknown"
    if risk_percentage < 30:
        return "Low"
    elif risk_percentage < 50: # Matches bls_job_mapper's category logic
        return "Moderate"
    elif risk_percentage < 70: # Matches bls_job_mapper's category logic
        return "High"
    else:
        return "Very High"

def get_job_displacement_risk(job_title: str) -> dict:
    """
    Get AI displacement risk data for a specific job title by calling bls_job_mapper.

    Args:
        job_title (str): The job title to analyze.

    Returns:
        dict: A dictionary containing risk assessment data, structured for compatibility
              with parts of the application that might expect the output format from
              a previous version of ai_job_displacement.py. Returns an error dictionary
              if data cannot be fetched or processed.
    """
    logger.info(f"Getting displacement risk for '{job_title}' via bls_job_mapper.")

    if not MODULE_IMPORT_SUCCESS or not hasattr(bls_job_mapper, 'get_complete_job_data'):
        error_msg = "Internal error: bls_job_mapper module is not available or not correctly loaded."
        logger.error(error_msg)
        return {"error": error_msg, "job_title": job_title}

    try:
        # Fetch comprehensive data from bls_job_mapper
        # bls_job_mapper.get_complete_job_data is the single source of truth for BLS data and its interpretation.
        job_data_from_mapper = bls_job_mapper.get_complete_job_data(job_title)

        if not job_data_from_mapper:
            error_msg = f"No data returned from bls_job_mapper for '{job_title}'."
            logger.warning(error_msg)
            return {"error": error_msg, "job_title": job_title}

        if "error" in job_data_from_mapper:
            logger.warning(f"Error from bls_job_mapper for '{job_title}': {job_data_from_mapper['error']}")
            # Propagate the error from bls_job_mapper
            return {
                "error": job_data_from_mapper['error'],
                "job_title": job_title,
                "source_module": "bls_job_mapper"
            }

        # Transform the output of bls_job_mapper to the structure expected by consumers of this module.
        # This mapping ensures compatibility while centralizing data logic in bls_job_mapper.
        
        year_1_risk = job_data_from_mapper.get('year_1_risk')
        year_5_risk = job_data_from_mapper.get('year_5_risk')
        
        # Use risk_category from mapper if available, otherwise calculate from year_5_risk
        year_5_level_text = job_data_from_mapper.get('risk_category') or _calculate_risk_level_text(year_5_risk)

        formatted_risk_data = {
            'job_title': job_data_from_mapper.get('job_title', job_title), # Standardized title from mapper
            'occupation_code': job_data_from_mapper.get('occupation_code'),
            'data_sources': [("BLS Data (via bls_job_mapper)", job_data_from_mapper.get('source', 'N/A'))],
            'job_category': job_data_from_mapper.get('job_category'),
            'risk_metrics': {
                'year_1_risk': year_1_risk,
                'year_5_risk': year_5_risk,
                'year_1_level': _calculate_risk_level_text(year_1_risk),
                'year_5_level': year_5_level_text
            },
            # Provide risk_factors and protective_factors as lists of strings, as returned by bls_job_mapper
            'risk_factors': job_data_from_mapper.get('risk_factors', []),
            'protective_factors': job_data_from_mapper.get('protective_factors', []),
            # 'trend' can be mapped from 'analysis' or 'summary' from bls_job_mapper
            'trend': job_data_from_mapper.get('analysis') or job_data_from_mapper.get('summary', "No trend analysis available."),
            'bls_employment_data': job_data_from_mapper.get('bls_data', {}), # Pass through nested BLS data
            'last_updated_from_bls': job_data_from_mapper.get('last_updated')
        }
        
        logger.info(f"Successfully transformed data for '{job_title}' from bls_job_mapper output.")
        return formatted_risk_data

    except Exception as e:
        logger.error(f"Unexpected error in get_job_displacement_risk for '{job_title}': {e}", exc_info=True)
        return {
            "error": f"An unexpected system error occurred: {str(e)}",
            "job_title": job_title
        }

if __name__ == "__main__":
    # Example usage for testing this module directly.
    # This requires bls_job_mapper.py to be functional and its dependencies (like bls_connector, database)
    # to be configured and working (e.g., DATABASE_URL, BLS_API_KEY set in environment).
    
    # Setup basic logging for standalone testing
    if not logger.handlers:
        _test_handler = logging.StreamHandler()
        _test_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
        _test_handler.setFormatter(_test_formatter)
        logger.addHandler(_test_handler)
        logger.setLevel(logging.INFO)

    logger.info("Running ai_job_displacement.py direct tests...")

    test_titles = [
        "Software Developer",
        "Registered Nurse",
        "NonExistentJobTitleXYZ123", # Expected to result in an error from bls_job_mapper
        "Engineer" 
    ]

    if not MODULE_IMPORT_SUCCESS:
        logger.critical("Cannot run tests because bls_job_mapper module failed to import.")
    else:
        for title in test_titles:
            logger.info(f"\n--- Testing job title: '{title}' ---")
            risk_info = get_job_displacement_risk(title)
            
            if "error" in risk_info:
                logger.error(f"Error for '{title}': {risk_info['error']}")
            else:
                logger.info(f"Successfully processed '{risk_info.get('job_title')}':")
                logger.info(f"  Occupation Code: {risk_info.get('occupation_code')}")
                logger.info(f"  Job Category: {risk_info.get('job_category')}")
                logger.info(f"  Risk Metrics: {risk_info.get('risk_metrics')}")
                logger.info(f"  Data Sources: {risk_info.get('data_sources')}")
                logger.info(f"  Risk Factors (first 2): {risk_info.get('risk_factors', [])[:2]}")
                logger.info(f"  Protective Factors (first 2): {risk_info.get('protective_factors', [])[:2]}")
                logger.info(f"  Trend/Analysis (snippet): {risk_info.get('trend', '')[:100]}...")
                logger.info(f"  BLS Employment Data (median wage): {risk_info.get('bls_employment_data',{}).get('median_wage')}")

    logger.info("\nai_job_displacement.py direct tests complete.")
