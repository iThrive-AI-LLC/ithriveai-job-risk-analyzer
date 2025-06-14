"""
BLS (Bureau of Labor Statistics) API Connector
This module handles communication with the BLS API to fetch official employment data.
It includes robust error handling, retry logic, and caching.
It strictly uses real BLS data and does not fall back to sample/fictional data.
"""
import os
import requests
import json
import time
import logging
import datetime # Added missing import
from typing import Dict, List, Any, Optional
import streamlit as st # For caching, assuming it's run in a Streamlit context

# Configure logging
logger = logging.getLogger(__name__)

# --- API Configuration ---
BLS_API_BASE_URL = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 5  # seconds
MAX_SERIES_PER_REQUEST = 50 # BLS API v2 limit

# --- Helper Function to Get API Key ---
def _get_api_key() -> Optional[str]:
    """Retrieves the BLS API key from environment variables or Streamlit secrets."""
    api_key = os.environ.get('BLS_API_KEY')
    if not api_key:
        try:
            if hasattr(st, 'secrets'):
                api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
        except Exception: # st.secrets might not be available if not in Streamlit context
            pass
    
    if not api_key:
        logger.error("BLS_API_KEY not found in environment variables or Streamlit secrets.")
        return None
    return api_key

# --- Core API Data Fetching Function with Cache and Retry ---
@st.cache_data(ttl=3600 * 24, show_spinner=False) # Cache for 24 hours
def get_bls_data(series_ids: List[str], start_year: str, end_year: str, api_key_param: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch data from BLS API for specified series IDs and date range.
    Includes caching, retry logic, and handles API key.

    Args:
        series_ids: List of BLS series IDs to fetch.
        start_year: Starting year for data (format: 'YYYY').
        end_year: Ending year for data (format: 'YYYY').
        api_key_param: Optionally pass API key (for non-Streamlit contexts or testing).

    Returns:
        Dictionary containing the API response or an error structure.
    """
    api_key = api_key_param or _get_api_key()
    if not api_key:
        return {"status": "error", "message": "BLS_API_KEY is not configured."}

    if not series_ids:
        logger.warning("get_bls_data called with no series_ids.")
        return {"status": "error", "message": "No series IDs provided."}
    
    # BLS API has a limit on series per request (typically 50 for v2)
    # This function expects the caller to handle batching if necessary,
    # or it processes only the first MAX_SERIES_PER_REQUEST.
    # For simplicity in this connector, we'll process in chunks if too many.
    
    all_results_data: Dict[str, Any] = {"Results": {"series": []}, "status": "REQUEST_SUCCEEDED", "message": []}
    series_chunks = [series_ids[i:i + MAX_SERIES_PER_REQUEST] for i in range(0, len(series_ids), MAX_SERIES_PER_REQUEST)]

    for chunk_idx, series_chunk in enumerate(series_chunks):
        logger.info(f"Fetching BLS data for chunk {chunk_idx + 1}/{len(series_chunks)}, {len(series_chunk)} series IDs: {', '.join(series_chunk[:3])}...")
        
        payload = {
            "seriesid": series_chunk,
            "startyear": start_year,
            "endyear": end_year,
            "registrationkey": api_key,
            "catalog": True, # Optionally get catalog data
            "annualaverage": True # Optionally get annual average if applicable
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(BLS_API_BASE_URL, json=payload, timeout=30) # 30-second timeout
                response.raise_for_status()  # Raise HTTPError for bad responses (4XX or 5XX)
                
                data = response.json()
                
                if data.get("status") != "REQUEST_SUCCEEDED":
                    error_message = f"BLS API request not successful. Status: {data.get('status')}. Messages: {data.get('message')}"
                    logger.error(error_message)
                    # If it's an API-level error for this chunk, we might not want to retry immediately
                    # unless it's a transient server issue.
                    if attempt == MAX_RETRIES - 1: # Last attempt
                        return {"status": "error", "message": error_message, "details": data.get('message', [])}
                    # For certain errors (e.g. invalid series ID), retrying won't help.
                    # However, simple retry for now.
                    
                # Aggregate results if successful
                if "Results" in data and "series" in data["Results"]:
                    all_results_data["Results"]["series"].extend(data["Results"]["series"])
                if "message" in data and data["message"]:
                     all_results_data["message"].extend(m for m in data["message"] if m not in all_results_data["message"])

                logger.info(f"Successfully fetched data for chunk {chunk_idx + 1}.")
                break  # Exit retry loop on success

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTPError on attempt {attempt + 1} for chunk {chunk_idx+1}: {e}. Response: {e.response.text}")
                if e.response.status_code == 400: # Bad request, likely invalid series or parameters
                     return {"status": "error", "message": f"BLS API Bad Request: {e.response.text}", "details": str(e)}
                if e.response.status_code == 429: # Rate limit
                    logger.warning("Rate limit hit. Waiting longer before retry.")
                    time.sleep(INITIAL_RETRY_DELAY * (attempt + 1) * 5) # Wait much longer for rate limits
                # Other HTTP errors might be retriable
            except requests.exceptions.RequestException as e:
                logger.error(f"RequestException on attempt {attempt + 1} for chunk {chunk_idx+1}: {e}")
            
            if attempt < MAX_RETRIES - 1:
                delay = INITIAL_RETRY_DELAY * (2 ** attempt) # Exponential backoff
                logger.info(f"Retrying chunk {chunk_idx+1} in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Failed to fetch BLS data for chunk {chunk_idx+1} after {MAX_RETRIES} attempts.")
                # If one chunk fails, we might return partial data or a full error.
                # For now, let's return an error for the whole request if any chunk fails.
                return {"status": "error", "message": f"Failed to fetch data for one or more series chunks after {MAX_RETRIES} attempts."}
        
        # Small delay between chunks if multiple chunks exist
        if len(series_chunks) > 1 and chunk_idx < len(series_chunks) - 1:
            time.sleep(1) # 1-second delay to be polite to the API

    if not all_results_data["Results"]["series"]:
        logger.warning(f"No series data found in BLS response for IDs: {series_ids}, despite reported success.")
        # This can happen if series IDs are valid but have no data for the period.
        # BLS API might still return REQUEST_SUCCEEDED with empty data.
        all_results_data["message"].append("No data available for the HHHHrequested series and period.")


    return all_results_data

# --- Functions to get specific types of data (using the core get_bls_data) ---

def get_occupation_data(occ_code: str, start_year: Optional[str] = None, end_year: Optional[str] = None) -> Dict[str, Any]:
    """
    Get OES employment and wage data for a specific national-level occupation code.
    Note: This constructs common series IDs. For more specific data (state, industry),
    the caller (`bls_job_mapper`) should construct and pass full series IDs to `get_bls_data`.

    Args:
        occ_code: SOC occupation code (e.g., '15-1252').
        start_year: Optional start year (defaults to 5 years ago).
        end_year: Optional end year (defaults to current year).

    Returns:
        Dictionary with structured occupation data or an error.
    """
    current_actual_year = datetime.datetime.now().year
    _end_year = end_year or str(current_actual_year)
    _start_year = start_year or str(int(_end_year) - 5) # Default to 5 years of data

    # Standard OES series IDs for national, cross-industry data:
    # OEU<area_code_7_digits><industry_code_6_digits><occupation_code_6_digits_no_hyphen><datatype_code_2_digits>
    # Area: 0000000 (U.S.)
    # Industry: 000000 (Cross-industry)
    # Datatype: 01 (Employment), 03 (Annual Mean Wage), 04 (Annual Median Wage)
    
    soc_part = occ_code.replace("-", "")
    if len(soc_part) != 6:
        logger.error(f"Invalid SOC code format for series ID construction: {occ_code}")
        return {"status": "error", "message": f"Invalid SOC code format: {occ_code}. Must be XX-XXXX."}

    series_to_fetch = [
        f"OEU0000000000000{soc_part}01",  # Employment
        f"OEU0000000000000{soc_part}03",  # Annual Mean Wage
        f"OEU0000000000000{soc_part}04",  # Annual Median Wage
    ]
    
    logger.info(f"Constructed series IDs for OES data for {occ_code}: {series_to_fetch}")
    raw_data = get_bls_data(series_to_fetch, _start_year, _end_year)

    if raw_data.get("status") != "REQUEST_SUCCEEDED":
        return raw_data # Return the error from get_bls_data

    # Parse the response
    parsed_data: Dict[str, Any] = {
        "occupation_code": occ_code,
        "employment": None, # Latest annual employment
        "annual_mean_wage": None, # Latest annual mean wage
        "annual_median_wage": None, # Latest annual median wage
        "employment_trend": [],
        "wage_trend": [], # Could be median or mean
        "source_series_ids": series_to_fetch,
        "messages": raw_data.get("message", [])
    }

    for series in raw_data.get("Results", {}).get("series", []):
        series_id = series.get("seriesID")
        data_points = series.get("data", [])
        
        if not data_points:
            logger.warning(f"No data points returned for series ID: {series_id}")
            continue
            
        # Sort data points by year descending to get the latest first
        latest_data_point = sorted(data_points, key=lambda x: x.get("year"), reverse=True)[0]
        value = latest_data_point.get("value")
        
        try: # Convert value to int/float
            numeric_value = float(value) if '.' in value else int(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert value '{value}' to numeric for series {series_id}.")
            numeric_value = None


        if series_id.endswith("01"): # Employment
            parsed_data["employment"] = numeric_value
            parsed_data["employment_trend"] = [{"year": dp.get("year"), "value": dp.get("value")} for dp in data_points]
        elif series_id.endswith("03"): # Annual Mean Wage
            parsed_data["annual_mean_wage"] = numeric_value
            # Choose one wage trend, e.g., mean wage
            if not parsed_data["wage_trend"]: # Prioritize median if available later
                 parsed_data["wage_trend"] = [{"year": dp.get("year"), "value": dp.get("value")} for dp in data_points]
        elif series_id.endswith("04"): # Annual Median Wage
            parsed_data["annual_median_wage"] = numeric_value
            parsed_data["wage_trend"] = [{"year": dp.get("year"), "value": dp.get("value")} for dp in data_points] # Prefer median for trend

    if parsed_data["employment"] is None and parsed_data["annual_median_wage"] is None:
        logger.warning(f"No employment or median wage data successfully parsed for {occ_code}.")
        # Add a message if no core data was found
        if not parsed_data["messages"]:
            parsed_data["messages"] = ["Could not retrieve key occupational data (employment/wage). The occupation code may be invalid for national OES data or data might be suppressed."]
        # parsed_data["status"] = "partial_error" # Indicate that some data might be missing

    return {"status": "success", "data": parsed_data}


def get_employment_projection(occ_code: str) -> Dict[str, Any]:
    """
    Get employment projections for a national-level occupation.
    This is a simplified version. The Employment Projections (EP) program data can be complex.
    `bls_job_mapper` might need to handle more detailed series ID construction for EP.

    Args:
        occ_code: SOC occupation code (e.g., '15-1252').

    Returns:
        Dictionary with projection data or an error.
    """
    # EP series IDs are often structured differently, e.g., EPU<soc_code_no_hyphen>...
    # This is a placeholder for constructing common EP series IDs.
    # Actual series IDs depend on the specific projection table and measures.
    # Example: Employment, Employment change, Percent change, Occupational openings
    # For now, this function will return an indicative structure or error,
    # as fetching comprehensive projections often requires knowing specific table series.
    
    soc_part = occ_code.replace("-", "")
    if len(soc_part) != 6:
        return {"status": "error", "message": f"Invalid SOC code format: {occ_code}."}

    # These are illustrative series IDs and might not be universally correct for all EP data.
    # The EP program publishes data in tables, and series IDs refer to specific cells/rows.
    # A more robust solution involves mapping SOC codes to the latest EP table series.
    # For example, series from the National Employment Matrix.
    # Let's assume we need to find series for:
    # - Base year employment
    # - Projected year employment
    # - Employment change (numeric)
    # - Employment change (percent)
    # - Annual job openings
    
    # This is highly dependent on knowing the current EP table structure and series patterns.
    # We will return a "not directly available" message, suggesting bls_job_mapper handle this.
    logger.warning(f"Direct fetching of Employment Projections for {occ_code} via generic series IDs is complex and not fully implemented in bls_connector. bls_job_mapper should handle specific EP series if needed.")
    return {
        "status": "info", 
        "message": f"Detailed Employment Projections for {occ_code} require specific series IDs from EP tables. This connector provides OES data. For projections, ensure bls_job_mapper queries appropriate EP series.",
        "occupation_code": occ_code,
        "projections": { # Placeholder structure
            "current_employment": None,
            "projected_employment": None,
            "employment_change_numeric": None,
            "employment_change_percent": None,
            "annual_job_openings": None,
            "note": "Fetch specific EP series via get_bls_data in bls_job_mapper."
        }
    }

def search_occupations(query: str, limit: int = 10) -> List[Dict[str, str]]:
    """
    Placeholder for searching occupation codes.
    A production system should use a local SOC database or O*NET API.
    This provides a very limited, hardcoded list for basic functionality.

    Args:
        query: Search term for occupation title.
        limit: Max number of results.

    Returns:
        List of matching occupations (dict with "code" and "title").
    """
    logger.warning("search_occupations is using a limited, hardcoded list. For production, integrate a full SOC database or O*NET API.")
    
    # Extremely simplified list for placeholder purposes.
    # This should be replaced by a comprehensive SOC list in a real application.
    sample_soc_codes = [
        {"code": "11-1011", "title": "Chief Executives"},
        {"code": "15-1252", "title": "Software Developers"},
        {"code": "15-1254", "title": "Web Developers"},
        {"code": "17-2071", "title": "Electrical Engineers"},
        {"code": "29-1141", "title": "Registered Nurses"},
        {"code": "25-2021", "title": "Elementary School Teachers, Except Special Education"},
        {"code": "13-2011", "title": "Accountants and Auditors"},
        {"code": "41-2031", "title": "Retail Salespersons"},
        {"code": "35-2014", "title": "Cooks, Restaurant"},
        {"code": "53-3032", "title": "Heavy and Tractor-Trailer Truck Drivers"},
        {"code": "43-4051", "title": "Customer Service Representatives"}
    ]
    
    query_lower = query.lower()
    matches = [
        item for item in sample_soc_codes 
        if query_lower in item["title"].lower() or query_lower in item["code"]
    ]
    
    return matches[:limit]

def check_api_connectivity() -> bool:
    """
    Check if the BLS API is accessible with the configured API key.

    Returns:
        Boolean indicating API accessibility.
    """
    logger.info("Checking BLS API connectivity...")
    api_key = _get_api_key()
    if not api_key:
        logger.error("BLS API connectivity check failed: API key not configured.")
        return False
    
    # Use a common, stable series ID for testing (e.g., Current Population Survey - Labor Force Participation Rate)
    # This is less likely to change than specific occupational series.
    test_series_id = "LNS11300000" # Civilian labor force participation rate, seasonally adjusted
    current_year = str(datetime.datetime.now().year)
    
    try:
        # Make a minimal request, bypass Streamlit cache for this check
        response_data = get_bls_data([test_series_id], current_year, current_year, api_key_param=api_key) # Pass key to bypass cache if needed
        
        if response_data.get("status") == "REQUEST_SUCCEEDED":
            logger.info("BLS API connectivity check successful.")
            return True
        else:
            logger.error(f"BLS API connectivity check failed. Status: {response_data.get('status')}, Message: {response_data.get('message')}")
            return False
    except Exception as e:
        logger.error(f"BLS API connectivity check failed with exception: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    # Example Usage (for testing outside Streamlit)
    # Ensure BLS_API_KEY is set as an environment variable for this test.
    logging.basicConfig(level=logging.INFO)
    logger.info("Running bls_connector.py direct tests...")

    if not _get_api_key():
        logger.error("Cannot run tests: BLS_API_KEY environment variable is not set.")
    else:
        logger.info(f"API Key found. Proceeding with tests.")
        
        # Test API Connectivity
        logger.info("\n--- Test 1: API Connectivity ---")
        is_connected = check_api_connectivity()
        logger.info(f"API Connected: {is_connected}")

        if is_connected:
            # Test get_bls_data with a known series
            logger.info("\n--- Test 2: get_bls_data (CPI) ---")
            cpi_series = ["CUUR0000SA0"] # CPI for All Urban Consumers, All Items
            year = str(datetime.datetime.now().year - 1) # Last full year
            cpi_data = get_bls_data(cpi_series, year, year)
            if cpi_data.get("status") == "REQUEST_SUCCEEDED" and cpi_data.get("Results", {}).get("series"):
                logger.info(f"CPI Data for {year} (Series {cpi_series[0]}): {cpi_data['Results']['series'][0]['data']}")
            else:
                logger.error(f"Failed to get CPI data: {cpi_data.get('message')}")

            # Test get_occupation_data
            logger.info("\n--- Test 3: get_occupation_data (Software Developers) ---")
            dev_occ_code = "15-1252"
            dev_data = get_occupation_data(dev_occ_code)
            if dev_data.get("status") == "success":
                logger.info(f"Software Developer ({dev_occ_code}) Data:")
                logger.info(f"  Latest Employment: {dev_data['data'].get('employment')}")
                logger.info(f"  Latest Median Wage: {dev_data['data'].get('annual_median_wage')}")
                if dev_data['data'].get('messages'):
                    logger.info(f"  Messages: {dev_data['data']['messages']}")
            else:
                logger.error(f"Failed to get data for {dev_occ_code}: {dev_data.get('message')}")
            
            logger.info("\n--- Test 4: get_occupation_data (Electrical Engineers) ---")
            ee_occ_code = "17-2071"
            ee_data = get_occupation_data(ee_occ_code)
            if ee_data.get("status") == "success":
                logger.info(f"Electrical Engineer ({ee_occ_code}) Data:")
                logger.info(f"  Latest Employment: {ee_data['data'].get('employment')}")
                logger.info(f"  Latest Median Wage: {ee_data['data'].get('annual_median_wage')}")
                if ee_data['data'].get('messages'):
                    logger.info(f"  Messages: {ee_data['data']['messages']}")
            else:
                logger.error(f"Failed to get data for {ee_occ_code}: {ee_data.get('message')}")


            # Test get_employment_projection (will show info message)
            logger.info("\n--- Test 5: get_employment_projection (Illustrative) ---")
            proj_data = get_employment_projection(dev_occ_code)
            logger.info(f"Projection info for {dev_occ_code}: {proj_data}")

            # Test search_occupations (placeholder)
            logger.info("\n--- Test 6: search_occupations (Placeholder) ---")
            search_results = search_occupations("developer")
            logger.info(f"Search results for 'developer': {search_results}")
            
            logger.info("\n--- Test 7: get_bls_data (multiple series, one chunk) ---")
            multi_series = ["CUUR0000SA0", "SUUR0000SA0"] # CPI and Chained CPI
            multi_data = get_bls_data(multi_series, year, year)
            if multi_data.get("status") == "REQUEST_SUCCEEDED":
                logger.info(f"Successfully fetched {len(multi_data.get('Results', {}).get('series', []))} series.")
            else:
                logger.error(f"Failed to get multiple series: {multi_data.get('message')}")
        else:
            logger.warning("Skipping further tests as API connectivity failed.")
            
    logger.info("\nbls_connector.py direct tests complete.")
