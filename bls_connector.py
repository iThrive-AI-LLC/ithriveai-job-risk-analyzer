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
import datetime
from typing import Dict, List, Any, Optional

import streamlit as st # For caching, assuming it's run in a Streamlit context

# Configure logging
logger = logging.getLogger(__name__)
if not logger.hasHandlers(): # Avoid duplicate handlers if re-run in some environments
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


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
            if hasattr(st, 'secrets') and callable(st.secrets.get): # Check if st.secrets is usable
                api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
        except Exception: 
            pass # st.secrets might not be available if not in Streamlit context
    
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
    
    all_results_data: Dict[str, Any] = {"Results": {"series": []}, "status": "REQUEST_SUCCEEDED", "message": []}
    series_chunks = [series_ids[i:i + MAX_SERIES_PER_REQUEST] for i in range(0, len(series_ids), MAX_SERIES_PER_REQUEST)]

    for chunk_idx, series_chunk in enumerate(series_chunks):
        logger.info(f"Fetching BLS data for chunk {chunk_idx + 1}/{len(series_chunks)}, {len(series_chunk)} series IDs: {', '.join(series_chunk[:3])}...")
        
        payload = {
            "seriesid": series_chunk,
            "startyear": start_year,
            "endyear": end_year,
            "registrationkey": api_key,
            "catalog": True, 
            "annualaverage": True 
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(BLS_API_BASE_URL, json=payload, timeout=30) 
                response.raise_for_status()  
                
                data = response.json()
                
                if data.get("status") != "REQUEST_SUCCEEDED":
                    error_message = f"BLS API request not successful. Status: {data.get('status')}. Messages: {data.get('message')}"
                    logger.error(error_message)
                    if attempt == MAX_RETRIES - 1: 
                        return {"status": "error", "message": error_message, "details": data.get('message', [])}
                    # Continue to retry if not last attempt
                else: # Successful status from BLS
                    if "Results" in data and "series" in data["Results"]:
                        all_results_data["Results"]["series"].extend(data["Results"]["series"])
                    if "message" in data and data["message"]:
                         all_results_data["message"].extend(m for m in data["message"] if m not in all_results_data["message"])
                    logger.info(f"Successfully fetched data for chunk {chunk_idx + 1}.")
                    break  # Exit retry loop on success

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTPError on attempt {attempt + 1} for chunk {chunk_idx+1}: {e}. Response: {e.response.text if e.response else 'No response text'}")
                if e.response is not None and e.response.status_code == 400: 
                     return {"status": "error", "message": f"BLS API Bad Request: {e.response.text if e.response else 'No response text'}", "details": str(e)}
                if e.response is not None and e.response.status_code == 429: 
                    logger.warning("Rate limit hit. Waiting longer before retry.")
                    time.sleep(INITIAL_RETRY_DELAY * (attempt + 1) * 5) 
            except requests.exceptions.RequestException as e:
                logger.error(f"RequestException on attempt {attempt + 1} for chunk {chunk_idx+1}: {e}")
            
            if attempt < MAX_RETRIES - 1:
                delay = INITIAL_RETRY_DELAY * (2 ** attempt) 
                logger.info(f"Retrying chunk {chunk_idx+1} in {delay} seconds...")
                time.sleep(delay)
            elif data.get("status") != "REQUEST_SUCCEEDED": # Ensure we return error if all retries failed for a non-200 but non-exception response
                logger.error(f"Failed to fetch BLS data for chunk {chunk_idx+1} after {MAX_RETRIES} attempts due to API error status.")
                return {"status": "error", "message": f"Failed to fetch data for one or more series chunks after {MAX_RETRIES} attempts. Last API status: {data.get('status')}, Messages: {data.get('message')}"}
        
        if len(series_chunks) > 1 and chunk_idx < len(series_chunks) - 1:
            time.sleep(1) 

    if not all_results_data["Results"]["series"] and all_results_data["status"] == "REQUEST_SUCCEEDED":
        logger.warning(f"No series data found in BLS response for IDs: {series_ids}, despite reported success by API.")
        # Do not add "No data available" message here if the API itself reported success but returned no series data.
        # The individual parsers should handle this.
        
    return all_results_data

# --- OES Data Functions ---
def construct_oes_series_ids(soc_code: str) -> List[str]:
    """
    Constructs OES series IDs for employment and wages based on the SOC code.
    Area: 0000000 (U.S.)
    Industry: 000000 (Cross-industry)
    Datatype: 01 (Employment), 03 (Annual Mean Wage), 04 (Annual Median Wage)
    """
    soc_part = soc_code.replace("-", "")
    if len(soc_part) != 6:
        logger.error(f"Invalid SOC code format for OES series ID construction: {soc_code}. Must be XX-XXXX.")
        return []

    series_ids = [
        f"OEU0000000000000{soc_part}01",  # Employment
        f"OEU0000000000000{soc_part}03",  # Annual Mean Wage
        f"OEU0000000000000{soc_part}04",  # Annual Median Wage
    ]
    logger.info(f"Constructed OES series IDs for {soc_code}: {series_ids}")
    return series_ids

def parse_oes_series_response(oes_response: Dict[str, Any], soc_code: str) -> Dict[str, Any]:
    """Parses raw OES API response into a structured dictionary."""
    parsed_data: Dict[str, Any] = {
        "occupation_code": soc_code,
        "employment": None,
        "annual_mean_wage": None,
        "annual_median_wage": None,
        "data_year": None, 
        "messages": oes_response.get("message", [])
    }

    if oes_response.get("status") != "REQUEST_SUCCEEDED":
        logger.warning(f"OES response status not successful for SOC {soc_code}: {oes_response.get('message', 'No message')}")
        return parsed_data 

    for series in oes_response.get("Results", {}).get("series", []):
        series_id = series.get("seriesID")
        data_points = series.get("data", [])
        
        if not data_points:
            logger.warning(f"No data points returned for OES series ID: {series_id}")
            continue
            
        latest_data_point = sorted(data_points, key=lambda x: x.get("year", "0"), reverse=True)[0]
        value_str = latest_data_point.get("value")
        year = latest_data_point.get("year")
        
        numeric_value: Optional[Union[int, float]] = None
        if value_str is not None:
            try:
                numeric_value = float(value_str) if '.' in value_str else int(value_str)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert OES value '{value_str}' to numeric for series {series_id}.")
        
        if year and (parsed_data["data_year"] is None or int(year) > int(parsed_data["data_year"])):
            parsed_data["data_year"] = year

        if series_id.endswith("01"): 
            parsed_data["employment"] = numeric_value
        elif series_id.endswith("03"): 
            parsed_data["annual_mean_wage"] = numeric_value
        elif series_id.endswith("04"): 
            parsed_data["annual_median_wage"] = numeric_value

    if parsed_data["employment"] is None and parsed_data["annual_median_wage"] is None:
        logger.warning(f"No employment or median wage data successfully parsed for OES SOC {soc_code}.")
        
    return parsed_data

def get_oes_data_for_soc(soc_code: str, start_year: str, end_year: str) -> Dict[str, Any]:
    """
    Fetches and parses OES data for a given SOC code and year range.
    """
    logger.info(f"Getting OES data for SOC {soc_code} from {start_year} to {end_year}.")
    series_ids = construct_oes_series_ids(soc_code)
    if not series_ids:
        return {"status": "error", "message": f"Could not construct OES series IDs for SOC {soc_code}."}
    
    raw_oes_data = get_bls_data(series_ids, start_year, end_year)
    return parse_oes_series_response(raw_oes_data, soc_code)

# --- Employment Projections (EP) Data Functions ---
def construct_ep_series_ids(soc_code: str) -> List[str]:
    """
    Constructs EP series IDs for national employment projections.
    Datatypes: 01 (Base Emp), 02 (Proj Emp), 03 (Change Num), 04 (Change Pct), 07 (Openings)
    """
    soc_part = soc_code.replace("-", "")
    if len(soc_part) != 6:
        logger.error(f"Invalid SOC code format for EP series ID construction: {soc_code}. Must be XX-XXXX.")
        return []
    
    # National data (U.S. total), All industries
    area_code = "0000000"  # National
    industry_code = "000000" # All industries
    
    series_ids = [
        f"EPU{area_code}{industry_code}{soc_part}01",  # Employment, base year
        f"EPU{area_code}{industry_code}{soc_part}02",  # Employment, projected year
        f"EPU{area_code}{industry_code}{soc_part}03",  # Employment change, numeric
        f"EPU{area_code}{industry_code}{soc_part}04",  # Employment change, percent
        f"EPU{area_code}{industry_code}{soc_part}07",  # Occupational openings, annual average
    ]
    logger.info(f"Constructed EP series IDs for {soc_code}: {series_ids}")
    return series_ids

def parse_ep_series_response(ep_response: Dict[str, Any], soc_code: str) -> Dict[str, Any]:
    """Parses raw EP API response into a structured dictionary."""
    parsed_data: Dict[str, Any] = {
        "occupation_code": soc_code,
        "current_employment": None,      # From datatype 01
        "projected_employment": None,    # From datatype 02
        "employment_change_numeric": None, # From datatype 03
        "employment_change_percent": None, # From datatype 04
        "annual_job_openings": None,     # From datatype 07
        "base_year": None,
        "projection_year": None,
        "messages": ep_response.get("message", [])
    }

    if ep_response.get("status") != "REQUEST_SUCCEEDED":
        logger.warning(f"EP response status not successful for SOC {soc_code}: {ep_response.get('message', 'No message')}")
        return parsed_data

    for series in ep_response.get("Results", {}).get("series", []):
        series_id = series.get("seriesID")
        catalog = series.get("catalog")
        data_points = series.get("data", [])

        if not data_points:
            logger.warning(f"No data points returned for EP series ID: {series_id}")
            continue
        
        # EP data usually has one data point per series for the projection span
        data_point = data_points[0] 
        value_str = data_point.get("value")
        
        numeric_value: Optional[Union[int, float]] = None
        if value_str is not None:
            try:
                numeric_value = float(value_str) if '.' in value_str else int(value_str)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert EP value '{value_str}' to numeric for series {series_id}.")

        if catalog:
            if parsed_data["base_year"] is None and catalog.get("projection_base_year"):
                parsed_data["base_year"] = catalog.get("projection_base_year")
            if parsed_data["projection_year"] is None and catalog.get("projection_target_year"):
                parsed_data["projection_year"] = catalog.get("projection_target_year")

        if series_id.endswith("01"):
            parsed_data["current_employment"] = numeric_value
        elif series_id.endswith("02"):
            parsed_data["projected_employment"] = numeric_value
        elif series_id.endswith("03"):
            parsed_data["employment_change_numeric"] = numeric_value
        elif series_id.endswith("04"):
            parsed_data["employment_change_percent"] = numeric_value
        elif series_id.endswith("07"):
            parsed_data["annual_job_openings"] = numeric_value
            
    return parsed_data

def get_employment_projection(occ_code: str) -> Dict[str, Any]:
    """
    Get employment projections for an occupation using BLS API.
    Fetches data for the typical 10-year projection span.
    """
    logger.info(f"Getting employment projections for SOC {occ_code}.")
    series_ids = construct_ep_series_ids(occ_code)
    if not series_ids:
        return {"status": "error", "message": f"Could not construct EP series IDs for SOC {occ_code}."}

    # Determine typical projection years. BLS usually projects 10 years out.
    # Example: If current year is 2024, base might be 2022, projection 2032.
    # We fetch a broad range to ensure we get the projection data.
    # The `annualaverage=True` in `get_bls_data` helps for some series.
    # The `catalog=True` helps get metadata about projection years.
    current_datetime_year = datetime.datetime.now().year
    start_year = str(current_datetime_year - 2) # Look back a bit for base year
    end_year = str(current_datetime_year + 10)  # Look forward for projection year
    
    raw_ep_data = get_bls_data(series_ids, start_year, end_year)
    
    # Reformat the parsed data to match the previous placeholder structure for compatibility
    parsed_data = parse_ep_series_response(raw_ep_data, occ_code)
    if parsed_data.get("status") == "error" or parsed_data.get("current_employment") is None : # check if parsing failed
         # If parsing failed or returned no data, return the error structure
        if "messages" in parsed_data and any("series id is not valid" in msg.lower() for msg in parsed_data["messages"]):
             logger.warning(f"EP data not available for SOC {occ_code} (invalid series ID).")
             return {"status": "error", "message": f"No employment projections found (invalid series ID) for occupation code {occ_code}", "projections": {}}
        logger.warning(f"Failed to parse EP data for SOC {occ_code}. Response: {parsed_data}")
        return {"status": "error", "message": f"Could not parse employment projections for {occ_code}. Messages: {parsed_data.get('messages')}", "projections": {}}

    # Return in the structure expected by bls_job_mapper
    return {
        "status": "success",
        "occupation_code": occ_code,
        "projections": {
            "current_employment": parsed_data["current_employment"],
            "projected_employment": parsed_data["projected_employment"],
            "percent_change": parsed_data["employment_change_percent"],
            "annual_job_openings": parsed_data["annual_job_openings"],
            "base_year": parsed_data["base_year"],
            "projection_year": parsed_data["projection_year"]
        },
        "messages": parsed_data.get("messages", [])
    }

def get_ep_data_for_soc(soc_code: str) -> Dict[str, Any]:
    """
    Wrapper function to get employment projection data for a SOC code.
    """
    logger.info(f"Calling get_employment_projection for SOC {soc_code} via get_ep_data_for_soc.")
    return get_employment_projection(soc_code)

# --- Utility Functions ---
def search_occupations(query: str) -> List[Dict[str, str]]:
    """
    Search for occupation codes matching the query.
    This is a placeholder. A real implementation would query a comprehensive SOC code database or API.
    """
    logger.info(f"Searching occupations for query: '{query}' (using placeholder).")
    # Sample SOC codes and titles (abbreviated list)
    # In a real app, this would come from a database or a more robust source.
    soc_codes_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'soc_structure_2018.json') 
    try:
        with open(soc_codes_data_path, 'r') as f:
            all_soc_codes = json.load(f)
    except FileNotFoundError:
        logger.error(f"SOC codes data file not found at {soc_codes_data_path}. Using minimal sample.")
        all_soc_codes = [
            {"code": "15-1252", "title": "Software Developers"},
            {"code": "29-1141", "title": "Registered Nurses"},
        ]
    
    query_lower = query.lower()
    matches = [
        item for item in all_soc_codes 
        if query_lower in item["title"].lower() or query_lower in item["code"]
    ]
    
    return matches[:20] # Limit results

def check_api_connectivity() -> bool:
    """
    Check if the BLS API is accessible with the provided API key.
    """
    logger.info("Checking BLS API connectivity.")
    api_key = _get_api_key()
    if not api_key:
        logger.warning("BLS API connectivity check: No API key found.")
        return False
    try:
        # Test with a simple, common series ID that should always have data
        # LAUCN040010000000005 = Unemployment rate in California (annual)
        test_data = get_bls_data(["LAUCN040010000000005"], str(datetime.datetime.now().year - 1), str(datetime.datetime.now().year - 1), api_key_param=api_key)
        if test_data.get("status") == "REQUEST_SUCCEEDED":
            logger.info("BLS API connectivity check: Successful.")
            return True
        else:
            logger.warning(f"BLS API connectivity check: Failed. Status: {test_data.get('status')}, Message: {test_data.get('message')}")
            return False
    except Exception as e:
        logger.error(f"BLS API connectivity check: Exception occurred: {e}", exc_info=True)
        return False

# --- Deprecated Function (to be removed or refactored) ---
def get_occupation_data(occ_code: str) -> Dict[str, Any]:
    """
    DEPRECATED: Use get_oes_data_for_soc instead.
    Get employment and wage data for a specific occupation code.
    """
    logger.warning(f"Deprecated function get_occupation_data called for SOC {occ_code}. Redirecting to get_oes_data_for_soc.")
    current_year = datetime.datetime.now().year
    # Fetch data for the last 3-5 available years for OES.
    # BLS data is often released with a lag.
    return get_oes_data_for_soc(occ_code, str(current_year - 4), str(current_year - 1))

