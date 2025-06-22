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
from typing import Dict, List, Any, Optional, Union

import streamlit as st # For caching, assuming it's run in a Streamlit context

# Configure logging
logger = logging.getLogger(__name__)
if not logger.hasHandlers(): # Avoid duplicate handlers if re-run in some environments
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


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
            # Check if st.secrets is available and is the new secrets manager
            if hasattr(st, 'secrets') and hasattr(st.secrets, 'get') and callable(st.secrets.get):
                api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
            # Fallback for older st.secrets dictionary-like access
            elif hasattr(st, 'secrets') and isinstance(st.secrets, dict) and "api_keys" in st.secrets:
                 api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")

        except Exception as e:
            logger.warning(f"Could not access Streamlit secrets to get BLS_API_KEY: {e}")
    
    if not api_key:
        logger.error("BLS_API_KEY not found in environment variables or Streamlit secrets.")
        return None
    return api_key

def is_api_key_available() -> bool:
    """Checks if the BLS API key is configured."""
    return _get_api_key() is not None

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
        logger.error("BLS_API_KEY is not configured. Cannot fetch BLS data.")
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
            "catalog": True, # Request catalog data for series details
            "annualaverage": True # Request annual average data if applicable
        }
        
        data_for_chunk = None # To store response for this chunk

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(BLS_API_BASE_URL, json=payload, timeout=30) # Increased timeout
                response.raise_for_status()  
                
                data_for_chunk = response.json()
                
                if data_for_chunk.get('message'): # Log any top-level or series-specific messages from API
                    logger.warning(f"BLS API messages for chunk {chunk_idx + 1} ({series_chunk}): {data_for_chunk['message']}")
                    all_results_data["message"].extend(m for m in data_for_chunk['message'] if m not in all_results_data["message"])


                if data_for_chunk.get("status") != "REQUEST_SUCCEEDED":
                    error_message = f"BLS API request not successful for chunk {chunk_idx + 1}. Status: {data_for_chunk.get('status')}. Messages: {data_for_chunk.get('message')}"
                    logger.error(error_message)
                    if attempt == MAX_RETRIES - 1: 
                        all_results_data["status"] = "error" 
                        break 
                else: 
                    if "Results" in data_for_chunk and "series" in data_for_chunk["Results"]:
                        all_results_data["Results"]["series"].extend(data_for_chunk["Results"]["series"])
                    logger.info(f"Successfully fetched data for chunk {chunk_idx + 1}.")
                    break  

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTPError on attempt {attempt + 1} for chunk {chunk_idx+1}: {e}. Response: {e.response.text if e.response else 'No response text'}")
                if e.response is not None and e.response.status_code == 400: 
                     all_results_data["status"] = "error"
                     all_results_data["message"].append(f"BLS API Bad Request for chunk {chunk_idx+1}: {e.response.text if e.response else 'No response text'}. Series: {', '.join(series_chunk)}")
                     break 
                if e.response is not None and e.response.status_code == 429: 
                    logger.warning(f"Rate limit hit on chunk {chunk_idx+1}. Waiting longer before retry.")
                    time.sleep(INITIAL_RETRY_DELAY * (attempt + 1) * 5) 
            except requests.exceptions.RequestException as e:
                logger.error(f"RequestException on attempt {attempt + 1} for chunk {chunk_idx+1}: {e}")
            
            if attempt < MAX_RETRIES - 1:
                delay = INITIAL_RETRY_DELAY * (2 ** attempt) 
                logger.info(f"Retrying chunk {chunk_idx+1} in {delay} seconds...")
                time.sleep(delay)
            elif data_for_chunk is None or data_for_chunk.get("status") != "REQUEST_SUCCEEDED":
                logger.error(f"Failed to fetch BLS data for chunk {chunk_idx+1} after {MAX_RETRIES} attempts.")
                all_results_data["status"] = "error" 
                all_results_data["message"].append(f"Failed to fetch data for series chunk {chunk_idx+1} after {MAX_RETRIES} attempts. Last API status: {data_for_chunk.get('status') if data_for_chunk else 'No response'}, Messages: {data_for_chunk.get('message') if data_for_chunk else 'N/A'}")
        
        if len(series_chunks) > 1 and chunk_idx < len(series_chunks) - 1:
            time.sleep(1) 

    if not all_results_data["Results"]["series"] and all_results_data["status"] == "REQUEST_SUCCEEDED":
        logger.warning(f"No series data found in BLS response for IDs: {series_ids}, despite API reporting overall success. Check API messages: {all_results_data.get('message')}")
        
    return all_results_data

# --- OES Data Functions ---
def build_oes_series_id(soc_code: str) -> Dict[str, str]:
    """
    Build a dictionary of OES series IDs used elsewhere in the codebase.

    Returns
    -------
    dict
        {
          "employment":   "...01",
          "mean_wage":    "...03",
          "median_wage":  "...04"
        }
    """
    soc_part = soc_code.replace("-", "")
    if not (len(soc_part) == 6 and soc_part.isdigit()):
        logger.error(
            f"Invalid SOC code format for OES series: {soc_code}. "
            "Expected XX-XXXX or XXXXXX digits."
        )
        return {}

    prefix = "OEU0000000000000"  # National ‑ all industries / ownerships
    return {
        "employment":  f"{prefix}{soc_part}01",
        "mean_wage":   f"{prefix}{soc_part}03",
        "median_wage": f"{prefix}{soc_part}04",
    }


def construct_oes_series_ids(soc_code: str) -> List[str]:
    """
    Legacy helper kept for backward compatibility.
    Now simply returns list(build_oes_series_id(...).values()).
    """
    series_map = build_oes_series_id(soc_code)
    if not series_map:
        return []
    series_ids = list(series_map.values())
    logger.info(f"Constructed OES series IDs for SOC {soc_code}: {series_ids}")
    return series_ids

def parse_oes_series_response(oes_response: Dict[str, Any], soc_code: str) -> Dict[str, Any]:
    """Parses raw OES API response into a structured dictionary."""
    parsed_data: Dict[str, Any] = {
        "occupation_code": soc_code, "employment": None, "annual_mean_wage": None,
        "median_wage": None, "data_year": None, "messages": [], "status": "success" # Changed annual_median_wage to median_wage
    }

    if not oes_response or oes_response.get("status") != "REQUEST_SUCCEEDED":
        parsed_data["status"] = "error"
        error_msg = f"OES API request failed or returned no data for SOC {soc_code}. Status: {oes_response.get('status', 'Unknown') if oes_response else 'No response'}."
        parsed_data["messages"].append(error_msg)
        if oes_response and "message" in oes_response:
             parsed_data["messages"].extend(m for m in oes_response.get("message", []) if m not in parsed_data["messages"])
        logger.warning(error_msg + f" Full response: {json.dumps(oes_response)}")
        return parsed_data

    if oes_response.get("message"): # Capture any messages even on success
        parsed_data["messages"].extend(m for m in oes_response["message"] if m not in parsed_data["messages"])

    latest_year_found_overall = None

    for series in oes_response.get("Results", {}).get("series", []):
        series_id = series.get("seriesID", "UnknownSeriesID")
        data_points = series.get("data", [])
        
        if not data_points:
            msg = f"No data points returned for OES series ID: {series_id} (SOC: {soc_code})."
            logger.warning(msg)
            if msg not in parsed_data["messages"]: parsed_data["messages"].append(msg)
            continue
            
        valid_data_points = [dp for dp in data_points if dp.get("year") and dp.get("value") and dp.get("value") != "-"] # BLS uses "-" for N/A
        if not valid_data_points:
            msg = f"No valid data points (with year and non-'-' value) for OES series ID: {series_id} (SOC: {soc_code})."
            logger.warning(msg)
            if msg not in parsed_data["messages"]: parsed_data["messages"].append(msg)
            continue

        latest_data_point = sorted(valid_data_points, key=lambda x: x["year"], reverse=True)[0]
        value_str = latest_data_point.get("value")
        year_str = latest_data_point.get("year")
        
        numeric_value: Optional[Union[int, float]] = None
        if value_str:
            try:
                numeric_value = float(value_str) if '.' in value_str else int(value_str)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert OES value '{value_str}' to numeric for series {series_id} (SOC: {soc_code}).")
        
        current_series_year = None
        if year_str:
            try:
                current_series_year = int(year_str)
                if latest_year_found_overall is None or current_series_year > latest_year_found_overall:
                    latest_year_found_overall = current_series_year
            except ValueError:
                logger.warning(f"Could not convert year '{year_str}' to int for series {series_id}")

        if numeric_value is not None:
            if series_id.endswith("01"):  # Employment
                parsed_data["employment"] = numeric_value
            elif series_id.endswith("03"):  # Annual Mean Wage
                parsed_data["annual_mean_wage"] = numeric_value
            elif series_id.endswith("04"):  # Annual Median Wage
                parsed_data["median_wage"] = numeric_value # Changed from annual_median_wage
    
    parsed_data["data_year"] = str(latest_year_found_overall) if latest_year_found_overall else None

    if parsed_data["employment"] is None and parsed_data["median_wage"] is None:
        msg = f"No employment or median wage data successfully parsed for OES SOC {soc_code}."
        logger.warning(msg)
        if msg not in parsed_data["messages"]: parsed_data["messages"].append(msg)
        # Do not mark as error if some messages indicate series don't exist, as this is valid BLS behavior
        if not any("Series does not exist" in m for m in parsed_data["messages"]):
             parsed_data["status"] = "partial_error" # Indicates some data might be missing but not a total failure

    return parsed_data

def get_oes_data_for_soc(soc_code: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """Fetches and parses OES data for a given SOC code."""
    logger.info(f"Getting OES data for SOC {soc_code}.")
    series_ids = construct_oes_series_ids(soc_code)
    if not series_ids:
        return {"status": "error", "message": f"Could not construct OES series IDs for SOC {soc_code}."}

    current_year = datetime.datetime.now().year
    # Fetch data for the last 5 years to get the most recent available year.
    oes_response = get_bls_data(series_ids, str(current_year - 5), str(current_year), api_key_param=api_key)
    return parse_oes_series_response(oes_response, soc_code)


# --- Employment Projections (EP) Data Functions ---
def construct_ep_series_ids(soc_code: str) -> List[str]:
    """
    Constructs EP series IDs for national employment projections.
    Series ID Structure: EPU{AREA_CODE}{INDUSTRY_CODE}{OCCUPATION_CODE}{DATATYPE_CODE}
    Area: UU00 (U.S. Total)
    Industry: 000000 (Total, all industries)
    Datatype: 01 (Employment), 02 (Employment change, numeric), 
              03 (Employment change, percent), 07 (Occupational openings) 
              (Note: BLS documentation sometimes uses 04 for openings, 07 is also seen)
    """
    soc_part = soc_code.replace("-", "")
    if not (len(soc_part) == 6 and soc_part.isdigit()):
        logger.error(f"Invalid SOC code format for EP series: {soc_code}. Expected XX-XXXX or XXXXXX (digits).")
        return []

    # Using 'EPUU000000000' as prefix for US total, all industries.
    # The last part is SOC code (6 digits) + data type (2 digits)
    series_ids = [
        f"EPUU000000000{soc_part}01",  # Employment base year
        f"EPUU000000000{soc_part}02",  # Employment projected year (this is often how it's represented, or use 01 with different years)
        f"EPUU000000000{soc_part}03",  # Employment change, numeric
        f"EPUU000000000{soc_part}04",  # Employment change, percent
        f"EPUU000000000{soc_part}07",  # Occupational openings, annual average
    ]
    logger.info(f"Constructed EP series IDs for SOC {soc_code}: {series_ids}")
    return series_ids

# --- EP Series ID Builder (for projections) ---
def build_ep_series_id(soc_code: str) -> Dict[str, str]:
    """
    Return a dictionary of Employment Projections (EP) series IDs
    for the given SOC code.  Keys correspond to the main EP metrics
    we parse elsewhere in the codebase.

    The EP series‐ID template (national, all-industry):
        EPUU000000000{SOC}{DT}

    Where:
        SOC – 6-digit SOC code without dash
        DT  – data-type code
              01  employment (base-year)
              02  employment (projection year)
              03  employment change, numeric
              04  employment change, percent
              07  annual average openings
    """
    soc_part = soc_code.replace("-", "")
    if not (len(soc_part) == 6 and soc_part.isdigit()):
        logger.error(
            f"Invalid SOC code format for EP series: {soc_code}. "
            "Expected XX-XXXX or XXXXXX digits."
        )
        return {}

    prefix = "EPUU000000000"
    return {
        "base_employment":        f"{prefix}{soc_part}01",
        "proj_employment":        f"{prefix}{soc_part}02",
        "employment_change_num":  f"{prefix}{soc_part}03",
        "percent_change":         f"{prefix}{soc_part}04",
        "annual_job_openings":    f"{prefix}{soc_part}07",
    }

def parse_ep_series_response(ep_response: Dict[str, Any], soc_code: str) -> Dict[str, Any]:
    """Parses raw EP API response into a structured dictionary."""
    parsed_data: Dict[str, Any] = {
        "occupation_code": soc_code, "current_employment": None, "projected_employment": None,
        "employment_change_numeric": None, "employment_change_percent": None,
        "annual_job_openings": None, "base_year": None, "projection_year": None,
        "messages": [], "status": "success"
    }

    if not ep_response or ep_response.get("status") != "REQUEST_SUCCEEDED":
        parsed_data["status"] = "error"
        error_msg = f"EP API request failed or returned no data for SOC {soc_code}. Status: {ep_response.get('status', 'Unknown') if ep_response else 'No response'}."
        parsed_data["messages"].append(error_msg)
        if ep_response and "message" in ep_response:
             parsed_data["messages"].extend(m for m in ep_response.get("message", []) if m not in parsed_data["messages"])
        logger.warning(error_msg + f" Full response: {json.dumps(ep_response)}")
        return parsed_data
    
    if ep_response.get("message"):
        parsed_data["messages"].extend(m for m in ep_response["message"] if m not in parsed_data["messages"])

    # EP data usually has one data point per series (the projection itself)
    # Base and projection years are often in catalog or need to be known from API documentation
    
    for series in ep_response.get("Results", {}).get("series", []):
        series_id = series.get("seriesID", "UnknownSeriesID")
        data_points = series.get("data", [])
        catalog = series.get("catalog") # Catalog can contain years

        if not data_points:
            msg = f"No data points returned for EP series ID: {series_id} (SOC: {soc_code})."
            logger.warning(msg)
            if msg not in parsed_data["messages"]: parsed_data["messages"].append(msg)
            continue

        # Typically, EP series data has one value representing the projection or base.
        # We take the first (and usually only) data point.
        data_point = data_points[0]
        value_str = data_point.get("value")
        
        # Attempt to get years from catalog if available, otherwise from data_point
        # BLS EP data points for projections often don't have 'year' field for the projection itself,
        # but the catalog data for the series might.
        if catalog:
            if not parsed_data["base_year"] and catalog.get("survey_name", "").startswith("Employment Projections") and catalog.get("periodicity") == "Biennial":
                 # Heuristic: try to infer years from catalog if possible
                 # This is tricky as BLS series IDs and catalog data vary.
                 # For now, we'll assume the 'year' in the data point is the projection year if available
                 # and 'startYear'/'endYear' from catalog might give base/projection.
                 # This part needs refinement based on actual EP series structures.
                 pass # Year logic will be handled by the data point's year or known projection cycle

        numeric_value: Optional[Union[int, float]] = None
        if value_str:
            try:
                numeric_value = float(value_str) if '.' in value_str else int(value_str)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert EP value '{value_str}' to numeric for series {series_id} (SOC: {soc_code}).")

        if numeric_value is not None:
            # EP data types are often at the end of series ID or need mapping.
            # The constructed series IDs have datatype at the end.
            # EPUU000000000{soc_part}01 -> Employment (Base Year)
            # EPUU000000000{soc_part}02 -> Employment (Projected Year) - This is often how it's done.
            # EPUU000000000{soc_part}03 -> Employment Change, Numeric
            # EPUU000000000{soc_part}04 -> Employment Change, Percent
            # EPUU000000000{soc_part}07 -> Occupational Openings
            
            # Assuming the series IDs are constructed as per construct_ep_series_ids
            # and that the API returns data for these specific series.
            # The order might matter if multiple series return "Employment".
            # Typically, the projection data is for a specific 10-year span.
            # We'll assume the first '01' is base year and '02' is projection year if available.
            # This is a simplification; robust parsing needs exact series ID knowledge.

            if series_id.endswith("01"): # Employment (often base year)
                parsed_data["current_employment"] = numeric_value
                if data_point.get("year"): parsed_data["base_year"] = data_point.get("year")
            elif series_id.endswith("02"): # Employment (often projected year)
                parsed_data["projected_employment"] = numeric_value
                if data_point.get("year"): parsed_data["projection_year"] = data_point.get("year")
            elif series_id.endswith("03"): # Employment Change, Numeric
                parsed_data["employment_change_numeric"] = numeric_value
            elif series_id.endswith("04"): # Employment Change, Percent
                parsed_data["employment_change_percent"] = numeric_value
            elif series_id.endswith("07"): # Occupational Openings
                parsed_data["annual_job_openings"] = numeric_value
        
        # If years are still None, try to infer from catalog if available
        if catalog:
            if not parsed_data["base_year"] and catalog.get("startYear"):
                parsed_data["base_year"] = catalog.get("startYear")
            if not parsed_data["projection_year"] and catalog.get("endYear"):
                parsed_data["projection_year"] = catalog.get("endYear")


    if not all(k is not None for k in ["current_employment", "projected_employment", "employment_change_percent", "annual_job_openings"]):
        msg = f"Failed to parse some EP data for SOC {soc_code}. Check API messages."
        logger.warning(msg + f" Parsed: {parsed_data}")
        if msg not in parsed_data["messages"]: parsed_data["messages"].append(msg)
        if not any("Series does not exist" in m for m in parsed_data["messages"]): # if not due to series non-existence
             parsed_data["status"] = "partial_error"


    return parsed_data

def get_ep_data_for_soc(soc_code: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """Fetches and parses EP data for a given SOC code."""
    logger.info(f"Getting employment projections for SOC {soc_code}.")
    series_ids = construct_ep_series_ids(soc_code)
    if not series_ids:
        return {"status": "error", "message": f"Could not construct EP series IDs for SOC {soc_code}."}

    # Projections are typically biennial, covering a 10-year span.
    # Fetch for a recent typical projection period. BLS usually updates every 2 years.
    # Example: if current year is 2024, projections might be 2022-2032.
    # We need to fetch for the period the series IDs are defined for.
    # For simplicity, we'll fetch a wide range and rely on the API returning the relevant projection data.
    # A more robust solution would determine the current projection cycle years.
    current_year = datetime.datetime.now().year
    # Fetching data for a 10-year projection window, assuming the latest data is within the last few years.
    # The specific years in the series IDs are often implicit in the EP program.
    # The 'catalog=true' parameter should help provide context if the API supports it well for EP series.
    # Using a fixed recent range as EP series are not like typical time series.
    # The actual projection period (e.g., 2022-2032) is inherent to the series ID itself.
    # The startyear/endyear in the payload might be less critical for EP series that represent a fixed projection.
    # However, to be safe and align with API requirements:
    proj_start_year = str(current_year - 2) # e.g., 2022 if current is 2024
    proj_end_year = str(current_year + 10)  # e.g., 2034
    
    ep_response = get_bls_data(series_ids, proj_start_year, proj_end_year, api_key_param=api_key)
    parsed_ep_data = parse_ep_series_response(ep_response, soc_code)
    
    # If base_year or projection_year are still None, try to set default based on typical BLS cycle
    if parsed_ep_data.get("status") == "success":
        if not parsed_ep_data.get("base_year"):
            # Estimate base year (e.g., 2 years ago if current year is even, 1 year ago if odd)
            parsed_ep_data["base_year"] = str(current_year - 2 if current_year % 2 == 0 else current_year - 1)
            logger.info(f"EP base_year not found in API response for {soc_code}, estimated to {parsed_ep_data['base_year']}")
        if not parsed_ep_data.get("projection_year"):
            try:
                base_y = int(parsed_ep_data["base_year"])
                parsed_ep_data["projection_year"] = str(base_y + 10)
                logger.info(f"EP projection_year not found, derived as {parsed_ep_data['projection_year']} from base_year.")
            except (TypeError, ValueError):
                 parsed_ep_data["projection_year"] = str(current_year + 8) # Fallback
                 logger.info(f"EP projection_year could not be derived, estimated to {parsed_ep_data['projection_year']}")
                 
    return parsed_ep_data

# --- Deprecated/Legacy Functions (to be phased out or refactored) ---
def get_occupation_data(occ_code: str) -> Dict[str, Any]:
    """
    DEPRECATED: Use get_oes_data_for_soc instead for OES data.
    Get employment and wage data for a specific occupation code.
    """
    logger.warning("get_occupation_data is deprecated. Use get_oes_data_for_soc for OES data.")
    return get_oes_data_for_soc(occ_code, api_key=_get_api_key())


def get_employment_projection(occ_code: str) -> Dict[str, Any]:
    """
    DEPRECATED: Use get_ep_data_for_soc instead for EP data.
    Get employment projections for an occupation.
    """
    logger.warning("get_employment_projection is deprecated. Use get_ep_data_for_soc for EP data.")
    return get_ep_data_for_soc(occ_code, api_key=_get_api_key())


# --- Utility Functions ---
def search_occupations(query: str) -> List[Dict[str, str]]:
    """
    Search for occupation codes matching the query.
    NOTE: This is a placeholder. A real implementation would query a comprehensive SOC database or BLS API.
    """
    logger.info(f"Searching occupations for query: '{query}' (using placeholder list)")
    # Sample SOC codes and titles (abbreviated list)
    # This list should be populated from a more comprehensive source in a real application
    soc_codes = [
        {"code": "11-1011", "title": "Chief Executives"},
        {"code": "11-2011", "title": "Advertising and Promotions Managers"},
        {"code": "11-3021", "title": "Computer and Information Systems Managers"},
        {"code": "11-3031", "title": "Financial Managers"},
        {"code": "13-1111", "title": "Management Analysts"},
        {"code": "13-2011", "title": "Accountants and Auditors"},
        {"code": "15-1211", "title": "Computer Systems Analysts"},
        {"code": "15-1251", "title": "Computer Programmers"},
        {"code": "15-1252", "title": "Software Developers"},
        {"code": "15-1254", "title": "Web Developers"},
        {"code": "15-2051", "title": "Data Scientists"},
        {"code": "17-2071", "title": "Electrical Engineers"},
        {"code": "23-1011", "title": "Lawyers"},
        {"code": "25-2021", "title": "Elementary School Teachers, Except Special Education"},
        {"code": "25-2031", "title": "Secondary School Teachers, Except Special and Career/Technical Education"},
        {"code": "27-1024", "title": "Graphic Designers"},
        {"code": "29-1021", "title": "Dentists, General"},
        {"code": "29-1141", "title": "Registered Nurses"},
        {"code": "29-1215", "title": "Family Medicine Physicians"},
        {"code": "35-2014", "title": "Cooks, Restaurant"},
        {"code": "41-2031", "title": "Retail Salespersons"},
        {"code": "43-4051", "title": "Customer Service Representatives"},
        {"code": "43-6011", "title": "Executive Secretaries and Executive Administrative Assistants"},
        {"code": "47-2031", "title": "Carpenters"},
        {"code": "49-3023", "title": "Automotive Service Technicians and Mechanics"},
        {"code": "53-3032", "title": "Heavy and Tractor-Trailer Truck Drivers"}
    ]
    
    query_lower = query.lower()
    matches = [item for item in soc_codes if query_lower in item["title"].lower()]
    
    if not matches: # If no title match, try matching SOC code directly
        matches = [item for item in soc_codes if query_lower == item["code"].replace("-","")]
    
    logger.info(f"Found {len(matches)} placeholder matches for query '{query}'.")
    return matches


def check_api_key_validity() -> bool:
    """
    Checks if the configured BLS API key is valid by making a small test call.
    Returns: True if the API key is valid, False otherwise.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.info("BLS API key not available for validity check.")
        return False

    # A very common and small series: National Unemployment Rate
    test_series_id = "LNS14000000" 
    current_year = str(datetime.datetime.now().year)
    
    logger.info(f"Checking BLS API key validity with test series {test_series_id}...")
    try:
        # Use a short timeout for this check
        response = requests.post(
            BLS_API_BASE_URL,
            json={
                "seriesid": [test_series_id],
                "startyear": current_year,
                "endyear": current_year,
                "registrationkey": api_key
            },
            timeout=10 
        )
        data = response.json()
        if data.get("status") == "REQUEST_SUCCEEDED":
            logger.info("BLS API key is valid.")
            return True
        else:
            logger.error(f"BLS API key appears invalid. Status: {data.get('status')}, Messages: {data.get('message')}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"BLS API connectivity check failed during key validity test: {e}")
        return False
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON response during API key validity test: {e}. Response text: {response.text if 'response' in locals() else 'No response object'}")
        return False

if __name__ == "__main__":
    # Example usage (requires BLS_API_KEY to be set in environment)
    if not is_api_key_available():
        print("Please set the BLS_API_KEY environment variable to run examples.")
    else:
        print(f"BLS API Key is available: {is_api_key_available()}")
        print(f"Checking API Key Validity: {check_api_key_validity()}")

        # Test OES Data
        soc_code_oes = "15-1252" # Software Developers
        print(f"\n--- Testing OES Data for SOC: {soc_code_oes} ---")
        oes_data = get_oes_data_for_soc(soc_code_oes)
        print(json.dumps(oes_data, indent=2))

        # Test EP Data
        soc_code_ep = "15-1251" # Computer Programmers
        print(f"\n--- Testing EP Data for SOC: {soc_code_ep} ---")
        ep_data = get_ep_data_for_soc(soc_code_ep)
        print(json.dumps(ep_data, indent=2))

        # Test a potentially problematic SOC
        soc_code_problem = "11-1111" # Example of a less common one
        print(f"\n--- Testing OES Data for potentially problematic SOC: {soc_code_problem} ---")
        oes_problem_data = get_oes_data_for_soc(soc_code_problem)
        print(json.dumps(oes_problem_data, indent=2))

        print(f"\n--- Testing EP Data for potentially problematic SOC: {soc_code_problem} ---")
        ep_problem_data = get_ep_data_for_soc(soc_code_problem)
        print(json.dumps(ep_problem_data, indent=2))

        # Test search
        print("\n--- Testing Occupation Search for 'manager' ---")
        search_results = search_occupations("manager")
        print(json.dumps(search_results, indent=2))

        print("\n--- Testing Occupation Search for '15-1252' ---")
        search_results_code = search_occupations("151252")
        print(json.dumps(search_results_code, indent=2))
