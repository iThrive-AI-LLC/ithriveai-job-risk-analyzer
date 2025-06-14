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
            if hasattr(st, 'secrets') and callable(st.secrets.get): # Check if st.secrets is usable
                api_key = st.secrets.get("api_keys", {}).get("BLS_API_KEY")
        except Exception: 
            pass # st.secrets might not be available if not in Streamlit context
    
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
        
        data_for_chunk = None # To store response for this chunk

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(BLS_API_BASE_URL, json=payload, timeout=30) 
                response.raise_for_status()  
                
                data_for_chunk = response.json()
                
                if data_for_chunk.get("status") != "REQUEST_SUCCEEDED":
                    error_message = f"BLS API request not successful for chunk {chunk_idx + 1}. Status: {data_for_chunk.get('status')}. Messages: {data_for_chunk.get('message')}"
                    logger.error(error_message)
                    if attempt == MAX_RETRIES - 1: 
                        # Store messages from the API if available, even on failure
                        if "message" in data_for_chunk and data_for_chunk["message"]:
                            all_results_data["message"].extend(m for m in data_for_chunk["message"] if m not in all_results_data["message"])
                        all_results_data["status"] = "error" # Mark overall status as error if any chunk fails definitively
                        # Do not return immediately, try other chunks if any. The final check will determine overall status.
                        break 
                else: # Successful status from BLS for this chunk
                    if "Results" in data_for_chunk and "series" in data_for_chunk["Results"]:
                        all_results_data["Results"]["series"].extend(data_for_chunk["Results"]["series"])
                    if "message" in data_for_chunk and data_for_chunk["message"]:
                         all_results_data["message"].extend(m for m in data_for_chunk["message"] if m not in all_results_data["message"])
                    logger.info(f"Successfully fetched data for chunk {chunk_idx + 1}.")
                    break  # Exit retry loop on success for this chunk

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTPError on attempt {attempt + 1} for chunk {chunk_idx+1}: {e}. Response: {e.response.text if e.response else 'No response text'}")
                if e.response is not None and e.response.status_code == 400: 
                     all_results_data["status"] = "error"
                     all_results_data["message"].append(f"BLS API Bad Request for chunk {chunk_idx+1}: {e.response.text if e.response else 'No response text'}. Series: {', '.join(series_chunk)}")
                     break # Don't retry on 400
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
                all_results_data["status"] = "error" # Mark overall status as error
                all_results_data["message"].append(f"Failed to fetch data for series chunk {chunk_idx+1} after {MAX_RETRIES} attempts. Last API status: {data_for_chunk.get('status') if data_for_chunk else 'No response'}, Messages: {data_for_chunk.get('message') if data_for_chunk else 'N/A'}")
        
        if len(series_chunks) > 1 and chunk_idx < len(series_chunks) - 1:
            time.sleep(1) # Small delay between chunks if multiple

    # If any chunk failed and marked status as error, the overall status is error.
    # Otherwise, it remains REQUEST_SUCCEEDED (initial value).
    if not all_results_data["Results"]["series"] and all_results_data["status"] == "REQUEST_SUCCEEDED":
        logger.warning(f"No series data found in BLS response for IDs: {series_ids}, despite API reporting overall success. Check individual series messages.")
        # Messages from API (like "Series does not exist") would be in all_results_data["message"]
        
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
        "messages": list(set(oes_response.get("message", []))) # Use set to remove duplicates
    }

    if oes_response.get("status") != "REQUEST_SUCCEEDED":
        logger.warning(f"OES response status not successful for SOC {soc_code}: {oes_response.get('message', 'No message')}")
        # Keep messages from the API response
        return parsed_data 

    latest_year_found = None

    for series in oes_response.get("Results", {}).get("series", []):
        series_id = series.get("seriesID")
        data_points = series.get("data", [])
        
        if not data_points:
            logger.warning(f"No data points returned for OES series ID: {series_id} for SOC {soc_code}")
            # Add a message if not already present from API
            msg = f"No data points found for series {series_id}."
            if msg not in parsed_data["messages"]:
                 parsed_data["messages"].append(msg)
            continue
            
        # OES data is annual, so we take the latest available year's data point.
        # Sort by year descending, then take the first.
        valid_data_points = [dp for dp in data_points if dp.get("year") and dp.get("value")]
        if not valid_data_points:
            logger.warning(f"No valid data points (with year and value) for OES series ID: {series_id}")
            continue

        latest_data_point = sorted(valid_data_points, key=lambda x: x["year"], reverse=True)[0]
        value_str = latest_data_point.get("value")
        year_str = latest_data_point.get("year")
        
        numeric_value: Optional[Union[int, float]] = None
        if value_str is not None:
            try:
                # BLS values are strings, attempt conversion
                numeric_value = float(value_str) if '.' in value_str else int(value_str)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert OES value '{value_str}' to numeric for series {series_id} (SOC: {soc_code}).")
        
        if year_str:
            try:
                current_series_year = int(year_str)
                if latest_year_found is None or current_series_year > latest_year_found:
                    latest_year_found = current_series_year
            except ValueError:
                 logger.warning(f"Invalid year format '{year_str}' for OES series {series_id}.")


        if series_id.endswith("01"): 
            parsed_data["employment"] = numeric_value
        elif series_id.endswith("03"): 
            parsed_data["annual_mean_wage"] = numeric_value
        elif series_id.endswith("04"): 
            parsed_data["annual_median_wage"] = numeric_value
    
    if latest_year_found:
        parsed_data["data_year"] = str(latest_year_found)

    if parsed_data["employment"] is None and parsed_data["annual_median_wage"] is None:
        logger.warning(f"No employment or median wage data successfully parsed for OES SOC {soc_code}.")
        
    return parsed_data

def get_oes_data_for_soc(soc_code: str, start_year: str, end_year: str) -> Dict[str, Any]:
    """
    Fetches and parses OES data for a given SOC code.
    Queries for the last 5-7 available years to improve chances of getting data.
    """
    # current_year = datetime.datetime.now().year
    # # OES data is typically annual and might have a 1-2 year lag.
    # # Querying a range like last 5-7 years up to "last year" is safer.
    # end_year_oes = str(current_year - 1) 
    # start_year_oes = str(current_year - 7) 
    # if int(start_year_oes) > int(end_year_oes): # Should not happen with current_year - 1 and current_year - 7
    #     start_year_oes = end_year_oes

    logger.info(f"Getting OES data for SOC {soc_code} from {start_year} to {end_year}.")
    series_ids = construct_oes_series_ids(soc_code)
    if not series_ids:
        return {"status": "error", "message": f"Could not construct OES series IDs for SOC {soc_code}."}
    
    raw_oes_data = get_bls_data(series_ids, start_year, end_year)
    
    # Add status to the parsed data
    parsed_response = parse_oes_series_response(raw_oes_data, soc_code)
    parsed_response["status"] = "success" if raw_oes_data.get("status") == "REQUEST_SUCCEEDED" and (parsed_response.get("employment") is not None or parsed_response.get("annual_median_wage") is not None) else "error"
    if raw_oes_data.get("status") != "REQUEST_SUCCEEDED" and "API error" not in parsed_response["messages"]: # Add generic API error if not specific
        parsed_response["messages"].append(f"API error during OES fetch: {raw_oes_data.get('message', 'Unknown API error')}")
    return {"status": parsed_response["status"], "data": parsed_response, "messages": parsed_response["messages"]}


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
    # Series ID format for EP: EPU + U (Unadjusted) + SOC (6 digits) + Datatype (2 digits)
    # Example: EPUU15125201 for Software Developers, Base Year Employment
    series_ids = [
        f"EPUU{soc_part}01",  # Base Year Employment
        f"EPUU{soc_part}02",  # Projected Year Employment
        f"EPUU{soc_part}03",  # Employment Change, Numeric
        f"EPUU{soc_part}04",  # Employment Change, Percent
        f"EPUU{soc_part}07",  # Occupational Openings
    ]
    logger.info(f"Constructed EP series IDs for {soc_code}: {series_ids}")
    return series_ids

def parse_ep_series_response(ep_response: Dict[str, Any], soc_code: str) -> Dict[str, Any]:
    """Parses raw EP API response into a structured dictionary."""
    parsed_data: Dict[str, Any] = {
        "occupation_code": soc_code,
        "current_employment": None,
        "projected_employment": None,
        "employment_change_numeric": None,
        "employment_change_percent": None,
        "annual_job_openings": None,
        "base_year": None,
        "projection_year": None,
        "messages": list(set(ep_response.get("message", []))) # Use set to remove duplicates
    }

    if ep_response.get("status") != "REQUEST_SUCCEEDED":
        logger.warning(f"EP response status not successful for SOC {soc_code}: {ep_response.get('message', 'No message')}")
        return parsed_data # Keep messages from API response

    # EP data is typically a single projection period.
    # We need to find the catalog data for base and projection years.
    # And then find the data point for that period.
    
    base_year_found: Optional[str] = None
    projection_year_found: Optional[str] = None

    for series in ep_response.get("Results", {}).get("series", []):
        series_id = series.get("seriesID")
        catalog = series.get("catalog")
        data_points = series.get("data", [])

        if not data_points:
            logger.warning(f"No data points returned for EP series ID: {series_id} for SOC {soc_code}")
            msg = f"No data points found for series {series_id}."
            if msg not in parsed_data["messages"]:
                 parsed_data["messages"].append(msg)
            continue
        
        # EP data usually has one data point per series for the projection period.
        data_point = data_points[0] if data_points else None
        if not data_point: continue

        value_str = data_point.get("value")
        numeric_value: Optional[Union[int, float]] = None
        if value_str is not None:
            try:
                numeric_value = float(value_str) if '.' in value_str else int(value_str)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert EP value '{value_str}' to numeric for series {series_id} (SOC: {soc_code}).")

        # Determine base and projection years from catalog if available
        if catalog:
            if catalog.get("survey_name", "").startswith("Employment Projections"):
                if catalog.get("periodicity") == "Biennial" and catalog.get("base_period") and catalog.get("projection_period"):
                    base_year_found = catalog.get("base_year") or catalog.get("base_period") # base_year is preferred
                    projection_year_found = catalog.get("projection_year") or catalog.get("projection_period") # projection_year is preferred
                    # Ensure they are just years
                    if base_year_found and len(base_year_found) > 4: base_year_found = base_year_found[:4]
                    if projection_year_found and len(projection_year_found) > 4: projection_year_found = projection_year_found[:4]

        # Assign values based on datatype code in series ID
        if series_id.endswith("01"): # Base Year Employment
            parsed_data["current_employment"] = numeric_value
        elif series_id.endswith("02"): # Projected Year Employment
            parsed_data["projected_employment"] = numeric_value
        elif series_id.endswith("03"): # Employment Change, Numeric
            parsed_data["employment_change_numeric"] = numeric_value
        elif series_id.endswith("04"): # Employment Change, Percent
            parsed_data["employment_change_percent"] = numeric_value
        elif series_id.endswith("07"): # Occupational Openings
            parsed_data["annual_job_openings"] = numeric_value
            
    # Set the years if found from catalog
    if base_year_found: parsed_data["base_year"] = base_year_found
    if projection_year_found: parsed_data["projection_year"] = projection_year_found

    # Basic validation: if projected employment is present, base employment should also be.
    if parsed_data["projected_employment"] is not None and parsed_data["current_employment"] is None:
        logger.warning(f"EP data for SOC {soc_code} has projected employment but missing base employment.")
        # This could indicate a data issue or parsing problem.

    if all(v is None for k, v in parsed_data.items() if k not in ["occupation_code", "messages"]):
        logger.warning(f"No EP data values successfully parsed for SOC {soc_code}.")

    return parsed_data

def get_ep_data_for_soc(soc_code: str) -> Dict[str, Any]:
    """
    Fetches and parses Employment Projections (EP) data for a given SOC code.
    EP data is typically biennial, so we query for a recent period.
    """
    # EP data is typically a 10-year projection, updated biennially.
    # We fetch for a period that should contain the latest projection.
    # Example: If current year is 2024, BLS might have 2022-2032 projections.
    # Querying for a single recent year like "current_year - 1" should get the latest projection data.
    current_year = datetime.datetime.now().year
    # Projections are usually for a future period, so we query a recent past year to get the "latest" projection data.
    # The start and end year for EP series often represent the *publication* period of the projection, not the projection span itself.
    # For EP, it's often better to query a single recent year or a small range.
    # Let's try querying for the last 2-3 years to catch the latest biennial release.
    query_year = str(current_year - 2) # Try to get data published in the last couple of years.
    
    logger.info(f"Getting employment projections for SOC {soc_code} around year {query_year}.")
    series_ids = construct_ep_series_ids(soc_code)
    if not series_ids:
        return {"status": "error", "message": f"Could not construct EP series IDs for SOC {soc_code}."}
    
    # For EP, the 'startyear' and 'endyear' for get_bls_data often refer to the publication/reference period
    # of the projection data, not the projection span itself.
    # The actual projection span (e.g., 2022-2032) is usually part of the series catalog.
    # We'll query for a recent period to get the latest available projection.
    raw_ep_data = get_bls_data(series_ids, query_year, query_year) # Querying a single recent year
    
    parsed_response = parse_ep_series_response(raw_ep_data, soc_code)
    
    # Determine overall status based on whether key projection data was found
    key_projection_fields = ["current_employment", "projected_employment", "employment_change_percent", "annual_job_openings"]
    if raw_ep_data.get("status") == "REQUEST_SUCCEEDED" and any(parsed_response.get(field) is not None for field in key_projection_fields):
        status = "success"
    else:
        status = "error"
        logger.warning(f"Failed to parse key EP data for SOC {soc_code}. Response: {parsed_response}")
        if "API error" not in parsed_response["messages"] and raw_ep_data.get("status") != "REQUEST_SUCCEEDED":
             parsed_response["messages"].append(f"API error during EP fetch: {raw_ep_data.get('message', 'Unknown API error')}")

    return {"status": status, "projections": parsed_response, "messages": parsed_response["messages"]}


# --- Occupation Search (Simplified) ---
def search_occupations(query: str) -> List[Dict[str, str]]:
    """
    Search for occupation codes matching the query.
    This is a simplified local search against a predefined list.
    In a real scenario, this might query a BLS API or a local SOC database.
    """
    from bls_job_mapper import TARGET_SOC_CODES # Import from bls_job_mapper to use its list
    
    query_lower = query.lower()
    matches: List[Dict[str, str]] = []

    # Prioritize exact matches
    for item in TARGET_SOC_CODES:
        if query_lower == item["title"].lower():
            matches.append({"soc_code": item["soc_code"], "title": item["title"]})
    
    # Add matches where query is a substring of the title
    for item in TARGET_SOC_CODES:
        if query_lower in item["title"].lower() and not any(m["soc_code"] == item["soc_code"] for m in matches):
            matches.append({"soc_code": item["soc_code"], "title": item["title"]})
            
    # Limit results if necessary (e.g., to top 10)
    logger.info(f"Found {len(matches)} potential SOC matches for query '{query}'. Returning up to 10.")
    return matches[:10]


# --- API Connectivity Check ---
@st.cache_data(ttl=300) # Cache for 5 minutes
def check_api_connectivity(api_key_to_check: Optional[str] = None) -> bool:
    """
    Check if the BLS API is accessible with the provided (or configured) API key.
    """
    key_to_use = api_key_to_check or _get_api_key()
    if not key_to_use:
        logger.warning("API connectivity check: No API key available.")
        return False
    try:
        # Test with a simple, common series ID that is likely to exist.
        # LAUCN040010000000005 = Unemployment rate in California (annual)
        # Using a very small date range to minimize data transfer.
        current_year_str = str(datetime.datetime.now().year - 1) # Use previous year for annual data
        test_data = get_bls_data(["LAUCN040010000000005"], current_year_str, current_year_str, api_key_param=key_to_use)
        
        if test_data.get("status") == "REQUEST_SUCCEEDED":
            logger.info("BLS API connectivity check successful.")
            return True
        else:
            logger.warning(f"BLS API connectivity check failed. Status: {test_data.get('status')}, Messages: {test_data.get('message')}")
            return False
    except Exception as e:
        logger.error(f"BLS API connectivity check encountered an exception: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    
    # Ensure API key is available for testing
    if not is_api_key_available():
        print("Please set the BLS_API_KEY environment variable for testing.")
    else:
        print("BLS API Key is available.")
        
        # Test API connectivity
        print(f"API Connectivity: {'OK' if check_api_connectivity() else 'Failed'}")

        # Test OES Data
        soc_code_oes = "15-1252"  # Software Developers
        print(f"\n--- Testing OES Data for SOC {soc_code_oes} ---")
        current_year = datetime.datetime.now().year
        oes_data = get_oes_data_for_soc(soc_code_oes, str(current_year - 3), str(current_year - 1))
        print(json.dumps(oes_data, indent=2))

        # Test EP Data
        soc_code_ep = "15-1252" # Software Developers
        print(f"\n--- Testing EP Data for SOC {soc_code_ep} ---")
        ep_data = get_ep_data_for_soc(soc_code_ep)
        print(json.dumps(ep_data, indent=2))

        # Test another EP code
        soc_code_ep_2 = "29-1141" # Registered Nurses
        print(f"\n--- Testing EP Data for SOC {soc_code_ep_2} ---")
        ep_data_2 = get_ep_data_for_soc(soc_code_ep_2)
        print(json.dumps(ep_data_2, indent=2))

        # Test a SOC that might have limited data or issues
        soc_code_problematic = "11-1011" # Chief Executives (often has less detailed public data)
        print(f"\n--- Testing Problematic OES Data for SOC {soc_code_problematic} ---")
        oes_problem_data = get_oes_data_for_soc(soc_code_problematic, str(current_year - 3), str(current_year - 1))
        print(json.dumps(oes_problem_data, indent=2))
        
        print(f"\n--- Testing Problematic EP Data for SOC {soc_code_problematic} ---")
        ep_problem_data = get_ep_data_for_soc(soc_code_problematic)
        print(json.dumps(ep_problem_data, indent=2))
        
        # Test occupation search
        print("\n--- Testing Occupation Search for 'developer' ---")
        search_results = search_occupations("developer")
        print(json.dumps(search_results, indent=2))

        print("\n--- Testing Occupation Search for 'nurse' ---")
        search_results_nurse = search_occupations("nurse")
        print(json.dumps(search_results_nurse, indent=2))
