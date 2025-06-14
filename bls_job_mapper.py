"""
BLS Job Mapper Module

This module provides mapping between job titles and BLS Standard Occupational Classification (SOC) codes.
It fetches real BLS data using bls_connector, caches it in the database,
and generates AI displacement risk assessments based on job categories and BLS statistics.
It strictly avoids using any fictional or synthetic fallback data.
"""

import os
import json
import datetime
import logging
from typing import Dict, Any, List, Optional, Tuple, Union
import threading

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, MetaData, inspect, Text, TIMESTAMP
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Attempt to import the custom BLS API connector
try:
    import bls_connector
except ImportError:
    logging.critical("bls_connector.py not found. This module is essential for fetching BLS data.")
    # Define a stub if bls_connector is missing so the application can at least report this critical error.
    class bls_connector_stub: # type: ignore
        @staticmethod
        def get_bls_data(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return {"status": "error", "message": "bls_connector module not found."}
        @staticmethod
        def get_oes_data_for_soc(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return {"status": "error", "message": "bls_connector module not found."}
        @staticmethod
        def get_ep_data_for_soc(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return {"status": "error", "message": "bls_connector module not found."}
        @staticmethod
        def search_occupations(*args: Any, **kwargs: Any) -> List[Dict[str, str]]:
            return []
    bls_connector = bls_connector_stub() # type: ignore

# Configure logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    logger.propagate = False

# --- Database Setup ---
metadata = MetaData()
bls_job_data_table = Table(
    'bls_job_data', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('occupation_code', String(10), nullable=False, unique=True, index=True),
    Column('job_title', String(255), nullable=False),
    Column('standardized_title', String(255), nullable=False, index=True),
    Column('job_category', String(100)),
    Column('current_employment', Integer, nullable=True),
    Column('projected_employment', Integer, nullable=True),
    Column('employment_change_numeric', Integer, nullable=True),
    Column('percent_change', Float, nullable=True),
    Column('annual_job_openings', Integer, nullable=True),
    Column('median_wage', Float, nullable=True),
    Column('mean_wage', Float, nullable=True),
    Column('oes_data_year', String(4), nullable=True),
    Column('ep_base_year', String(4), nullable=True),
    Column('ep_proj_year', String(4), nullable=True),
    Column('raw_oes_data_json', Text, nullable=True),
    Column('raw_ep_data_json', Text, nullable=True),
    Column('last_api_fetch', String(10), nullable=False),
    Column('last_updated', String(10), nullable=False, default=lambda: datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d'))
)

_engine_instance: Optional[sqlalchemy.engine.Engine] = None
_engine_lock = threading.Lock()

def get_db_engine(force_new: bool = False) -> sqlalchemy.engine.Engine:
    """Creates and returns a SQLAlchemy engine, ensuring singleton-like behavior."""
    global _engine_instance
    with _engine_lock:
        if _engine_instance is None or force_new:
            database_url = os.environ.get('DATABASE_URL')
            if not database_url:
                try:
                    import streamlit as st # type: ignore
                    database_url = st.secrets.get("database", {}).get("DATABASE_URL")
                except (ImportError, AttributeError):
                    pass

            if not database_url:
                logger.critical("DATABASE_URL environment variable or secret not set. Cannot connect to database.")
                raise ValueError("DATABASE_URL not configured.")

            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql://', 1)

            connect_args = {}
            if 'postgresql' in database_url:
                connect_args = {
                    "connect_timeout": 15, "keepalives": 1, "keepalives_idle": 30,
                    "keepalives_interval": 10, "keepalives_count": 5, "sslmode": 'require'
                }
            try:
                db_host_info = database_url.split('@')[-1] if '@' in database_url else database_url
                logger.info(f"Creating new database engine instance for URL ending with: ...@{db_host_info}")
                _engine_instance = create_engine(database_url, connect_args=connect_args, pool_pre_ping=True, pool_recycle=1800, echo=False)
                
                with _engine_instance.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("Database engine created and connection tested successfully.")
                
                try:
                    metadata.create_all(_engine_instance, checkfirst=True)
                    logger.info(f"Table '{bls_job_data_table.name}' ensured to exist.")
                except Exception as e_table:
                    logger.error(f"Failed to create table '{bls_job_data_table.name}': {e_table}", exc_info=True)

            except Exception as e:
                logger.error(f"Failed to create database engine or test connection: {e}", exc_info=True)
                _engine_instance = None
                raise
        return _engine_instance

# --- Constants and Mappings ---
JOB_TITLE_TO_SOC_STATIC: Dict[str, str] = {
    "software developer": "15-1252", "software engineer": "15-1252", "programmer": "15-1251",
    "web developer": "15-1254", "registered nurse": "29-1141", "nurse": "29-1141",
    "teacher": "25-2021", "elementary school teacher": "25-2021", "high school teacher": "25-2031",
    "lawyer": "23-1011", "attorney": "23-1011", "doctor": "29-1221", "physician": "29-1221",
    "accountant": "13-2011", "project manager": "13-1199",
    "product manager": "11-2021", "marketing manager": "11-2021", "retail salesperson": "41-2031",
    "cashier": "41-2011", "customer service representative": "43-4051", "truck driver": "53-3032",
    "receptionist": "43-4171", "data scientist": "15-2051", "data analyst": "15-2051",
    "business analyst": "13-1111", "financial analyst": "13-2051", "human resources specialist": "13-1071",
    "graphic designer": "27-1024", "police officer": "33-3051",
    "chef": "35-1011", "cook": "35-2014", "waiter": "35-3031", "waitress": "35-3031",
    "janitor": "37-2011", "administrative assistant": "43-6011", "executive assistant": "43-6011",
    "dental hygienist": "29-1292", "electrician": "47-2111", "plumber": "47-2152",
    "carpenter": "47-2031", "construction worker": "47-2061", "mechanic": "49-3023",
    "automotive mechanic": "49-3023", "taxi driver": "53-3054", "uber driver": "53-3054",
    "journalist": "27-3023", "reporter": "27-3023", "writer": "27-3042",
    "editor": "27-3041", "photographer": "27-4021", "court reporter": "23-2011",
    "stenographer": "23-2011", "digital court reporter": "23-2011", "travel agent": "41-3041"
}

SOC_TO_CATEGORY: Dict[str, str] = {
    "11-": "Management Occupations", "13-": "Business and Financial Operations Occupations",
    "15-": "Computer and Mathematical Occupations", "17-": "Architecture and Engineering Occupations",
    "19-": "Life, Physical, and Social Science Occupations", "21-": "Community and Social Service Occupations",
    "23-": "Legal Occupations", "25-": "Educational Instruction and Library Occupations",
    "27-": "Arts, Design, Entertainment, Sports, and Media Occupations", "29-": "Healthcare Practitioners and Technical Occupations",
    "31-": "Healthcare Support Occupations", "33-": "Protective Service Occupations",
    "35-": "Food Preparation and Serving Related Occupations", "37-": "Building and Grounds Cleaning and Maintenance Occupations",
    "39-": "Personal Care and Service Occupations", "41-": "Sales and Related Occupations",
    "43-": "Office and Administrative Support Occupations", "45-": "Farming, Fishing, and Forestry Occupations",
    "47-": "Construction and Extraction Occupations", "49-": "Installation, Maintenance, and Repair Occupations",
    "51-": "Production Occupations", "53-": "Transportation and Material Moving Occupations",
    "00-0000": "Unknown or Unclassified" # For fallback
}

def standardize_job_title(title: str) -> str:
    """Standardize job title format for consistent mapping."""
    standardized = title.lower().strip()
    suffixes = [" i", " ii", " iii", " iv", " v", " specialist", " assistant", " associate", " senior", " junior", " lead"]
    for suffix in suffixes:
        if standardized.endswith(suffix):
            standardized = standardized[:-len(suffix)].strip()
            break
    return standardized

def get_job_category_from_soc(occupation_code: str) -> str:
    """Get the job category based on SOC code prefix."""
    if not occupation_code or not isinstance(occupation_code, str): return "General"
    for prefix, category in SOC_TO_CATEGORY.items():
        if occupation_code.startswith(prefix):
            return category
    return "General"

def find_soc_code_and_title(job_title_query: str, engine: sqlalchemy.engine.Engine) -> Tuple[Optional[str], str, str]:
    """Find SOC code and standardized title for a job title, checking DB first, then BLS API."""
    std_query_title = standardize_job_title(job_title_query)
    logger.info(f"Standardized '{job_title_query}' to '{std_query_title}' for SOC search.")

    # 1. Check static mapping
    if std_query_title in JOB_TITLE_TO_SOC_STATIC:
        soc = JOB_TITLE_TO_SOC_STATIC[std_query_title]
        category = get_job_category_from_soc(soc)
        # Attempt to get a more official title from DB if this was an alias
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT standardized_title FROM bls_job_data WHERE occupation_code = :soc LIMIT 1"), {"soc": soc}).fetchone()
                official_title = res[0] if res else job_title_query
            logger.info(f"Found SOC {soc} for '{std_query_title}' via static map. Official title: '{official_title}', Category: {category}")
            return soc, official_title, category
        except Exception as e_db_title:
            logger.warning(f"DB lookup for official title for SOC {soc} failed: {e_db_title}. Using query title.")
            return soc, job_title_query, category


    # 2. Search BLS API (via bls_connector)
    logger.info(f"SOC for '{std_query_title}' not in static map. Querying BLS API via bls_connector.")
    try:
        matches = bls_connector.search_occupations(job_title_query) # Use original query for broader search
        if matches:
            best_match = matches[0]
            soc = best_match["code"]
            official_title = best_match["title"]
            category = get_job_category_from_soc(soc)
            logger.info(f"Found SOC {soc} ('{official_title}') for '{job_title_query}' via BLS API. Category: {category}")
            
            # Cache this new mapping (optional, consider if JOB_TITLE_TO_SOC_STATIC should be dynamic)
            # JOB_TITLE_TO_SOC_STATIC[std_query_title] = soc 
            return soc, official_title, category
    except Exception as e_bls_search:
        logger.error(f"BLS API search for '{job_title_query}' failed: {e_bls_search}", exc_info=True)

    logger.warning(f"Could not find SOC code for '{job_title_query}'. Defaulting to 00-0000.")
    return "00-0000", job_title_query, "General"


def parse_oes_series_response(oes_data_raw: Dict[str, Any], soc_code: str) -> Dict[str, Any]:
    """Parses raw OES data from BLS API into a structured dictionary."""
    parsed_data: Dict[str, Any] = {
        "occupation_code": soc_code, "employment": None, "annual_mean_wage": None,
        "annual_median_wage": None, "data_year": None, "messages": [], "status": "success"
    }
    if not oes_data_raw or oes_data_raw.get("status") != "REQUEST_SUCCEEDED":
        parsed_data["status"] = "error"
        parsed_data["messages"].append(f"OES API request failed or returned no data. Status: {oes_data_raw.get('status', 'Unknown')}")
        if oes_data_raw and "message" in oes_data_raw:
             parsed_data["messages"].extend(oes_data_raw.get("message", []))
        logger.warning(f"OES API request failed for SOC {soc_code}: {json.dumps(oes_data_raw)}")
        return parsed_data

    series_data = oes_data_raw.get("Results", {}).get("series", [])
    if not series_data:
        parsed_data["status"] = "error"
        parsed_data["messages"].append("No 'series' data found in OES API response.")
        logger.warning(f"No series data in OES response for SOC {soc_code}: {json.dumps(oes_data_raw)}")
        return parsed_data
    
    # Store all messages from API
    if "message" in oes_data_raw:
        parsed_data["messages"].extend(oes_data_raw.get("message", []))

    for series in series_data:
        series_id = series.get("seriesID", "")
        data_points = series.get("data", [])
        if not data_points:
            msg = f"No data points found for series {series_id}."
            parsed_data["messages"].append(msg)
            logger.warning(f"{msg} SOC: {soc_code}")
            continue

        latest_data_point = data_points[0] # Assuming data is sorted with latest first
        value_str = latest_data_point.get("value")
        data_year = latest_data_point.get("year")
        
        if parsed_data["data_year"] is None and data_year:
             parsed_data["data_year"] = data_year

        if value_str:
            try:
                value = float(value_str)
                if series_id.endswith("01"): # Employment
                    parsed_data["employment"] = int(value)
                elif series_id.endswith("03"): # Annual Mean Wage
                    parsed_data["annual_mean_wage"] = value
                elif series_id.endswith("04"): # Annual Median Wage
                    parsed_data["annual_median_wage"] = value
            except ValueError:
                msg = f"Could not convert value '{value_str}' to float for series {series_id}."
                parsed_data["messages"].append(msg)
                logger.warning(f"{msg} SOC: {soc_code}")
        else:
            msg = f"No value found for series {series_id} in latest data point."
            parsed_data["messages"].append(msg)
            logger.warning(f"{msg} SOC: {soc_code}")
            
    if not any([parsed_data["employment"], parsed_data["annual_mean_wage"], parsed_data["annual_median_wage"]]):
        if parsed_data["status"] == "success": # Only change to error if not already error
            parsed_data["status"] = "error" # Mark as error if no key data points were parsed
        parsed_data["messages"].append("No key OES data points (employment, mean wage, median wage) could be parsed.")
        logger.warning(f"No key OES data points parsed for SOC {soc_code}.")

    return parsed_data


def parse_ep_series_response(ep_data_raw: Dict[str, Any], soc_code: str) -> Dict[str, Any]:
    """Parses raw EP data from BLS API into a structured dictionary."""
    parsed_data: Dict[str, Any] = {
        "occupation_code": soc_code, "current_employment": None, "projected_employment": None,
        "employment_change_numeric": None, "employment_change_percent": None,
        "annual_job_openings": None, "base_year": None, "projection_year": None,
        "messages": [], "status": "success"
    }

    if not ep_data_raw or ep_data_raw.get("status") != "REQUEST_SUCCEEDED":
        parsed_data["status"] = "error"
        parsed_data["messages"].append(f"EP API request failed or returned no data. Status: {ep_data_raw.get('status', 'Unknown')}")
        if ep_data_raw and "message" in ep_data_raw:
            parsed_data["messages"].extend(ep_data_raw.get("message", []))
        logger.warning(f"EP API request failed for SOC {soc_code}: {json.dumps(ep_data_raw)}")
        return parsed_data

    series_data = ep_data_raw.get("Results", {}).get("series", [])
    if not series_data:
        parsed_data["status"] = "error"
        parsed_data["messages"].append("No 'series' data found in EP API response.")
        logger.warning(f"No series data in EP response for SOC {soc_code}: {json.dumps(ep_data_raw)}")
        return parsed_data

    if "message" in ep_data_raw:
        parsed_data["messages"].extend(ep_data_raw.get("message", []))

    for series in series_data:
        series_id = series.get("seriesID", "")
        data_points = series.get("data", [])
        if not data_points:
            msg = f"No data points found for series {series_id}."
            parsed_data["messages"].append(msg)
            logger.warning(f"{msg} SOC: {soc_code}")
            continue
        
        latest_data_point = data_points[0]
        value_str = latest_data_point.get("value")
        base_year = latest_data_point.get("latestPeriodYear") # Using this as a proxy
        proj_year = latest_data_point.get("year")

        if parsed_data["base_year"] is None and base_year: # Assuming first series gives the right base year
            parsed_data["base_year"] = base_year
        if parsed_data["projection_year"] is None and proj_year:
            parsed_data["projection_year"] = proj_year
            
        if value_str:
            try:
                value = float(value_str)
                # EP Series ID suffixes: 01=Empl, 02=Empl Change Num, 03=Empl Change Percent, 04=Occupational Openings
                if series_id.endswith("01"): # Current/Base Employment
                    parsed_data["current_employment"] = int(value * 1000) # EP data is in thousands
                elif series_id.endswith("02"): # Employment Change Numeric
                    parsed_data["employment_change_numeric"] = int(value * 1000)
                elif series_id.endswith("03"): # Employment Change Percent
                    parsed_data["employment_change_percent"] = value
                elif series_id.endswith("04"): # Annual Job Openings
                    parsed_data["annual_job_openings"] = int(value * 1000)
                # Note: Projected employment is not directly available as a series, it's calculated.
                # We'll calculate it later if current_employment and change_numeric are available.
            except ValueError:
                msg = f"Could not convert value '{value_str}' to float for series {series_id}."
                parsed_data["messages"].append(msg)
                logger.warning(f"{msg} SOC: {soc_code}")
        else:
            msg = f"No value found for series {series_id} in latest data point."
            parsed_data["messages"].append(msg)
            logger.warning(f"{msg} SOC: {soc_code}")

    # Calculate projected employment if possible
    if parsed_data["current_employment"] is not None and parsed_data["employment_change_numeric"] is not None:
        parsed_data["projected_employment"] = parsed_data["current_employment"] + parsed_data["employment_change_numeric"]
    
    if not any([parsed_data["current_employment"], parsed_data["projected_employment"], parsed_data["employment_change_percent"], parsed_data["annual_job_openings"]]):
        if parsed_data["status"] == "success":
            parsed_data["status"] = "error"
        parsed_data["messages"].append("No key EP data points could be parsed.")
        logger.warning(f"No key EP data points parsed for SOC {soc_code}.")
        
    return parsed_data


def fetch_and_process_soc_data(soc_code: str, original_job_title: str, standardized_soc_title: str, job_category: str) -> Dict[str, Any]:
    """Fetches OES and EP data for a SOC, processes it, and prepares for DB storage."""
    current_time = datetime.datetime.now(datetime.timezone.utc)
    last_api_fetch_str = current_time.strftime('%Y-%m-%d')

    logger.info(f"Fetching OES data for SOC {soc_code} for years 2022-2024") # BLS typically has a lag
    oes_data_raw = bls_connector.get_oes_data_for_soc(soc_code, start_year="2022", end_year="2024")
    if oes_data_raw is None: # Handle case where connector might return None on severe error
        oes_data_raw = {"status": "error", "message": ["OES connector returned None."]}
    oes_data = parse_oes_series_response(oes_data_raw, soc_code)
    
    if oes_data['status'] == 'error':
        logger.warning(f"Failed to fetch or parse OES data for SOC {soc_code}. API Response: {json.dumps(oes_data_raw)}")

    logger.info(f"Fetching EP data for SOC {soc_code}")
    ep_data_raw = bls_connector.get_ep_data_for_soc(soc_code)
    if ep_data_raw is None: # Handle case where connector might return None
        ep_data_raw = {"status": "error", "message": ["EP connector returned None."]}
    ep_data = parse_ep_series_response(ep_data_raw, soc_code)

    if ep_data['status'] == 'error':
        logger.warning(f"Failed to fetch or parse EP data for SOC {soc_code}. API Response: {json.dumps(ep_data_raw)}")

    # Prepare data for database insertion/update
    # Ensure all fields match the table definition, defaulting to None if data is missing
    processed_data = {
        "occupation_code": soc_code,
        "job_title": original_job_title, # Store the original search term that led to this SOC
        "standardized_title": standardized_soc_title,
        "job_category": job_category,
        "current_employment": ep_data.get("current_employment"), # From EP data
        "projected_employment": ep_data.get("projected_employment"), # Calculated in parse_ep_series_response
        "employment_change_numeric": ep_data.get("employment_change_numeric"),
        "percent_change": ep_data.get("employment_change_percent"),
        "annual_job_openings": ep_data.get("annual_job_openings"),
        "median_wage": oes_data.get("annual_median_wage"), # From OES data
        "mean_wage": oes_data.get("annual_mean_wage"), # From OES data
        "oes_data_year": oes_data.get("data_year"),
        "ep_base_year": ep_data.get("base_year"),
        "ep_proj_year": ep_data.get("projection_year"),
        "raw_oes_data_json": json.dumps(oes_data_raw) if isinstance(oes_data_raw, dict) else (oes_data_raw if isinstance(oes_data_raw, str) else '{}'),
        "raw_ep_data_json": json.dumps(ep_data_raw) if isinstance(ep_data_raw, dict) else (ep_data_raw if isinstance(ep_data_raw, str) else '{}'),
        "last_api_fetch": last_api_fetch_str,
        # last_updated will be handled by the database default or explicitly set in save_bls_data_to_db
    }
    return processed_data

def save_bls_data_to_db(data_to_save: Dict[str, Any], engine: sqlalchemy.engine.Engine) -> bool:
    """Saves or updates BLS data in the database using an upsert operation."""
    if not data_to_save or not data_to_save.get("occupation_code"):
        logger.error("No data or occupation_code provided to save_bls_data_to_db.")
        return False

    # Ensure 'last_updated' is set and correctly formatted for the database
    data_to_save['last_updated'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
    
    # Ensure 'last_api_fetch' is also correctly formatted if it came as datetime
    if isinstance(data_to_save.get('last_api_fetch'), datetime.datetime):
        data_to_save['last_api_fetch'] = data_to_save['last_api_fetch'].strftime('%Y-%m-%d')


    # Filter data to only include columns present in the table
    inspector = inspect(engine)
    table_columns = {col['name'] for col in inspector.get_columns(bls_job_data_table.name)}
    
    # Add 'id' to table_columns if it's not there (it should be, but as a safeguard)
    if 'id' not in table_columns: # Should not happen with a primary key
        table_columns.add('id')

    filtered_data_to_save = {k: v for k, v in data_to_save.items() if k in table_columns}

    # Ensure all required non-nullable fields (besides PK) have values or defaults
    # occupation_code, job_title, standardized_title, last_api_fetch, last_updated
    for required_field in ['occupation_code', 'job_title', 'standardized_title', 'last_api_fetch', 'last_updated']:
        if required_field not in filtered_data_to_save or filtered_data_to_save[required_field] is None:
            logger.error(f"Missing required field '{required_field}' for SOC {data_to_save.get('occupation_code')}. Aborting save.")
            # Provide defaults for logging if they are missing in data_to_save
            if required_field == 'job_title' and required_field not in filtered_data_to_save:
                filtered_data_to_save['job_title'] = "Unknown Job Title"
            if required_field == 'standardized_title' and required_field not in filtered_data_to_save:
                filtered_data_to_save['standardized_title'] = "Unknown Standardized Title"
            if required_field == 'last_api_fetch' and required_field not in filtered_data_to_save:
                filtered_data_to_save['last_api_fetch'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
            # last_updated is already handled
            # occupation_code is checked at the beginning
            # return False # Do not save if critical data is missing.
            # For now, we'll let it try and potentially fail, to see the DB error.
            # Better: ensure defaults or raise specific error.
            # Let's set defaults for title fields if missing.
            if filtered_data_to_save.get(required_field) is None:
                 logger.warning(f"Field {required_field} is None for SOC {data_to_save.get('occupation_code')}, this might cause an error if it's not nullable.")


    if not filtered_data_to_save.get('occupation_code'): # Should have been caught earlier
        logger.error("Occupation code is missing in filtered_data_to_save. Aborting save.")
        return False

    logger.info(f"Attempting to save/update data for SOC: {filtered_data_to_save['occupation_code']}")
    
    try:
        with engine.connect() as conn:
            # Check if record exists
            select_stmt = text("SELECT id FROM bls_job_data WHERE occupation_code = :occupation_code")
            result = conn.execute(select_stmt, {"occupation_code": filtered_data_to_save['occupation_code']})
            existing_row = result.fetchone()

            if existing_row:
                # UPDATE
                update_values = {k: v for k, v in filtered_data_to_save.items() if k != 'id' and k != 'occupation_code'}
                
                # Ensure 'last_updated' is explicitly set for updates
                update_values['last_updated'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')

                set_clauses = ", ".join([f"{key} = :{key}" for key in update_values.keys()])
                update_stmt = text(f"UPDATE bls_job_data SET {set_clauses} WHERE occupation_code = :occupation_code")
                
                params_for_update = {**update_values, "occupation_code": filtered_data_to_save['occupation_code']}
                conn.execute(update_stmt, params_for_update)
                logger.info(f"Updated data in DB for SOC {filtered_data_to_save['occupation_code']}.")
            else:
                # INSERT
                # Ensure all columns are present, defaulting to None if not in filtered_data_to_save
                # This is critical to match the INSERT statement's column list.
                data_for_insert = {col: filtered_data_to_save.get(col) for col in table_columns if col != 'id'}
                
                # Ensure 'last_updated' is set for new inserts
                data_for_insert['last_updated'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
                # Ensure 'last_api_fetch' is set if not already
                if 'last_api_fetch' not in data_for_insert or data_for_insert['last_api_fetch'] is None:
                     data_for_insert['last_api_fetch'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')


                # Ensure required string fields are not None
                for field in ['occupation_code', 'job_title', 'standardized_title', 'last_api_fetch', 'last_updated']:
                    if data_for_insert.get(field) is None:
                        logger.error(f"Cannot insert NULL for non-nullable field '{field}' for SOC {data_for_insert.get('occupation_code')}. Aborting.")
                        # This should ideally raise an error or be handled more gracefully
                        # For now, we'll log and it will likely fail at DB level if constraints are violated.
                        # Let's try to provide a default for title fields if they are None to avoid DB error
                        if field == 'job_title' and data_for_insert.get(field) is None: data_for_insert[field] = "Unknown Job Title (DB Fallback)"
                        if field == 'standardized_title' and data_for_insert.get(field) is None: data_for_insert[field] = "Unknown Standardized Title (DB Fallback)"


                # Construct the insert statement dynamically based on available keys in data_for_insert
                # that are also actual table columns
                valid_insert_data = {k: v for k,v in data_for_insert.items() if k in table_columns and k != 'id'}
                
                cols_to_insert = ", ".join(valid_insert_data.keys())
                vals_to_insert = ", ".join([f":{k}" for k in valid_insert_data.keys()])
                
                insert_stmt = text(f"INSERT INTO bls_job_data ({cols_to_insert}) VALUES ({vals_to_insert})")
                conn.execute(insert_stmt, valid_insert_data)
                logger.info(f"Inserted new data into DB for SOC {filtered_data_to_save['occupation_code']}.")
            
            conn.commit()
        return True
    except IntegrityError as e: # Catch specific integrity errors like NotNullViolation
        logger.error(f"Database integrity error saving data for SOC {data_to_save.get('occupation_code')}: {e}", exc_info=True)
        # Log the problematic data for inspection
        logger.error(f"Data that caused integrity error: {json.dumps(filtered_data_to_save, default=str)}")
        return False
    except SQLAlchemyError as e:
        logger.error(f"Database error saving data for SOC {data_to_save.get('occupation_code')}: {e}", exc_info=True)
        logger.error(f"Data that caused error: {json.dumps(filtered_data_to_save, default=str)}")
        return False
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error saving data for SOC {data_to_save.get('occupation_code')}: {e}", exc_info=True)
        logger.error(f"Data that caused unexpected error: {json.dumps(filtered_data_to_save, default=str)}")
        return False


def get_bls_data_from_db(occupation_code: str, engine: sqlalchemy.engine.Engine) -> Optional[Dict[str, Any]]:
    """Retrieves BLS data from the database for a given SOC code."""
    if not occupation_code: return None
    try:
        with engine.connect() as conn:
            stmt = bls_job_data_table.select().where(bls_job_data_table.c.occupation_code == occupation_code)
            result = conn.execute(stmt).fetchone()
            if result:
                data = dict(result._mapping) # type: ignore
                # Check freshness (e.g., data less than 90 days old)
                last_updated_str = data.get("last_updated")
                if last_updated_str:
                    try:
                        last_updated_date = datetime.datetime.strptime(last_updated_str, '%Y-%m-%d').date()
                        if (datetime.date.today() - last_updated_date).days < 90:
                            logger.info(f"Found fresh data for SOC {occupation_code} in DB, updated {last_updated_str}.")
                            return data
                        else:
                            logger.info(f"Data for SOC {occupation_code} in DB is stale (updated {last_updated_str}). Will refresh.")
                    except ValueError:
                         logger.warning(f"Could not parse last_updated_in_db date '{last_updated_str}' for SOC {occupation_code}.")
                else:
                    logger.info(f"Data for SOC {occupation_code} in DB has no last_updated_in_db. Will refresh.")
            else:
                logger.info(f"No data found in DB for SOC {occupation_code}.")
    except SQLAlchemyError as e:
        logger.error(f"Database error retrieving data for SOC {occupation_code}: {e}", exc_info=True)
    except Exception as e_gen: # Catch any other error during DB fetch
        logger.error(f"Unexpected error retrieving data for SOC {occupation_code} from DB: {e_gen}", exc_info=True)
    return None

def get_job_data_from_db_or_api(job_title_query: str) -> Dict[str, Any]:
    """
    Main function to get job data. Tries DB first, then BLS API if stale/missing.
    This is the primary entry point for fetching job data for the application.
    Strictly uses real data; no synthetic fallbacks.
    """
    engine = get_db_engine()
    soc_code, standardized_soc_title, job_category = find_soc_code_and_title(job_title_query, engine)

    if soc_code == "00-0000": # Unclassifiable or not found by BLS search
        logger.warning(f"Job title '{job_title_query}' (std: '{standardized_soc_title}') could not be mapped to a valid SOC. Returning error.")
        return {
            "error": f"Job title '{job_title_query}' could not be classified or found in BLS data.",
            "job_title": job_title_query, "standardized_title": standardized_soc_title,
            "occupation_code": soc_code, "job_category": job_category,
            "source": "soc_lookup_failed"
        }

    # Try fetching from DB first
    db_data = get_bls_data_from_db(soc_code, engine)
    if db_data:
        logger.info(f"Using data from database for SOC {soc_code} ('{standardized_soc_title}')")
        # Ensure all expected fields are present, even if None
        formatted_db_data = format_database_row_to_app_schema(db_data, job_title_query, standardized_soc_title, job_category)
        return formatted_db_data

    # If not in DB or stale, fetch from BLS API and store
    logger.info(f"Data for SOC {soc_code} not in DB or stale. Fetching from BLS API for '{standardized_soc_title}'.")
    
    try:
        processed_data_from_api = fetch_and_process_soc_data(soc_code, job_title_query, standardized_soc_title, job_category)
        
        if save_bls_data_to_db(processed_data_from_api, engine):
            logger.info(f"Successfully saved/updated data for SOC {soc_code} to database.")
        else:
            logger.error(f"Failed to save data for SOC {soc_code} to database. Proceeding with API data for this request.")
            # Even if DB save fails, we can still return the data fetched from API for the current request.
            # The `processed_data_from_api` is already in the correct structure.

        # Ensure all expected fields are present after API fetch and processing
        # The `fetch_and_process_soc_data` should return a complete dict.
        # We might need a final formatting step if its output isn't directly usable by the app.
        # For now, assume `fetch_and_process_soc_data` returns data in the app's expected final format.
        # Re-format to ensure consistency if needed (especially for risk data which is calculated separately)
        final_data = format_api_processed_data_to_app_schema(processed_data_from_api, job_title_query, standardized_soc_title, job_category)
        final_data["source"] = "bls_api_live_fetch" # Indicate it was a live fetch
        return final_data

    except Exception as e:
        logger.error(f"Critical error in get_job_data_from_db_or_api for SOC {soc_code} ('{standardized_soc_title}'): {e}", exc_info=True)
        return {
            "error": f"Failed to fetch or process data for '{standardized_soc_title}'. Source: bls_api_fetch_error_or_db_save_failed",
            "job_title": job_title_query, "standardized_title": standardized_soc_title,
            "occupation_code": soc_code, "job_category": job_category,
            "source": "bls_api_fetch_error_or_db_save_failed"
        }

def format_database_row_to_app_schema(db_row_dict: Dict[str, Any], original_job_title: str, standardized_soc_title:str, job_category_from_soc: str) -> Dict[str, Any]:
    """Formats a dictionary from a database row to the application's expected schema."""
    
    # Use the job_category derived from SOC code as it's more consistent
    # than what might be stored from previous, potentially less accurate, categorizations.
    # However, if the DB has a more specific one (not "General"), prefer that.
    final_job_category = db_row_dict.get('job_category') or job_category_from_soc
    if final_job_category == "General" and job_category_from_soc != "General":
        final_job_category = job_category_from_soc
    
    risk_data = calculate_ai_risk_from_category(
        final_job_category, # Use the determined job category
        db_row_dict.get('occupation_code', '00-0000')
    )
    
    # Generate employment trend data
    current_emp = db_row_dict.get('current_employment')
    projected_emp = db_row_dict.get('projected_employment')
    trend_years = list(range(int(db_row_dict.get('ep_base_year', datetime.datetime.now().year - 1)), int(db_row_dict.get('ep_proj_year', datetime.datetime.now().year + 10)) + 1))
    trend_employment: List[Optional[int]] = []
    if current_emp is not None and projected_emp is not None and len(trend_years) > 1:
        trend_employment = generate_employment_trend(current_emp, projected_emp, len(trend_years))
    elif current_emp is not None: # If only current employment is available
        trend_employment = [current_emp] * len(trend_years) # Flat line
        if not trend_years: trend_years = [datetime.datetime.now().year]


    return {
        "job_title": standardized_soc_title, # Use the official SOC title
        "original_search_title": original_job_title, # Keep what user searched
        "occupation_code": db_row_dict.get('occupation_code'),
        "job_category": final_job_category,
        "source": "bls_database_cache",
        
        "current_employment": db_row_dict.get('current_employment'),
        "projected_employment": db_row_dict.get('projected_employment'),
        "employment_change_numeric": db_row_dict.get('employment_change_numeric'),
        "employment_change_percent": db_row_dict.get('percent_change'),
        "annual_job_openings": db_row_dict.get('annual_job_openings'),
        "median_wage": db_row_dict.get('median_wage'),
        "mean_wage": db_row_dict.get('mean_wage'),
        "oes_data_year": db_row_dict.get('oes_data_year'),
        "ep_base_year": db_row_dict.get('ep_base_year'),
        "ep_proj_year": db_row_dict.get('ep_proj_year'),
        
        "risk_scores": { # Nest risk scores
            "year_1": risk_data["year_1_risk"],
            "year_5": risk_data["year_5_risk"]
        },
        "risk_category": risk_data["risk_category"],
        "risk_factors": risk_data["risk_factors"],
        "protective_factors": risk_data["protective_factors"],
        "analysis": risk_data["analysis"], # Analysis is now generated with risk data
        "summary": risk_data["summary"], # Summary is now generated with risk data

        "trend_data": {
            "years": trend_years if trend_employment else [],
            "employment": trend_employment
        },
        "raw_oes_data_json": db_row_dict.get('raw_oes_data_json'),
        "raw_ep_data_json": db_row_dict.get('raw_ep_data_json'),
        "last_api_fetch": db_row_dict.get('last_api_fetch'),
        "last_updated_in_db": db_row_dict.get('last_updated') # Use the correct column name from DB
    }

def format_api_processed_data_to_app_schema(processed_data: Dict[str, Any], original_job_title: str, standardized_soc_title:str, job_category_from_soc: str) -> Dict[str, Any]:
    """Formats data processed from API calls into the application's expected schema."""
    
    final_job_category = processed_data.get('job_category') or job_category_from_soc
    if final_job_category == "General" and job_category_from_soc != "General":
        final_job_category = job_category_from_soc

    risk_data = calculate_ai_risk_from_category(
        final_job_category,
        processed_data.get('occupation_code', '00-0000')
    )

    current_emp = processed_data.get('current_employment')
    projected_emp = processed_data.get('projected_employment')
    
    base_year_str = processed_data.get('ep_base_year', str(datetime.datetime.now().year -1))
    proj_year_str = processed_data.get('ep_proj_year', str(datetime.datetime.now().year + 10))
    
    try:
        base_year = int(float(base_year_str)) if base_year_str else datetime.datetime.now().year -1
        proj_year = int(float(proj_year_str)) if proj_year_str else datetime.datetime.now().year + 10
        trend_years = list(range(base_year, proj_year + 1))
    except ValueError:
        logger.warning(f"Could not parse EP years: base='{base_year_str}', proj='{proj_year_str}'. Defaulting years.")
        trend_years = list(range(datetime.datetime.now().year -1, datetime.datetime.now().year + 10))


    trend_employment: List[Optional[int]] = []
    if current_emp is not None and projected_emp is not None and len(trend_years) > 1:
        trend_employment = generate_employment_trend(current_emp, projected_emp, len(trend_years))
    elif current_emp is not None:
        trend_employment = [current_emp] * len(trend_years)
        if not trend_years: trend_years = [datetime.datetime.now().year]


    return {
        "job_title": standardized_soc_title,
        "original_search_title": original_job_title,
        "occupation_code": processed_data.get('occupation_code'),
        "job_category": final_job_category,
        "source": "bls_api_live_fetch", # Explicitly set source
        
        "current_employment": current_emp,
        "projected_employment": projected_emp,
        "employment_change_numeric": processed_data.get('employment_change_numeric'),
        "employment_change_percent": processed_data.get('percent_change'),
        "annual_job_openings": processed_data.get('annual_job_openings'),
        "median_wage": processed_data.get('median_wage'),
        "mean_wage": processed_data.get('mean_wage'),
        "oes_data_year": processed_data.get('oes_data_year'),
        "ep_base_year": str(base_year) if base_year else None, # ensure string
        "ep_proj_year": str(proj_year) if proj_year else None, # ensure string
        
        "risk_scores": {
            "year_1": risk_data["year_1_risk"],
            "year_5": risk_data["year_5_risk"]
        },
        "risk_category": risk_data["risk_category"],
        "risk_factors": risk_data["risk_factors"],
        "protective_factors": risk_data["protective_factors"],
        "analysis": risk_data["analysis"],
        "summary": risk_data["summary"],

        "trend_data": {
            "years": trend_years if trend_employment else [],
            "employment": trend_employment
        },
        "raw_oes_data_json": processed_data.get('raw_oes_data_json'),
        "raw_ep_data_json": processed_data.get('raw_ep_data_json'),
        "last_api_fetch": processed_data.get('last_api_fetch'),
        "last_updated_in_db": processed_data.get('last_updated') # This is the new 'last_updated' field
    }


# --- Risk Calculation (Simplified placeholder - relies on job_category from BLS) ---
def calculate_ai_risk_from_category(job_category: str, occupation_code: str) -> Dict[str, Any]:
    """Calculates AI displacement risk based on job category."""
    logger.debug(f"Calculating AI risk for category: '{job_category}', SOC: '{occupation_code}'")
    # Default risk scores
    year_1_risk, year_5_risk = 30.0, 50.0
    risk_cat_text = "Moderate"
    
    # Simplified risk assignment based on keywords in category
    cat_lower = job_category.lower()
    if any(k in cat_lower for k in ["computer", "mathematical", "engineering", "architecture"]):
        year_1_risk, year_5_risk, risk_cat_text = 20.0, 45.0, "Moderate"
    elif any(k in cat_lower for k in ["administrative", "office support", "production", "transportation"]):
        year_1_risk, year_5_risk, risk_cat_text = 40.0, 70.0, "High"
    elif any(k in cat_lower for k in ["healthcare", "education", "legal", "community", "social service"]):
        year_1_risk, year_5_risk, risk_cat_text = 10.0, 25.0, "Low"
    elif any(k in cat_lower for k in ["management", "business", "financial"]):
        year_1_risk, year_5_risk, risk_cat_text = 25.0, 40.0, "Moderate"
    elif any(k in cat_lower for k in ["sales", "food preparation", "personal care"]):
        year_1_risk, year_5_risk, risk_cat_text = 35.0, 60.0, "High"
    
    # Generate generic factors for now
    risk_factors = generate_risk_factors(job_category, occupation_code) # Pass SOC for potential future use
    protective_factors = generate_protective_factors(job_category, occupation_code)
    
    analysis_text = f"The role, falling under the '{job_category}' category (SOC: {occupation_code}), faces a {risk_cat_text.lower()} risk of AI displacement. "
    analysis_text += "Key tasks may be impacted by automation, while skills requiring complex human judgment and interaction are likely to remain valuable."
    
    summary_text = f"Overall risk for '{job_category}' (SOC: {occupation_code}) is {risk_cat_text}. Consider focusing on {protective_factors[0].lower() if protective_factors else 'strategic upskilling'}."


    return {
        "year_1_risk": year_1_risk,
        "year_5_risk": year_5_risk,
        "risk_category": risk_cat_text,
        "risk_factors": risk_factors,
        "protective_factors": protective_factors,
        "analysis": analysis_text,
        "summary": summary_text
    }

def generate_risk_factors(job_category: str, occupation_code: str) -> List[str]:
    """Generates generic risk factors based on job category."""
    base_factors = [
        "Automation of routine data entry and processing tasks.",
        "AI tools assisting or replacing standard reporting and analysis.",
        "Predictable physical tasks being handled by robotics.",
        "Customer interaction and support via AI chatbots and virtual assistants."
    ]
    cat_lower = job_category.lower()
    if any(k in cat_lower for k in ["computer", "mathematical", "engineering"]):
        return base_factors[:2] + ["AI-driven code generation and software testing.", "Automated system monitoring and maintenance."]
    if any(k in cat_lower for k in ["administrative", "office support", "production"]):
        return ["High degree of repetitive tasks.", "Structured data processing.", "Well-defined procedures amenable to automation."] + base_factors[:1]
    return base_factors[:4] # Return a default of 4 factors

def generate_protective_factors(job_category: str, occupation_code: str) -> List[str]:
    """Generates generic protective factors based on job category."""
    base_factors = [
        "Requires complex problem-solving and critical thinking.",
        "Involves significant interpersonal interaction and empathy.",
        "Needs high levels of creativity and original thought.",
        "Requires adaptability in unpredictable environments."
    ]
    cat_lower = job_category.lower()
    if any(k in cat_lower for k in ["healthcare", "education", "social service", "legal"]):
        return base_factors[:2] + ["Ethical judgment and nuanced decision-making.", "Building trust and rapport with individuals."]
    if any(k in cat_lower for k in ["management", "arts", "design"]):
        return ["Strategic planning and foresight.", "Leadership and team motivation."] + base_factors[2:]
    return base_factors[:4]

def generate_employment_trend(current_emp: Optional[int], projected_emp: Optional[int], num_years: int) -> List[Optional[int]]:
    """Generates a simple linear trend for employment values."""
    if current_emp is None or projected_emp is None or num_years <= 1:
        return [current_emp] * num_years if current_emp is not None and num_years > 0 else []

    trend: List[Optional[int]] = []
    # Ensure we don't divide by zero if num_years is 1, though already checked.
    # If num_years is 0 or less, this will also be problematic, but the check num_years <=1 handles it.
    annual_change = (projected_emp - current_emp) / (num_years -1) if num_years > 1 else 0
    
    for i in range(num_years):
        trend.append(int(current_emp + (annual_change * i)))
    return trend

def get_all_soc_codes_from_db(engine: sqlalchemy.engine.Engine) -> List[Tuple[str, str]]:
    """Retrieves all unique SOC codes and their standardized titles from the database."""
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT occupation_code, standardized_title FROM bls_job_data ORDER BY occupation_code")
            result = conn.execute(query)
            return [(row[0], row[1]) for row in result.fetchall()]
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching all SOC codes: {e}", exc_info=True)
        return []

def get_job_titles_for_autocomplete() -> List[Dict[str, str]]:
    """
    Fetches job titles from the database for autocomplete functionality.
    Prioritizes standardized titles and includes original job titles as aliases.
    """
    engine = get_db_engine()
    job_titles_set = set() # To avoid duplicates if a job_title is same as standardized_title
    autocomplete_list = []

    try:
        with engine.connect() as conn:
            # Fetch standardized titles first (these are preferred)
            query_std = text("SELECT DISTINCT standardized_title, occupation_code FROM bls_job_data WHERE standardized_title IS NOT NULL ORDER BY standardized_title")
            result_std = conn.execute(query_std)
            for row in result_std:
                title, soc = row._mapping['standardized_title'], row._mapping['occupation_code']
                if title and title not in job_titles_set:
                    autocomplete_list.append({"title": title, "soc_code": soc, "is_primary": True})
                    job_titles_set.add(title)
            
            # Fetch original job_titles as aliases if they are different
            query_orig = text("SELECT DISTINCT job_title, occupation_code, standardized_title FROM bls_job_data WHERE job_title IS NOT NULL ORDER BY job_title")
            result_orig = conn.execute(query_orig)
            for row in result_orig:
                title, soc, std_title = row._mapping['job_title'], row._mapping['occupation_code'], row._mapping['standardized_title']
                if title and title != std_title and title not in job_titles_set:
                    autocomplete_list.append({"title": title, "soc_code": soc, "is_primary": False})
                    job_titles_set.add(title)
            
            # Add static mappings as non-primary if not already present
            for title_alias, soc_code_alias in JOB_TITLE_TO_SOC_STATIC.items():
                if title_alias not in job_titles_set:
                    autocomplete_list.append({"title": title_alias.title(), "soc_code": soc_code_alias, "is_primary": False})
                    job_titles_set.add(title_alias)

            # Sort the final list by title for consistent display
            autocomplete_list.sort(key=lambda x: x['title'])

        logger.info(f"Loaded {len(autocomplete_list)} job titles for autocomplete.")
        return autocomplete_list
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching job titles for autocomplete: {e}", exc_info=True)
        # Fallback to static list if DB fails
        return [{"title": k.title(), "soc_code": v, "is_primary": True} for k,v in JOB_TITLE_TO_SOC_STATIC.items()]
    except Exception as e_gen:
        logger.error(f"Unexpected error fetching job titles for autocomplete: {e_gen}", exc_info=True)
        return [{"title": k.title(), "soc_code": v, "is_primary": True} for k,v in JOB_TITLE_TO_SOC_STATIC.items()]

if __name__ == "__main__":
    # Example usage (for testing)
    # Ensure DATABASE_URL is set in your environment if you run this directly
    if not os.environ.get("DATABASE_URL"):
        print("DATABASE_URL not set. Please set it to test bls_job_mapper.py directly.")
    else:
        logging.basicConfig(level=logging.INFO)
        logger.setLevel(logging.INFO) # Ensure logger for this module is also INFO for testing
        
        test_job_titles = [
            "Software Developer", "Registered Nurse", "Truck Driver", 
            "Financial Analyst", "Marketing Manager", "Teacher", "Lawyer",
            "Data Entry Keyer", # Example of a potentially high-risk job
            "Chief Executive",    # Example of a management role
            "Robotics Engineer" # A job title that might not be in static map
        ]
        
        for title in test_job_titles:
            print(f"\n--- Testing: {title} ---")
            data = get_job_data_from_db_or_api(title)
            if "error" in data:
                print(f"Error: {data['error']}")
                if data.get("message"): print(f"Message: {data['message']}")
            else:
                print(f"Job Title (Standardized): {data.get('job_title')}")
                print(f"Original Search: {data.get('original_search_title')}")
                print(f"SOC Code: {data.get('occupation_code')}")
                print(f"Job Category: {data.get('job_category')}")
                print(f"Source: {data.get('source')}")
                print(f"Current Employment: {data.get('current_employment')}")
                print(f"Projected Employment: {data.get('projected_employment')}")
                print(f"Employment Change %: {data.get('employment_change_percent')}")
                print(f"Annual Job Openings: {data.get('annual_job_openings')}")
                print(f"Median Wage: {data.get('median_wage')}")
                print(f"Risk Category: {data.get('risk_category')}")
                print(f"5-Year Risk: {data.get('risk_scores', {}).get('year_5')}%")
                print(f"Last API Fetch: {data.get('last_api_fetch')}")
                print(f"Last Updated in DB: {data.get('last_updated_in_db')}")
                # print(f"Risk Factors: {data.get('risk_factors')}")
                # print(f"Protective Factors: {data.get('protective_factors')}")
                # print(f"Analysis: {data.get('analysis')}")
                # print(f"Summary: {data.get('summary')}")

        print("\n--- Testing Autocomplete ---")
        autocomplete_titles = get_job_titles_for_autocomplete()
        print(f"Loaded {len(autocomplete_titles)} titles for autocomplete.")
        if autocomplete_titles:
            print("First 5 autocomplete titles:")
            for item in autocomplete_titles[:5]:
                print(item)
        
        print("\n--- Testing get_all_soc_codes_from_db ---")
        all_socs = get_all_soc_codes_from_db(get_db_engine())
        print(f"Found {len(all_socs)} unique SOC codes in DB.")
        if all_socs:
            print("First 5 SOCs from DB:")
            for soc, title_std in all_socs[:5]:
                print(f"{soc}: {title_std}")

