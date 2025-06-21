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

import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, MetaData, inspect, Text, TIMESTAMP
from sqlalchemy.exc import SQLAlchemyError

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
if not logger.handlers: # Ensure logger is configured more robustly
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO) # Set default level if not already set
    logger.propagate = False # Prevent duplicate logs if root logger is also configured

# --- Database Setup ---
metadata = MetaData()
bls_job_data_table = Table(
    'bls_job_data', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('occupation_code', String(10), nullable=False, unique=True, index=True),
    Column('job_title', String(255), nullable=False), # Original title searched that resolved to this SOC
    Column('standardized_title', String(255), nullable=False, index=True), # Official/common title for the SOC
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
    Column('last_api_fetch', TIMESTAMP(timezone=True), nullable=False),
    Column('last_updated_in_db', TIMESTAMP(timezone=True), nullable=False, default=lambda: datetime.datetime.now(datetime.timezone.utc))
)

_engine_instance = None
_engine_lock = threading.Lock()

def get_db_engine(force_new: bool = False) -> sqlalchemy.engine.Engine:
    """Creates and returns a SQLAlchemy engine, ensuring singleton-like behavior."""
    global _engine_instance
    with _engine_lock:
        if _engine_instance is None or force_new:
            database_url = os.environ.get('DATABASE_URL')
            if not database_url:
                try:
                    import streamlit as st
                    database_url = st.secrets.get("database", {}).get("DATABASE_URL")
                except (ImportError, AttributeError):
                    pass # Handled by the None check below

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
                logger.info(f"Creating new database engine instance for URL: {database_url.split('@')[-1] if '@' in database_url else database_url}") # Log db host, not full creds
                _engine_instance = create_engine(database_url, connect_args=connect_args, pool_pre_ping=True, pool_recycle=1800, echo=False) # Set echo=False for production
                
                # Test connection immediately
                with _engine_instance.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("Database engine created and connection tested successfully.")
                
                # Create table if it doesn't exist
                try:
                    metadata.create_all(_engine_instance, checkfirst=True)
                    logger.info(f"Table '{bls_job_data_table.name}' ensured to exist.")
                except Exception as e_table:
                    logger.error(f"Failed to create table '{bls_job_data_table.name}': {e_table}", exc_info=True)
                    # Don't raise here, allow app to try to function if table exists but check failed

            except Exception as e:
                logger.error(f"Failed to create database engine or test connection: {e}", exc_info=True)
                _engine_instance = None # Ensure it's None if creation failed
                raise
        return _engine_instance

# --- Constants and Mappings ---
JOB_TITLE_TO_SOC_STATIC: Dict[str, str] = {
    "software developer": "15-1252", "software engineer": "15-1252", "programmer": "15-1251",
    "web developer": "15-1254", "registered nurse": "29-1141", "nurse": "29-1141",
    "teacher": "25-2021", "elementary school teacher": "25-2021", "high school teacher": "25-2031",
    "lawyer": "23-1011", "attorney": "23-1011", "doctor": "29-1221", "physician": "29-1221",
    "accountant": "13-2011", "project manager": "13-1199", # Changed from 11-3021 to a more general one
    "product manager": "11-2021", "marketing manager": "11-2021", "retail salesperson": "41-2031",
    "cashier": "41-2011", "customer service representative": "43-4051", "truck driver": "53-3032",
    "receptionist": "43-4171", "data scientist": "15-2051", "data analyst": "15-2051", # Mapped Data Analyst to Data Scientist SOC
    "business analyst": "13-1111", "financial analyst": "13-2051", "human resources specialist": "13-1071",
    "graphic designer": "27-1024", "police officer": "33-3051",
    "chef": "35-1011", "cook": "35-2014", "waiter": "35-3031", "waitress": "35-3031",
    "janitor": "37-2011", "administrative assistant": "43-6011", "executive assistant": "43-6011",
    "dental hygienist": "29-1292", "electrician": "47-2111", "plumber": "47-2152",
    "carpenter": "47-2031", "construction worker": "47-2061", "mechanic": "49-3023",
    "automotive mechanic": "49-3023", "taxi driver": "53-3054", "uber driver": "53-3054",
    "journalist": "27-3023", "reporter": "27-3023", "writer": "27-3042",
    "editor": "27-3041", "photographer": "27-4021", "court reporter": "23-2011",
    "stenographer": "23-2011", "digital court reporter": "23-2011", "travel agent": "41-3041" # Corrected SOC
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
    "51-": "Production Occupations", "53-": "Transportation and Material Moving Occupations"
}

TARGET_SOC_CODES: List[Dict[str, str]] = [
    # Computer and Mathematical
    {"soc_code": "15-1252", "title": "Software Developers"}, {"soc_code": "15-1251", "title": "Computer Programmers"},
    {"soc_code": "15-1254", "title": "Web Developers"}, {"soc_code": "15-2051", "title": "Data Scientists"},
    {"soc_code": "15-1211", "title": "Computer Systems Analysts"}, {"soc_code": "15-1244", "title": "Network and Computer Systems Administrators"},
    {"soc_code": "15-1232", "title": "Computer User Support Specialists"}, {"soc_code": "15-1299", "title": "Computer Occupations, All Other"}, # Broad category
    # Business and Financial
    {"soc_code": "13-2011", "title": "Accountants and Auditors"}, {"soc_code": "13-2051", "title": "Financial Analysts"},
    {"soc_code": "13-1111", "title": "Management Analysts"}, {"soc_code": "13-1161", "title": "Market Research Analysts and Marketing Specialists"},
    # Management
    {"soc_code": "11-1021", "title": "General and Operations Managers"}, {"soc_code": "11-2021", "title": "Marketing Managers"},
    {"soc_code": "11-3021", "title": "Computer and Information Systems Managers"}, {"soc_code": "11-9199", "title": "Managers, All Other"},
    # Healthcare Practitioners and Technical
    {"soc_code": "29-1141", "title": "Registered Nurses"}, {"soc_code": "29-1229", "title": "Physicians, All Other"}, # Broad category for doctors
    {"soc_code": "29-1021", "title": "Dentists, General"}, {"soc_code": "29-2061", "title": "Licensed Practical and Licensed Vocational Nurses"},
    # Education, Training, and Library
    {"soc_code": "25-2021", "title": "Elementary School Teachers, Except Special Education"}, {"soc_code": "25-2031", "title": "Secondary School Teachers, Except Special and Career/Technical Education"},
    {"soc_code": "25-3099", "title": "Teachers and Instructors, All Other"}, # Broad category
    # Office and Administrative Support
    {"soc_code": "43-4051", "title": "Customer Service Representatives"}, {"soc_code": "43-6011", "title": "Executive Secretaries and Executive Administrative Assistants"},
    {"soc_code": "43-9061", "title": "Office Clerks, General"}, {"soc_code": "43-9021", "title": "Data Entry Keyers"},
    # Sales and Related
    {"soc_code": "41-2031", "title": "Retail Salespersons"}, {"soc_code": "41-1011", "title": "First-Line Supervisors of Retail Sales Workers"},
    # Transportation and Material Moving
    {"soc_code": "53-3032", "title": "Heavy and Tractor-Trailer Truck Drivers"}, {"soc_code": "53-3033", "title": "Light Truck Drivers"},
    # Construction and Extraction
    {"soc_code": "47-2061", "title": "Construction Laborers"}, {"soc_code": "47-2111", "title": "Electricians"},
    {"soc_code": "47-2031", "title": "Carpenters"},
    # Food Preparation and Serving Related
    {"soc_code": "35-2014", "title": "Cooks, Restaurant"}, {"soc_code": "35-3031", "title": "Waiters and Waitresses"},
    # Legal
    {"soc_code": "23-1011", "title": "Lawyers"}, {"soc_code": "23-2011", "title": "Paralegals and Legal Assistants"},
    # Arts, Design, Entertainment, Sports, and Media
    {"soc_code": "27-1024", "title": "Graphic Designers"}, {"soc_code": "27-3042", "title": "Technical Writers"},
    {"soc_code": "27-3023", "title": "News Analysts, Reporters, and Journalists"}
]

# --- Helper Functions ---
def get_job_category(occupation_code: str) -> str:
    """Gets job category based on SOC code prefix."""
    for prefix, category in SOC_TO_CATEGORY.items():
        if occupation_code.startswith(prefix):
            return category
    return "General Occupations"

def standardize_job_title_for_soc_lookup(title: str) -> str:
    """Standardizes job title for SOC code lookup."""
    std_title = title.lower().strip()
    suffixes = [" i", " ii", " iii", " iv", " v", " specialist", " assistant", " associate", " senior", " junior", " lead", " intern"]
    for suffix in suffixes:
        if std_title.endswith(suffix):
            std_title = std_title[:-len(suffix)].strip()
            break
    return std_title

def find_occupation_code(job_title: str, engine: sqlalchemy.engine.Engine) -> Tuple[str, str, str]:
    """Finds SOC code, standardized title, and category for a job title."""
    std_title_query = standardize_job_title_for_soc_lookup(job_title)

    # 1. Check static mapping
    if std_title_query in JOB_TITLE_TO_SOC_STATIC:
        soc_code = JOB_TITLE_TO_SOC_STATIC[std_title_query]
        # Try to get a more official title from TARGET_SOC_CODES if available
        official_title = next((item["title"] for item in TARGET_SOC_CODES if item["soc_code"] == soc_code), job_title)
        category = get_job_category(soc_code)
        logger.info(f"Found SOC {soc_code} for '{job_title}' (standardized: '{std_title_query}') via static map. Official: '{official_title}', Category: {category}")
        return soc_code, official_title, category

    # 2. Check database for existing standardized titles or job titles
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT occupation_code, standardized_title, job_category FROM bls_job_data
                WHERE LOWER(standardized_title) = LOWER(:title) OR LOWER(job_title) = LOWER(:title)
                LIMIT 1
            """)
            result = conn.execute(query, {"title": std_title_query}).fetchone()
            if result:
                logger.info(f"Found SOC {result.occupation_code} for '{job_title}' via database lookup. Standardized: '{result.standardized_title}', Category: {result.job_category}")
                return result.occupation_code, result.standardized_title, result.job_category
    except Exception as e:
        logger.warning(f"Database lookup for '{std_title_query}' failed: {e}")

    # 3. Use BLS API search (via bls_connector) as a last resort if API key is available
    if bls_connector.is_api_key_available():
        try:
            logger.info(f"Job title '{job_title}' not in static map or DB, attempting BLS API search.")
            matches = bls_connector.search_occupations(job_title)
            if matches:
                best_match = matches[0]
                soc_code = best_match["soc_code"]
                official_title_api = best_match["title"]
                category = get_job_category(soc_code)
                logger.info(f"Found SOC {soc_code} for '{job_title}' via BLS API. Official: '{official_title_api}', Category: {category}")
                # Optionally, add this new mapping to a dynamic cache or suggest for static map update
                return soc_code, official_title_api, category
        except Exception as e:
            logger.error(f"BLS API search for '{job_title}' failed: {e}")
    else:
        logger.warning("BLS API key not available, skipping API search for occupation code.")

    logger.warning(f"Could not find specific SOC code for '{job_title}'. Using default '00-0000'.")
    return "00-0000", job_title, "General Occupations"


# --- Database Interaction Functions ---
def get_bls_data_from_db(occupation_code: str, engine: sqlalchemy.engine.Engine) -> Optional[Dict[str, Any]]:
    """Gets BLS data from database if available and fresh."""
    if not engine: return None
    try:
        with engine.connect() as conn:
            query = bls_job_data_table.select().where(bls_job_data_table.c.occupation_code == occupation_code)
            result = conn.execute(query).fetchone()
            if result:
                data = dict(result._mapping) # type: ignore
                last_fetch = data.get("last_api_fetch")
                if isinstance(last_fetch, str): # Handle string dates from older DB entries if any
                    last_fetch = datetime.datetime.fromisoformat(last_fetch)
                
                # Ensure last_fetch is timezone-aware (assume UTC if naive)
                if last_fetch and last_fetch.tzinfo is None:
                    last_fetch = last_fetch.replace(tzinfo=datetime.timezone.utc)

                days_since_update = (datetime.datetime.now(datetime.timezone.utc) - last_fetch).days if last_fetch else float('inf')
                
                if days_since_update < 90: # Data considered fresh for 90 days
                    logger.info(f"Using cached DB data for SOC {occupation_code} (updated {days_since_update} days ago).")
                    return data
                else:
                    logger.info(f"DB data for SOC {occupation_code} is stale (updated {days_since_update} days ago). Will re-fetch.")
        return None
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching data for SOC {occupation_code}: {e}", exc_info=True)
        return None


def save_bls_data_to_db(data_to_save: Dict[str, Any], engine: sqlalchemy.engine.Engine) -> bool:
    """Saves or updates BLS data in the database."""
    if not engine: return False
    
    # Ensure all required columns are present, provide defaults for nullable ones if missing
    required_fields = {col.name: None for col in bls_job_data_table.columns if not col.primary_key and not col.default}
    
    # Set default for last_updated_in_db if not present (though it has a Python-level default)
    if 'last_updated_in_db' not in data_to_save:
        data_to_save['last_updated_in_db'] = datetime.datetime.now(datetime.timezone.utc)
    if 'last_api_fetch' not in data_to_save: # Should always be set by fetch_and_process
        data_to_save['last_api_fetch'] = datetime.datetime.now(datetime.timezone.utc)

    # Ensure datetime fields are actual datetime objects
    for dt_field in ['last_api_fetch', 'last_updated_in_db']:
        if isinstance(data_to_save.get(dt_field), str):
            try:
                data_to_save[dt_field] = datetime.datetime.fromisoformat(data_to_save[dt_field])
            except ValueError:
                 data_to_save[dt_field] = datetime.datetime.now(datetime.timezone.utc) # Fallback
        if data_to_save.get(dt_field) and data_to_save[dt_field].tzinfo is None: # type: ignore
             data_to_save[dt_field] = data_to_save[dt_field].replace(tzinfo=datetime.timezone.utc) # type: ignore


    # Filter data to only include columns present in the table
    table_columns = {col.name for col in bls_job_data_table.columns}
    filtered_data_to_save = {k: v for k, v in data_to_save.items() if k in table_columns}

    # Ensure all non-nullable fields (without defaults) are present
    for field in required_fields:
        if field not in filtered_data_to_save and field not in ['id', 'last_updated_in_db']: # id is auto, last_updated_in_db has default
             logger.warning(f"Missing required field '{field}' when saving SOC {data_to_save.get('occupation_code')}. Setting to NULL if nullable, or skipping if not.")
             # If it's truly required and not nullable, this insert/update might fail or use DB default if any.
             # For now, we allow SQLAlchemy to handle it based on table schema.

    try:
        with engine.connect() as conn:
            # Check if record exists
            select_stmt = text("SELECT id FROM bls_job_data WHERE occupation_code = :occupation_code")
            existing_record = conn.execute(select_stmt, {"occupation_code": filtered_data_to_save["occupation_code"]}).fetchone()

            if existing_record:
                # Update existing record
                update_stmt = bls_job_data_table.update().\
                    where(bls_job_data_table.c.occupation_code == filtered_data_to_save["occupation_code"]).\
                    values(**filtered_data_to_save)
                conn.execute(update_stmt)
                logger.info(f"Updated data in DB for SOC {filtered_data_to_save['occupation_code']}.")
            else:
                # Insert new record
                insert_stmt = bls_job_data_table.insert().values(**filtered_data_to_save)
                conn.execute(insert_stmt)
                logger.info(f"Inserted new data into DB for SOC {filtered_data_to_save['occupation_code']}.")
            conn.commit()
        return True
    except SQLAlchemyError as e:
        logger.error(f"Database error saving data for SOC {filtered_data_to_save.get('occupation_code', 'N/A')}: {e}", exc_info=True)
        return False
    except KeyError as e:
        logger.error(f"Missing key in data_to_save for SOC {filtered_data_to_save.get('occupation_code', 'N/A')}: {e}. Data: {filtered_data_to_save}", exc_info=True)
        return False


# --- Core Data Fetching and Processing ---
def fetch_and_process_soc_data(soc_code_info: Dict[str, str], engine: sqlalchemy.engine.Engine, original_job_title_search: str) -> Dict[str, Any]:
    """
    Fetches data from BLS API for a given SOC code, processes it, and stores it.
    This function uses the bls_connector module for API calls.
    It ensures that only real BLS data is used.
    """
    soc_code = soc_code_info["soc_code"]
    representative_title = soc_code_info["title"] # Use the official title for clarity
    logger.info(f"Fetching and processing data for SOC: {soc_code} ('{representative_title}') based on original search: '{original_job_title_search}'")

    current_time = datetime.datetime.now(datetime.timezone.utc)
    current_year = current_time.year
    oes_start_year = str(current_year - 3) # Fetch last 3 years of OES data
    oes_end_year = str(current_year -1) # OES data is typically available up to the previous year

    # Initialize data structure
    processed_data: Dict[str, Any] = {
        "occupation_code": soc_code,
        "job_title": original_job_title_search, # Store the title user actually searched for
        "standardized_title": representative_title, # Store the official/common title for this SOC
        "job_category": get_job_category(soc_code),
        "last_api_fetch": current_time,
        "raw_oes_data_json": None, "raw_ep_data_json": None,
        "current_employment": None, "projected_employment": None, "employment_change_numeric": None,
        "percent_change": None, "annual_job_openings": None, "median_wage": None, "mean_wage": None,
        "oes_data_year": None, "ep_base_year": None, "ep_proj_year": None,
        "source": "bls_api_fetch_error_or_db_save_failed" # Default source indicating potential failure
    }

    # Fetch OES data (Employment and Wages)
    try:
        logger.info(f"Fetching OES data for SOC {soc_code} for years {oes_start_year}-{oes_end_year}")
        oes_data_raw = bls_connector.get_oes_data_for_soc(soc_code, oes_start_year, oes_end_year)
        
        if oes_data_raw and oes_data_raw.get("status") == "success" and oes_data_raw.get("data"):
            processed_data["raw_oes_data_json"] = json.dumps(oes_data_raw["data"])
            processed_data["current_employment"] = oes_data_raw["data"].get("employment")
            processed_data["median_wage"] = oes_data_raw["data"].get("median_wage")
            processed_data["mean_wage"] = oes_data_raw["data"].get("mean_wage")
            processed_data["oes_data_year"] = oes_data_raw["data"].get("data_year")
            logger.info(f"Successfully processed OES data for SOC {soc_code}.")
            processed_data["source"] = "bls_api_success" # Mark as success if we got this far
        else:
            logger.warning(f"Failed to fetch or parse OES data for SOC {soc_code}. API Response: {oes_data_raw}")
            processed_data["raw_oes_data_json"] = json.dumps(oes_data_raw) # Store raw error response
    except Exception as e:
        logger.error(f"Exception fetching/processing OES data for SOC {soc_code}: {e}", exc_info=True)
        processed_data["raw_oes_data_json"] = json.dumps({"error": str(e)})

    # Fetch EP data (Employment Projections)
    try:
        logger.info(f"Fetching EP data for SOC {soc_code}")
        ep_data_raw = bls_connector.get_ep_data_for_soc(soc_code)
        
        if ep_data_raw and ep_data_raw.get("status") == "success" and ep_data_raw.get("projections"):
            processed_data["raw_ep_data_json"] = json.dumps(ep_data_raw["projections"])
            # If current_employment is still None from OES, try to get it from EP (less ideal but better than nothing)
            if processed_data["current_employment"] is None:
                 processed_data["current_employment"] = ep_data_raw["projections"].get("current_employment")
            processed_data["projected_employment"] = ep_data_raw["projections"].get("projected_employment")
            processed_data["employment_change_numeric"] = ep_data_raw["projections"].get("employment_change_numeric")
            processed_data["percent_change"] = ep_data_raw["projections"].get("employment_change_percent")
            processed_data["annual_job_openings"] = ep_data_raw["projections"].get("annual_job_openings")
            processed_data["ep_base_year"] = ep_data_raw["projections"].get("base_year")
            processed_data["ep_proj_year"] = ep_data_raw["projections"].get("projection_year")
            logger.info(f"Successfully processed EP data for SOC {soc_code}.")
            # If OES also succeeded, source remains bls_api_success. If OES failed but EP succeeded, this is still a partial success.
            if processed_data["source"] != "bls_api_success": # i.e. OES failed
                 processed_data["source"] = "bls_api_partial_success_ep_only"
        else:
            logger.warning(f"Failed to fetch or parse EP data for SOC {soc_code}. API Response: {ep_data_raw}")
            processed_data["raw_ep_data_json"] = json.dumps(ep_data_raw) # Store raw error response
            # If OES succeeded but EP failed, mark as partial success
            if processed_data["source"] == "bls_api_success":
                processed_data["source"] = "bls_api_partial_success_oes_only"

    except Exception as e:
        logger.error(f"Exception fetching/processing EP data for SOC {soc_code}: {e}", exc_info=True)
        processed_data["raw_ep_data_json"] = json.dumps({"error": str(e)})
        if processed_data["source"] == "bls_api_success": # OES succeeded, EP failed
            processed_data["source"] = "bls_api_partial_success_oes_only"


    # Save to database
    if not save_bls_data_to_db(processed_data, engine):
        logger.error(f"Failed to save processed data for SOC {soc_code} to database.")
        # Keep source as "bls_api_fetch_error_or_db_save_failed" if DB save fails
        # but if API fetch was partially/fully successful, reflect that.
        if "success" in processed_data["source"]:
             processed_data["source"] = "bls_api_success_db_save_failed" # More specific error
    else:
        logger.info(f"Successfully saved data for SOC {soc_code} to database.")
        # If DB save is successful, the source reflects the API fetch status
    
    return processed_data

# --- Main Function for App Integration ---
def get_bls_data_for_job_title(job_title: str, engine: sqlalchemy.engine.Engine) -> Dict[str, Any]:
    """
    Main function to get all BLS data for a job title.
    It checks the database first, then fetches from API if necessary.
    This function ensures that only real BLS data is used.
    """
    logger.info(f"Getting BLS data for job title: '{job_title}'")
    soc_code, standardized_title, job_category = find_occupation_code(job_title, engine)

    if soc_code == "00-0000": # Unmapped job title
        logger.warning(f"Job title '{job_title}' could not be mapped to a specific SOC code.")
        return {
            "error": f"Job title '{job_title}' not found or could not be mapped to a BLS occupation.",
            "job_title": job_title,
            "occupation_code": soc_code,
            "standardized_title": standardized_title,
            "job_category": job_category,
            "source": "mapping_failed"
        }

    # Try to get data from the database
    cached_data = get_bls_data_from_db(soc_code, engine)
    if cached_data:
        # Ensure all necessary fields for risk calculation are present
        cached_data["job_category"] = cached_data.get("job_category") or get_job_category(soc_code) # Recalculate if missing
        cached_data["standardized_title"] = cached_data.get("standardized_title") or standardized_title
        return cached_data

    # If not in cache or stale, fetch from BLS API and store
    logger.info(f"No fresh data in DB for SOC {soc_code}. Fetching from BLS API.")
    soc_code_info = {"soc_code": soc_code, "title": standardized_title}
    
    # Pass the original job title for context, but use standardized_title for BLS interaction
    return fetch_and_process_soc_data(soc_code_info, engine, job_title)

if __name__ == "__main__":
    # Example usage (for testing)
    # Ensure DATABASE_URL and BLS_API_KEY are set as environment variables for this to run
    logging.basicConfig(level=logging.DEBUG) # More verbose for direct script run
    logger.setLevel(logging.DEBUG)

    test_engine = None
    try:
        test_engine = get_db_engine()
        logger.info("Test: Database engine acquired.")
        
        # Test with a few SOC codes from TARGET_SOC_CODES
        test_socs_info = TARGET_SOC_CODES[:2] # Test first 2 SOCs
        
        for soc_info_item in test_socs_info:
            logger.info(f"--- Testing SOC: {soc_info_item['soc_code']} ({soc_info_item['title']}) ---")
            data = fetch_and_process_soc_data(soc_info_item, test_engine, soc_info_item['title'])
            logger.info(f"Processed data for {soc_info_item['soc_code']}:")
            for key, value in data.items():
                if isinstance(value, str) and len(value) > 100: # Don't print huge JSON strings
                    logger.info(f"  {key}: {value[:100]}...")
                else:
                    logger.info(f"  {key}: {value}")
            logger.info("--------------------------------------------------")
            time.sleep(1) # Respect API rate limits even in testing

        # Test fetching for a job title that might not be in TARGET_SOC_CODES
        # but should be in JOB_TITLE_TO_SOC_STATIC
        logger.info("--- Testing Job Title: 'Software Engineer' ---")
        data_sw_eng = get_bls_data_for_job_title("Software Engineer", test_engine)
        logger.info("Processed data for 'Software Engineer':")
        for key, value in data_sw_eng.items():
            if isinstance(value, str) and len(value) > 100:
                 logger.info(f"  {key}: {value[:100]}...")
            else:
                logger.info(f"  {key}: {value}")
        logger.info("--------------------------------------------------")

    except ValueError as ve:
        logger.error(f"Test run failed due to configuration error: {ve}")
    except Exception as e:
        logger.error(f"An error occurred during testing: {e}", exc_info=True)
    finally:
        if test_engine:
            test_engine.dispose()
            logger.info("Test: Database engine disposed.")
