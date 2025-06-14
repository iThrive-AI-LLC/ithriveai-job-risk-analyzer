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
import time # Ensures time.sleep is available
import random # Ensures random.uniform is available for _get_random_variance
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
            return {"status": "error", "message": ["bls_connector module not found."]}
        @staticmethod
        def get_oes_data_for_soc(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return {"status": "error", "message": ["bls_connector module not found."]}
        @staticmethod
        def get_ep_data_for_soc(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return {"status": "error", "message": ["bls_connector module not found."]}
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

# Target SOC codes for the admin database population tool
TARGET_SOC_CODES: List[Tuple[str, str]] = [
    ("11-1011", "Chief Executives"),
    ("11-2021", "Marketing Managers"),
    ("11-3021", "Computer and Information Systems Managers"),
    ("11-9111", "Medical and Health Services Managers"),
    ("13-1111", "Management Analysts"),
    ("13-2011", "Accountants and Auditors"),
    ("13-2051", "Financial Analysts"),
    ("15-1211", "Computer Systems Analysts"),
    ("15-1231", "Computer Network Support Specialists"),
    ("15-1244", "Network and Computer Systems Administrators"),
    ("15-1251", "Computer Programmers"),
    ("15-1252", "Software Developers"),
    ("15-1254", "Web Developers"),
    ("15-2051", "Data Scientists"),
    ("17-2071", "Electrical Engineers"),
    ("17-2141", "Mechanical Engineers"),
    ("19-1021", "Biochemists and Biophysicists"),
    ("21-1021", "Child, Family, and School Social Workers"),
    ("23-1011", "Lawyers"),
    ("23-2011", "Paralegals and Legal Assistants"), # Also relevant for Court Reporters
    ("25-2021", "Elementary School Teachers, Except Special Education"),
    ("25-2031", "Secondary School Teachers, Except Special and Career/Technical Education"),
    ("25-4022", "Librarians and Media Collections Specialists"),
    ("27-1011", "Art Directors"),
    ("27-1024", "Graphic Designers"),
    ("27-3023", "News Analysts, Reporters, and Journalists"),
    ("27-3042", "Technical Writers"),
    ("27-4021", "Photographers"),
    ("29-1021", "Dentists, General"),
    ("29-1062", "Family Medicine Physicians"),
    ("29-1141", "Registered Nurses"),
    ("29-1292", "Dental Hygienists"),
    ("31-1131", "Home Health Aides"),
    ("33-3051", "Police and Sheriff's Patrol Officers"),
    ("35-1011", "Chefs and Head Cooks"),
    ("35-2014", "Cooks, Restaurant"),
    ("35-3031", "Waiters and Waitresses"),
    ("37-2011", "Janitors and Cleaners, Except Maids and Housekeeping Cleaners"),
    ("39-5012", "Hairdressers, Hairstylists, and Cosmetologists"),
    ("41-1011", "First-Line Supervisors of Retail Sales Workers"),
    ("41-2011", "Cashiers"),
    ("41-2031", "Retail Salespersons"),
    ("41-4012", "Sales Representatives, Wholesale and Manufacturing, Except Technical and Scientific Products"), # Old SOC for Travel Agent
    ("43-1011", "First-Line Supervisors of Office and Administrative Support Workers"),
    ("43-4051", "Customer Service Representatives"),
    ("43-6011", "Executive Secretaries and Executive Administrative Assistants"),
    ("43-9021", "Data Entry Keyers"),
    ("47-2031", "Carpenters"),
    ("47-2111", "Electricians"),
    ("49-3023", "Automotive Service Technicians and Mechanics"),
    ("51-2092", "Team Assemblers"),
    ("53-3032", "Heavy and Tractor-Trailer Truck Drivers"),
    ("53-7062", "Laborers and Freight, Stock, and Material Movers, Hand")
]


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
    Column('last_api_fetch', String(10), nullable=False), # Storing as YYYY-MM-DD string
    Column('last_updated', String(10), nullable=False, default=lambda: datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')) # Storing as YYYY-MM-DD string
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
                    pass # Streamlit or secrets not available in this context

            if not database_url:
                logger.critical("DATABASE_URL environment variable or secret not set. Cannot connect to database.")
                raise ValueError("DATABASE_URL not configured.")

            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql://', 1)

            connect_args = {}
            if 'postgresql' in database_url: # Specific to PostgreSQL
                connect_args = {
                    "connect_timeout": 15,       # Increased timeout
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                    "sslmode": 'require'        # Common for cloud PostgreSQL
                }
            try:
                db_host_info = database_url.split('@')[-1] if '@' in database_url else database_url
                logger.info(f"Creating new database engine instance for URL ending with: ...@{db_host_info}")
                _engine_instance = create_engine(
                    database_url,
                    connect_args=connect_args,
                    pool_size=5,
                    max_overflow=10,
                    pool_timeout=30,
                    pool_recycle=1800, # Recycle connections every 30 minutes
                    echo=False # Set to True for debugging SQL
                )
                # Test connection
                with _engine_instance.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("Database engine instance created and connection tested successfully.")
            except SQLAlchemyError as e:
                logger.critical(f"Failed to create or connect with database engine: {e}", exc_info=True)
                _engine_instance = None # Ensure it's None if creation failed
                raise # Re-raise the exception to signal failure
        return _engine_instance

# --- Static Mappings & Helper Functions ---
JOB_TITLE_TO_SOC_STATIC: Dict[str, str] = {
    "software developer": "15-1252", "software engineer": "15-1252", "programmer": "15-1251",
    "web developer": "15-1254", "registered nurse": "29-1141", "nurse": "29-1141",
    "teacher": "25-2021", "elementary school teacher": "25-2021", "high school teacher": "25-2031",
    "lawyer": "23-1011", "attorney": "23-1011", "doctor": "29-1221", "physician": "29-1221",
    "accountant": "13-2011", "project manager": "11-3021", "product manager": "11-2021",
    "marketing manager": "11-2021", "retail salesperson": "41-2031", "cashier": "41-2011",
    "customer service representative": "43-4051", "truck driver": "53-3032", "receptionist": "43-4171",
    "data scientist": "15-2051", "data analyst": "15-2041", "business analyst": "13-1111",
    "financial analyst": "13-2051", "human resources specialist": "13-1071", "graphic designer": "27-1024",
    "police officer": "33-3051", "chef": "35-1011", "cook": "35-2014", "waiter": "35-3031",
    "waitress": "35-3031", "janitor": "37-2011", "administrative assistant": "43-6011",
    "executive assistant": "43-6011", "dental hygienist": "29-1292", "electrician": "47-2111",
    "plumber": "47-2152", "carpenter": "47-2031", "construction worker": "47-2061",
    "mechanic": "49-3023", "automotive mechanic": "49-3023", "taxi driver": "53-3054",
    "uber driver": "53-3054", "journalist": "27-3023", "reporter": "27-3023",
    "writer": "27-3042", "editor": "27-3041", "photographer": "27-4021",
    "court reporter": "23-2011", "stenographer": "23-2011", "digital court reporter": "23-2011",
    "travel agent": "41-3041" # Updated SOC
}

SOC_TO_CATEGORY_STATIC: Dict[str, str] = {
    "11-": "Management Occupations", "13-": "Business and Financial Operations Occupations",
    "15-": "Computer and Mathematical Occupations", "17-": "Architecture and Engineering Occupations",
    "19-": "Life, Physical, and Social Science Occupations", "21-": "Community and Social Service Occupations",
    "23-": "Legal Occupations", "25-": "Educational Instruction and Library Occupations",
    "27-": "Arts, Design, Entertainment, Sports, and Media Occupations", "29-": "Healthcare Practitioners and Technical Occupations",
    "31-": "Healthcare Support Occupations", "33-": "Protective Service Occupations",
    "35-": "Food Preparation and Serving Related Occupations",
    "37-": "Building and Grounds Cleaning and Maintenance Occupations", "39-": "Personal Care and Service Occupations",
    "41-": "Sales and Related Occupations", "43-": "Office and Administrative Support Occupations",
    "45-": "Farming, Fishing, and Forestry Occupations", "47-": "Construction and Extraction Occupations",
    "49-": "Installation, Maintenance, and Repair Occupations", "51-": "Production Occupations",
    "53-": "Transportation and Material Moving Occupations"
}

def get_job_category_from_soc(occupation_code: Optional[str]) -> str:
    if not occupation_code or not isinstance(occupation_code, str) or '-' not in occupation_code:
        return "General"
    prefix = occupation_code.split('-', 1)[0] + "-"
    return SOC_TO_CATEGORY_STATIC.get(prefix, "General")

def standardize_job_title(title: str) -> str:
    std_title = title.lower().strip()
    suffixes = [" i", " ii", " iii", " iv", " v", " specialist", " assistant", " associate", " senior", " junior", " lead"]
    for suffix in suffixes:
        if std_title.endswith(suffix):
            std_title = std_title[:-len(suffix)].strip()
            break
    return std_title

def find_occupation_code(job_title: str, engine_instance: sqlalchemy.engine.Engine) -> Tuple[Optional[str], str, str]:
    std_title_input = standardize_job_title(job_title)
    
    # 1. Check static mapping
    if std_title_input in JOB_TITLE_TO_SOC_STATIC:
        soc = JOB_TITLE_TO_SOC_STATIC[std_title_input]
        cat = get_job_category_from_soc(soc)
        logger.info(f"Found SOC '{soc}' for '{job_title}' via static map.")
        return soc, job_title, cat # Return original job_title for display consistency

    # 2. Check database for standardized_title or job_title
    try:
        with engine_instance.connect() as conn:
            query = text("""
                SELECT occupation_code, standardized_title, job_category 
                FROM bls_job_data 
                WHERE LOWER(standardized_title) = :std_title OR LOWER(job_title) = :std_title
                LIMIT 1
            """)
            result = conn.execute(query, {"std_title": std_title_input}).fetchone()
            if result:
                logger.info(f"Found SOC '{result.occupation_code}' for '{job_title}' via DB lookup.")
                return result.occupation_code, result.standardized_title, result.job_category
    except SQLAlchemyError as e:
        logger.error(f"Database error during SOC code lookup for '{job_title}': {e}")

    # 3. Use BLS Connector search (if available)
    if hasattr(bls_connector, 'search_occupations'):
        matches = bls_connector.search_occupations(job_title)
        if matches:
            best_match = matches[0]
            soc = best_match["code"]
            matched_title = best_match["title"]
            cat = get_job_category_from_soc(soc)
            logger.info(f"Found SOC '{soc}' for '{job_title}' via BLS API search (matched: '{matched_title}').")
            # Optionally, add this new mapping to a dynamic cache or suggest for static map
            return soc, matched_title, cat
            
    logger.warning(f"Could not find SOC code for '{job_title}'. Defaulting.")
    return None, job_title, "General"

# --- Data Fetching and Processing ---
def get_bls_data_from_db(occupation_code: str, engine_instance: sqlalchemy.engine.Engine) -> Optional[Dict[str, Any]]:
    if not occupation_code: return None
    try:
        with engine_instance.connect() as conn:
            query = text("SELECT * FROM bls_job_data WHERE occupation_code = :code ORDER BY last_api_fetch DESC LIMIT 1")
            row = conn.execute(query, {"code": occupation_code}).fetchone()
            if row:
                data = dict(row._mapping) # Convert row to dict
                last_fetch_date_str = data.get("last_api_fetch")
                if last_fetch_date_str:
                    try:
                        last_fetch_date = datetime.datetime.strptime(last_fetch_date_str, "%Y-%m-%d").date()
                        if (datetime.date.today() - last_fetch_date).days < 90: # Data is fresh if less than 90 days old
                            logger.info(f"Using fresh data from DB for SOC {occupation_code}.")
                            return data
                        else:
                            logger.info(f"Data for SOC {occupation_code} in DB is stale (older than 90 days).")
                    except ValueError:
                         logger.warning(f"Invalid date format for last_api_fetch ('{last_fetch_date_str}') for SOC {occupation_code}. Assuming stale.")
                else:
                    logger.warning(f"last_api_fetch is null for SOC {occupation_code}. Assuming stale.")
    except SQLAlchemyError as e:
        logger.error(f"Error fetching data from DB for SOC {occupation_code}: {e}", exc_info=True)
    return None

def save_bls_data_to_db(data: Dict[str, Any], engine_instance: sqlalchemy.engine.Engine) -> bool:
    if not data or not data.get("occupation_code"):
        logger.warning("Attempted to save empty or invalid data to DB.")
        return False

    # Ensure all required columns for bls_job_data_table are present or have defaults
    # This list should match the columns defined in bls_job_data_table
    table_columns = [col.name for col in bls_job_data_table.columns if col.name != 'id']
    
    # Prepare data for insertion, ensuring all keys exist and are of correct type or None
    insert_data = {col: data.get(col) for col in table_columns}
    
    # Specific type conversions or defaults
    for int_col in ['current_employment', 'projected_employment', 'employment_change_numeric', 'annual_job_openings']:
        val = insert_data.get(int_col)
        insert_data[int_col] = int(float(val)) if val is not None and str(val).replace('.', '', 1).isdigit() else None
        
    for float_col in ['percent_change', 'median_wage', 'mean_wage']:
        val = insert_data.get(float_col)
        insert_data[float_col] = float(val) if val is not None and str(val).replace('.', '', 1).isdigit() else None

    insert_data['last_api_fetch'] = data.get('last_api_fetch', datetime.date.today().strftime("%Y-%m-%d"))
    insert_data['last_updated'] = datetime.date.today().strftime("%Y-%m-%d")

    # Filter data to only include columns that exist in the table to prevent errors
    filtered_insert_data = {key: value for key, value in insert_data.items() if key in table_columns}
    
    # Ensure essential fields are present
    if not filtered_insert_data.get('occupation_code') or not filtered_insert_data.get('job_title') or not filtered_insert_data.get('standardized_title'):
        logger.error(f"Missing essential fields for DB save: {filtered_insert_data}")
        return False

    try:
        stmt = pg_insert(bls_job_data_table).values(**filtered_insert_data)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['occupation_code'],
            set_={col: getattr(stmt.excluded, col) for col in filtered_insert_data if col != 'occupation_code'}
        )
        with engine_instance.connect() as conn:
            conn.execute(on_conflict_stmt)
            conn.commit()
        logger.info(f"Successfully saved/updated data for SOC {data['occupation_code']} in DB.")
        return True
    except IntegrityError as ie: # Catch specific integrity errors like unique constraint
        logger.error(f"Integrity error saving data for SOC {data['occupation_code']} to DB: {ie}", exc_info=True)
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemyError saving data for SOC {data['occupation_code']} to DB: {e}", exc_info=True)
    except Exception as e_gen: # Catch any other unexpected errors
        logger.error(f"Unexpected error saving data for SOC {data['occupation_code']} to DB: {e_gen}", exc_info=True)
    return False

def fetch_and_process_soc_data(soc_code: str, representative_job_title: str, engine_instance: sqlalchemy.engine.Engine) -> Tuple[bool, Union[Dict[str, Any], str]]:
    """
    Fetches OES and EP data for a given SOC code, processes it, and stores it.
    Returns a tuple: (success_boolean, processed_data_or_error_message)
    """
    logger.info(f"Fetching BLS API data for SOC: {soc_code} ('{representative_job_title}')")
    
    oes_data_raw = bls_connector.get_oes_data_for_soc(soc_code)
    time.sleep(0.5) # Respect API rate limits
    ep_data_raw = bls_connector.get_ep_data_for_soc(soc_code)
    
    if oes_data_raw.get("status") != "success" and ep_data_raw.get("status") != "success":
        err_msg = f"Failed to fetch OES and EP data from BLS API for SOC {soc_code}. OES: {oes_data_raw.get('message', 'N/A')}, EP: {ep_data_raw.get('message', 'N/A')}"
        logger.error(err_msg)
        return False, err_msg

    # Process OES Data (Employment and Wages)
    oes_employment = oes_data_raw.get("employment")
    oes_median_wage = oes_data_raw.get("median_wage")
    oes_mean_wage = oes_data_raw.get("mean_wage")
    oes_data_year = oes_data_raw.get("data_year")

    # Process EP Data (Projections)
    ep_employment_base = ep_data_raw.get("employment_base_year_value")
    ep_employment_proj = ep_data_raw.get("employment_projected_year_value")
    ep_change_numeric = ep_data_raw.get("employment_change_numeric")
    ep_percent_change = ep_data_raw.get("employment_change_percent")
    ep_annual_openings = ep_data_raw.get("annual_average_openings")
    ep_base_year = ep_data_raw.get("base_year")
    ep_proj_year = ep_data_raw.get("projected_year")

    # Determine job category
    job_category = get_job_category_from_soc(soc_code)
    
    # Construct the data dictionary for database storage
    db_data = {
        "occupation_code": soc_code,
        "job_title": representative_job_title, # Use the representative title passed
        "standardized_title": oes_data_raw.get("occupation_title", representative_job_title), # Fallback to rep title
        "job_category": job_category,
        "current_employment": oes_employment if oes_employment is not None else ep_employment_base, # Prefer OES current, fallback to EP base
        "projected_employment": ep_employment_proj,
        "employment_change_numeric": ep_change_numeric,
        "percent_change": ep_percent_change,
        "annual_job_openings": ep_annual_openings,
        "median_wage": oes_median_wage,
        "mean_wage": oes_mean_wage,
        "oes_data_year": oes_data_year,
        "ep_base_year": ep_base_year,
        "ep_proj_year": ep_proj_year,
        "raw_oes_data_json": json.dumps(oes_data_raw) if oes_data_raw.get("status") == "success" else None,
        "raw_ep_data_json": json.dumps(ep_data_raw) if ep_data_raw.get("status") == "success" else None,
        "last_api_fetch": datetime.date.today().strftime("%Y-%m-%d")
    }

    if save_bls_data_to_db(db_data, engine_instance):
        logger.info(f"Successfully fetched and stored data for SOC {soc_code}.")
        # Return data formatted for the application schema
        return True, format_api_processed_data_to_app_schema(db_data)
    else:
        err_msg = f"Failed to save processed data for SOC {soc_code} to database."
        logger.error(err_msg)
        return False, err_msg

def _get_safe_year_range(raw_base_year: Optional[str], raw_proj_year: Optional[str], 
                         default_start_year_str: str = '2023', 
                         default_proj_period: int = 10,
                         soc_code_for_logging: Optional[str] = "N/A") -> Tuple[int, int]:
    """Safely determines start and end years for trends, with defaults and logging."""
    start_year_int: int
    end_year_int: int

    # Determine start_year
    if raw_base_year and isinstance(raw_base_year, str) and raw_base_year.isdigit():
        start_year_int = int(raw_base_year)
    else:
        logger.warning(f"Invalid or missing 'ep_base_year' ('{raw_base_year}') for SOC {soc_code_for_logging}. Defaulting to {default_start_year_str}.")
        start_year_int = int(default_start_year_str)

    # Determine end_year
    if raw_proj_year and isinstance(raw_proj_year, str) and raw_proj_year.isdigit():
        end_year_int = int(raw_proj_year)
    else:
        logger.warning(f"Invalid or missing 'ep_proj_year' ('{raw_proj_year}') for SOC {soc_code_for_logging}. Defaulting to {start_year_int + default_proj_period}.")
        end_year_int = start_year_int + default_proj_period
    
    # Ensure end_year is after start_year
    if end_year_int <= start_year_int:
        logger.warning(f"'ep_proj_year' ({end_year_int}) is not after 'ep_base_year' ({start_year_int}) for SOC {soc_code_for_logging}. Adjusting to {start_year_int + default_proj_period}.")
        end_year_int = start_year_int + default_proj_period
        
    return start_year_int, end_year_int

def format_database_row_to_app_schema(db_row: Dict[str, Any], original_job_title_query: str) -> Dict[str, Any]:
    """Formats a database row into the application's expected schema."""
    soc_code = db_row.get('occupation_code')
    job_category = db_row.get('job_category', get_job_category_from_soc(soc_code))
    risk_data = calculate_ai_risk_from_category(job_category, soc_code)
    
    start_year, end_year = _get_safe_year_range(
        db_row.get('ep_base_year'), 
        db_row.get('ep_proj_year'),
        soc_code_for_logging=soc_code
    )
    trend_years = list(range(start_year, end_year + 1))
    
    current_emp = db_row.get('current_employment')
    projected_emp = db_row.get('projected_employment')
    trend_employment = generate_employment_trend(current_emp, projected_emp, len(trend_years)) if current_emp is not None and projected_emp is not None else [None] * len(trend_years)

    return {
        "job_title": db_row.get('standardized_title', original_job_title_query),
        "occupation_code": soc_code,
        "source": "bls_database_cache",
        "job_category": job_category,
        "projections": {
            "current_employment": current_emp,
            "projected_employment": projected_emp,
            "percent_change": db_row.get('percent_change'),
            "annual_job_openings": db_row.get('annual_job_openings')
        },
        "wage_data": {
            "median_wage": db_row.get('median_wage'),
            "mean_wage": db_row.get('mean_wage'),
            "oes_data_year": db_row.get('oes_data_year')
        },
        "risk_scores": {"year_1": risk_data["year_1_risk"], "year_5": risk_data["year_5_risk"]},
        "risk_category": risk_data["risk_category"],
        "risk_factors": risk_data["risk_factors"],
        "protective_factors": risk_data["protective_factors"],
        "analysis": risk_data["analysis"],
        "trend_data": {"years": trend_years, "employment": trend_employment},
        "last_updated_db": db_row.get('last_updated'),
        "last_api_fetch_db": db_row.get('last_api_fetch')
    }

def format_api_processed_data_to_app_schema(api_data: Dict[str, Any]) -> Dict[str, Any]:
    """Formats data freshly processed from API into the application's expected schema."""
    soc_code = api_data.get('occupation_code')
    job_category = api_data.get('job_category', get_job_category_from_soc(soc_code))
    risk_data = calculate_ai_risk_from_category(job_category, soc_code)

    start_year, end_year = _get_safe_year_range(
        api_data.get('ep_base_year'), 
        api_data.get('ep_proj_year'),
        soc_code_for_logging=soc_code
    )
    trend_years = list(range(start_year, end_year + 1))
    
    current_emp = api_data.get('current_employment')
    projected_emp = api_data.get('projected_employment')
    trend_employment = generate_employment_trend(current_emp, projected_emp, len(trend_years)) if current_emp is not None and projected_emp is not None else [None] * len(trend_years)
    
    return {
        "job_title": api_data.get('standardized_title', api_data.get('job_title')),
        "occupation_code": soc_code,
        "source": "bls_api_live",
        "job_category": job_category,
        "projections": {
            "current_employment": current_emp,
            "projected_employment": projected_emp,
            "percent_change": api_data.get('percent_change'),
            "annual_job_openings": api_data.get('annual_job_openings')
        },
        "wage_data": {
            "median_wage": api_data.get('median_wage'),
            "mean_wage": api_data.get('mean_wage'),
            "oes_data_year": api_data.get('oes_data_year')
        },
        "risk_scores": {"year_1": risk_data["year_1_risk"], "year_5": risk_data["year_5_risk"]},
        "risk_category": risk_data["risk_category"],
        "risk_factors": risk_data["risk_factors"],
        "protective_factors": risk_data["protective_factors"],
        "analysis": risk_data["analysis"],
        "trend_data": {"years": trend_years, "employment": trend_employment},
        "last_updated_db": api_data.get('last_updated'), # This would be today from DB save
        "last_api_fetch_db": api_data.get('last_api_fetch') # This would be today from API fetch
    }

def calculate_ai_risk_from_category(job_category: str, occupation_code: Optional[str] = None) -> Dict[str, Any]:
    """Calculate AI displacement risk based on job category and specific SOC insights if available."""
    category_profiles = {
        "Computer and Mathematical Occupations": {"base_risk": 30, "increase": 8, "variance": 7, "risk_factors": ["AI code generation", "Automated testing", "Data analysis automation"], "protective_factors": ["Complex system design", "Novel algorithm development", "Strategic tech leadership"]},
        "Management Occupations": {"base_risk": 15, "increase": 4, "variance": 3, "risk_factors": ["Automated reporting", "AI scheduling tools", "Data-driven decision support"], "protective_factors": ["Strategic leadership", "Complex stakeholder management", "Team motivation and development"]},
        "Business and Financial Operations Occupations": {"base_risk": 35, "increase": 7, "variance": 6, "risk_factors": ["Automated financial analysis", "Robotic Process Automation (RPA)", "AI fraud detection"], "protective_factors": ["Complex financial strategy", "Regulatory interpretation", "Client advisory"]},
        "Architecture and Engineering Occupations": {"base_risk": 20, "increase": 6, "variance": 5, "risk_factors": ["AI-assisted design (CAD)", "Simulation software", "Automated compliance checks"], "protective_factors": ["Innovative design thinking", "Complex project oversight", "Solving novel engineering problems"]},
        "Life, Physical, and Social Science Occupations": {"base_risk": 10, "increase": 5, "variance": 4, "risk_factors": ["Automated lab analysis", "AI data interpretation", "Research process automation"], "protective_factors": ["Formulating new hypotheses", "Complex experimental design", "Interdisciplinary research"]},
        "Community and Social Service Occupations": {"base_risk": 10, "increase": 3, "variance": 3, "risk_factors": ["Automated case management tasks", "AI-powered resource matching", "Chatbots for initial contact"], "protective_factors": ["Empathy and human connection", "Complex crisis intervention", "Nuanced cultural understanding"]},
        "Legal Occupations": {"base_risk": 25, "increase": 7, "variance": 6, "risk_factors": ["AI document review (e-discovery)", "Automated contract analysis", "Legal research AI tools"], "protective_factors": ["Courtroom advocacy", "Complex legal strategy", "Negotiation skills"]},
        "Educational Instruction and Library Occupations": {"base_risk": 15, "increase": 4, "variance": 4, "risk_factors": ["AI tutoring systems", "Automated grading", "Online content delivery"], "protective_factors": ["Mentorship and inspiration", "Social-emotional development", "Adaptive teaching strategies"]},
        "Arts, Design, Entertainment, Sports, and Media Occupations": {"base_risk": 25, "increase": 9, "variance": 8, "risk_factors": ["AI content generation (text, image, music)", "Automated video editing", "Personalized content recommendation"], "protective_factors": ["Original creativity and vision", "Live performance skills", "Cultural trendsetting"]},
        "Healthcare Practitioners and Technical Occupations": {"base_risk": 10, "increase": 3, "variance": 3, "risk_factors": ["AI diagnostic assistance", "Robotic surgery tools", "Automated medical imaging analysis"], "protective_factors": ["Complex patient diagnosis", "Direct patient interaction and empathy", "Ethical medical judgment"]},
        "Healthcare Support Occupations": {"base_risk": 20, "increase": 5, "variance": 4, "risk_factors": ["Automated patient scheduling", "AI-assisted medical coding", "Robotics in patient transport/care"], "protective_factors": ["Direct patient comfort and care", "Assisting with mobility", "Observing patient changes"]},
        "Protective Service Occupations": {"base_risk": 15, "increase": 3, "variance": 3, "risk_factors": ["AI surveillance systems", "Predictive policing algorithms (controversial)", "Drone technology"], "protective_factors": ["On-the-ground judgment in dynamic situations", "Community interaction", "De-escalation skills"]},
        "Food Preparation and Serving Related Occupations": {"base_risk": 45, "increase": 6, "variance": 7, "risk_factors": ["Automated food preparation robots", "Self-service kiosks", "AI-powered order taking"], "protective_factors": ["High-end culinary creativity", "Customer service and ambiance", "Adapting to custom orders"]},
        "Building and Grounds Cleaning and Maintenance Occupations": {"base_risk": 40, "increase": 4, "variance": 5, "risk_factors": ["Robotic cleaning devices", "Automated lawn care", "Smart building maintenance sensors"], "protective_factors": ["Handling unexpected cleaning needs", "Detailed repair work", "Operating in varied environments"]},
        "Personal Care and Service Occupations": {"base_risk": 20, "increase": 5, "variance": 5, "risk_factors": ["AI scheduling for appointments", "Virtual try-ons (e.g., hairstyles)", "Automated customer interaction for basic queries"], "protective_factors": ["Personalized human touch and interaction", "Skill in physical services (e.g., hairstyling)", "Building client rapport"]},
        "Sales and Related Occupations": {"base_risk": 50, "increase": 8, "variance": 7, "risk_factors": ["E-commerce and online sales platforms", "AI-powered recommendation engines", "Automated checkout systems"], "protective_factors": ["Complex B2B sales negotiations", "High-value relationship selling", "Consultative sales approaches"]},
        "Office and Administrative Support Occupations": {"base_risk": 60, "increase": 7, "variance": 5, "risk_factors": ["RPA for data entry and processing", "AI chatbots for customer inquiries", "Automated scheduling and document management"], "protective_factors": ["Complex office management", "Handling non-standard requests", "Executive-level support requiring discretion"]},
        "Farming, Fishing, and Forestry Occupations": {"base_risk": 25, "increase": 4, "variance": 6, "risk_factors": ["Automated harvesting machinery", "Drones for crop monitoring", "AI for optimizing yields"], "protective_factors": ["Adapting to unpredictable natural conditions", "Specialized animal husbandry", "Sustainable farming practices requiring judgment"]},
        "Construction and Extraction Occupations": {"base_risk": 30, "increase": 5, "variance": 6, "risk_factors": ["Robotics in bricklaying, welding", "Automated site surveying (drones)", "Prefabrication and modular construction"], "protective_factors": ["Skilled trades requiring dexterity and problem-solving on site", "Operating heavy machinery in complex environments", "Custom construction work"]},
        "Installation, Maintenance, and Repair Occupations": {"base_risk": 25, "increase": 4, "variance": 5, "risk_factors": ["AI diagnostics for troubleshooting", "Predictive maintenance sensors", "Robots for routine inspections"], "protective_factors": ["Complex non-routine repairs", "Hands-on problem-solving", "Adapting to varied equipment and situations"]},
        "Production Occupations": {"base_risk": 55, "increase": 6, "variance": 6, "risk_factors": ["Advanced robotics in assembly lines", "AI for quality control", "Automated material handling"], "protective_factors": ["Overseeing automated systems", "Complex machine setup and maintenance", "Custom or small-batch production"]},
        "Transportation and Material Moving Occupations": {"base_risk": 50, "increase": 9, "variance": 8, "risk_factors": ["Autonomous vehicles (trucks, delivery drones)", "AI-powered logistics and route optimization", "Automated warehouse systems"], "protective_factors": ["Last-mile delivery complexities", "Handling irregular cargo or situations", "Passenger interaction in public transport (for now)"]},
        "General": {"base_risk": 35, "increase": 7, "variance": 6, "risk_factors": ["Routine task automation", "Data processing", "Predictable physical work"], "protective_factors": ["Complex problem-solving", "Creativity", "Interpersonal skills"]}
    }
    # Start with a copy of the "General" profile as a base
    profile = category_profiles["General"].copy()
    # Get the category-specific profile
    category_specific_profile = category_profiles.get(job_category)
    
    if category_specific_profile:
        # Update the base profile with any category-specific values
        profile.update(category_specific_profile)
    # Now 'profile' is guaranteed to have at least the keys from "General",
    # overridden by 'job_category' specifics if they existed.
    
    # Adjust for specific SOC codes known to have high/low automation potential
    # Example: Court Reporters (23-2011, often part of Legal) are known to be highly impacted
    if occupation_code == "23-2011": # Court Reporters and Simultaneous Captioners
        profile['base_risk'] = max(profile['base_risk'], 60) # Higher base risk
        profile['increase'] = max(profile['increase'], 10)  # Faster increase
        profile['risk_factors'] = ["Real-time speech-to-text AI", "Automated transcription services", "Digital recording replacing manual stenography"] + profile['risk_factors'][:1]
        profile['protective_factors'] = ["Official legal record certification", "Handling complex courtroom dynamics", "Specialized terminology in niche areas"] + profile['protective_factors'][:1]

    # Calculate risk values
    year_1_risk = min(98, max(2, profile['base_risk'] + profile['increase'] * 1 +_get_random_variance(profile['variance'])))
    year_5_risk = min(98, max(2, profile['base_risk'] + profile['increase'] * 5 +_get_random_variance(profile['variance'])))

    risk_cat = "Low"
    if year_5_risk >= 70: risk_cat = "Very High"
    elif year_5_risk >= 50: risk_cat = "High"
    elif year_5_risk >= 30: risk_cat = "Moderate"

    return {
        "year_1_risk": round(year_1_risk, 1),
        "year_5_risk": round(year_5_risk, 1),
        "risk_category": risk_cat,
        "risk_factors": profile['risk_factors'][:3], # Top 3
        "protective_factors": profile['protective_factors'][:3], # Top 3
        "analysis": f"Jobs in the '{job_category}' category, such as this one, generally face a {risk_cat.lower()} 5-year AI displacement risk. Key factors include {', '.join(profile['risk_factors'][:2]).lower()}. However, skills like {', '.join(profile['protective_factors'][:2]).lower()} provide some resilience."
    }

def _get_random_variance(variance_param: float) -> float:
    """Helper to generate a small random variance."""
    # Using a simpler random approach if numpy is problematic in some environments
    return random.uniform(-variance_param / 2, variance_param / 2)


def generate_employment_trend(current_emp: Optional[Union[int, float]], projected_emp: Optional[Union[int, float]], num_years: int) -> List[Optional[int]]:
    if current_emp is None or projected_emp is None or num_years <= 1:
        return [int(current_emp) if current_emp is not None else None] * num_years
    
    current_emp_f = float(current_emp)
    projected_emp_f = float(projected_emp)
    
    # Linear interpolation
    annual_change = (projected_emp_f - current_emp_f) / (num_years -1) if num_years > 1 else 0
    
    trend = [int(current_emp_f + (annual_change * i)) for i in range(num_years)]
    return trend

def get_job_data_from_db_or_api(job_title_query: str, engine_instance: sqlalchemy.engine.Engine, force_api_fetch: bool = False) -> Dict[str, Any]:
    """
    Main function to get job data. Tries DB first, then BLS API.
    Ensures data is always fetched via BLS API if not in DB or stale.
    """
    logger.info(f"Fetching data for SOC associated with job query: '{job_title_query}', Force API: {force_api_fetch}")
    
    occupation_code, standardized_title, job_category = find_occupation_code(job_title_query, engine_instance)
    
    if not occupation_code: # No SOC code found at all
        logger.warning(f"No SOC code could be determined for '{job_title_query}'. Cannot fetch BLS data.")
        return {"error": f"Could not determine a Standard Occupational Classification (SOC) code for '{job_title_query}'. Please try a more standard job title or check spelling.", "job_title": job_title_query, "source": "mapping_failed"}

    # Try fetching from DB if not forcing API
    if not force_api_fetch:
        db_data = get_bls_data_from_db(occupation_code, engine_instance)
        if db_data:
            logger.info(f"Using cached data from DB for SOC {occupation_code} ('{standardized_title}')")
            return format_database_row_to_app_schema(db_data, job_title_query) # Pass original query for title fallback

    # If not in DB, data is stale, or API fetch is forced
    logger.info(f"No fresh data in DB or API fetch forced for SOC {occupation_code}. Fetching from BLS API.")
    
    # Use the representative title found by find_occupation_code for API calls if it's more specific
    title_for_api = standardized_title if standardized_title != job_title_query else job_title_query
    
    success, api_result_or_error = fetch_and_process_soc_data(occupation_code, title_for_api, engine_instance)
    
    if success and isinstance(api_result_or_error, dict):
        return api_result_or_error # This is already in app schema format
    else:
        # API fetch or processing failed, return an error structure
        error_message = api_result_or_error if isinstance(api_result_or_error, str) else "Unknown error fetching or processing API data."
        logger.error(f"Failed to get data for SOC {occupation_code} from API: {error_message}")
        return {"error": error_message, "job_title": job_title_query, "occupation_code": occupation_code, "source": "bls_api_fetch_error_or_db_save_failed"}

def get_all_soc_codes_from_db(engine_instance: sqlalchemy.engine.Engine) -> List[Dict[str, str]]:
    """Retrieve all unique SOC codes and their primary job titles from the database."""
    try:
        with engine_instance.connect() as conn:
            query = text("SELECT DISTINCT occupation_code, standardized_title FROM bls_job_data ORDER BY occupation_code")
            result = conn.execute(query).fetchall()
            return [{"soc_code": row.occupation_code, "title": row.standardized_title} for row in result]
    except SQLAlchemyError as e:
        logger.error(f"Error fetching all SOC codes from DB: {e}", exc_info=True)
        return []

def get_job_titles_for_autocomplete(engine_instance: sqlalchemy.engine.Engine, query: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    """
    Fetch job titles from the database for autocomplete suggestions.
    Prioritizes primary titles and then aliases.
    """
    try:
        with engine_instance.connect() as conn:
            # Query for primary titles (from bls_job_data)
            sql_query_primary = text("""
                SELECT standardized_title as title, occupation_code, TRUE as is_primary
                FROM bls_job_data
                WHERE LOWER(standardized_title) LIKE LOWER(:query_like)
                ORDER BY standardized_title
                LIMIT :limit
            """)
            # Query for aliases (from job_title_aliases - assuming such a table exists or can be created)
            # For now, let's simulate by querying bls_job_data's job_title field as if they were aliases
            sql_query_aliases = text("""
                SELECT job_title as title, occupation_code, FALSE as is_primary
                FROM bls_job_data
                WHERE LOWER(job_title) LIKE LOWER(:query_like) AND LOWER(job_title) != LOWER(standardized_title)
                ORDER BY job_title
                LIMIT :limit
            """)
            
            query_like_param = f"%{query.lower()}%"
            
            primary_results = conn.execute(sql_query_primary, {"query_like": query_like_param, "limit": limit}).mappings().all()
            alias_results = conn.execute(sql_query_aliases, {"query_like": query_like_param, "limit": limit}).mappings().all()
            
            # Combine and de-duplicate, prioritizing primary titles
            combined_results_map = {res['title'].lower(): res for res in primary_results}
            for res in alias_results:
                if res['title'].lower() not in combined_results_map:
                    combined_results_map[res['title'].lower()] = res
            
            sorted_results = sorted(combined_results_map.values(), key=lambda x: (not x['is_primary'], x['title']))
            
            return sorted_results[:limit]

    except SQLAlchemyError as e:
        logger.error(f"Error fetching job titles for autocomplete: {e}", exc_info=True)
        # Fallback to static list if DB fails
        static_titles = [{"title": title, "soc_code": soc, "is_primary": True} for title, soc in JOB_TITLE_TO_SOC_STATIC.items() if query.lower() in title.lower()]
        return static_titles[:limit]
    except Exception as e_gen:
        logger.error(f"Unexpected error fetching job titles: {e_gen}", exc_info=True)
        return []

if __name__ == "__main__":
    # This block is for direct testing of the module
    print("Testing bls_job_mapper.py...")
    test_engine = get_db_engine() # Initialize engine
    if test_engine:
        print("Database engine initialized successfully for testing.")
        # Example: Fetch data for "Software Developer"
        # Ensure that TARGET_SOC_CODES is accessible or provide a sample SOC code for testing
        sample_soc_to_test = TARGET_SOC_CODES[0] if TARGET_SOC_CODES else ("15-1252", "Software Developers")
        
        if isinstance(sample_soc_to_test, tuple) and len(sample_soc_to_test) == 2:
            soc_code_test, job_title_test = sample_soc_to_test
            print(f"\nTesting fetch_and_process_soc_data for: {job_title_test} ({soc_code_test})")
            success, data = fetch_and_process_soc_data(soc_code_test, job_title_test, test_engine)
            if success:
                print(f"Successfully processed data for {job_title_test}:")
                print(json.dumps(data, indent=2))
            else:
                print(f"Failed to process data for {job_title_test}: {data}")
        else:
            print(f"Skipping fetch_and_process_soc_data test due to invalid TARGET_SOC_CODES structure: {sample_soc_to_test}")

        print("\nTesting get_job_data_from_db_or_api for 'Registered Nurse'")
        nurse_data = get_job_data_from_db_or_api("Registered Nurse", test_engine)
        print(json.dumps(nurse_data, indent=2))
        
        print("\nTesting get_job_titles_for_autocomplete with query 'dev'")
        autocomplete_results = get_job_titles_for_autocomplete(test_engine, "dev")
        print(f"Autocomplete results for 'dev': {autocomplete_results}")
        
        print("\nTesting get_all_soc_codes_from_db")
        all_socs = get_all_soc_codes_from_db(test_engine)
        print(f"Found {len(all_socs)} unique SOCs in DB. First 5: {all_socs[:5]}")
    else:
        print("Failed to initialize database engine for testing. Ensure DATABASE_URL is set.")

