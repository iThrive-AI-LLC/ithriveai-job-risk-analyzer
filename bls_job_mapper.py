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
    "travel agent": "41-3041" # Updated SOC for Travel Agents
}

SOC_TO_CATEGORY_STATIC: Dict[str, str] = {
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
    "00-0000": "Unknown or Unclassified"
}

def get_job_category_from_soc(occupation_code: str) -> str:
    """Determines job category from SOC code prefix."""
    if not occupation_code or not isinstance(occupation_code, str):
        return "General"
    for prefix, category in SOC_TO_CATEGORY_STATIC.items():
        if occupation_code.startswith(prefix):
            return category
    return "General"

def standardize_job_title(title: str) -> str:
    """Standardizes job title for consistent mapping."""
    if not title or not isinstance(title, str):
        return ""
    std_title = title.lower().strip()
    suffixes_prefixes = [
        " i", " ii", " iii", " iv", " v", " specialist", " assistant", " associate",
        " senior", " sr.", " junior", " jr.", " lead", " head of", " chief ", " entry level",
        " intern", "(internship)", " trainee", " apprentice"
    ]
    for item in suffixes_prefixes:
        if item.startswith(" ") and std_title.endswith(item):
            std_title = std_title[:-len(item)]
        elif item.endswith(" ") and std_title.startswith(item):
            std_title = std_title[len(item):]
    std_title = re.sub(r'\s+', ' ', std_title).strip() # Normalize multiple spaces
    return std_title

def find_soc_code_and_title(job_title_query: str, engine: sqlalchemy.engine.Engine) -> Tuple[Optional[str], str, str]:
    """
    Finds SOC code and standardized title for a job query.
    Prioritizes static mapping, then database, then BLS API search.
    """
    standardized_query = standardize_job_title(job_title_query)
    logger.debug(f"Standardized job title query: '{job_title_query}' -> '{standardized_query}'")

    # 1. Check static mapping
    if standardized_query in JOB_TITLE_TO_SOC_STATIC:
        soc_code = JOB_TITLE_TO_SOC_STATIC[standardized_query]
        category = get_job_category_from_soc(soc_code)
        logger.debug(f"Found '{standardized_query}' in static map: SOC {soc_code}, Category: {category}")
        return soc_code, job_title_query, category # Return original query as title for consistency if found this way

    # 2. Check database for standardized title or original title
    try:
        with engine.connect() as conn:
            # Query for standardized title first
            query_db = text("SELECT occupation_code, standardized_title, job_category FROM bls_job_data WHERE LOWER(standardized_title) = LOWER(:query) LIMIT 1")
            result = conn.execute(query_db, {"query": standardized_query}).fetchone()
            if result:
                logger.debug(f"Found '{standardized_query}' (as standardized_title) in DB: SOC {result[0]}, Title: {result[1]}, Category: {result[2]}")
                return result[0], result[1], result[2]

            # Query for original job title if standardized not found
            query_db_orig = text("SELECT occupation_code, standardized_title, job_category FROM bls_job_data WHERE LOWER(job_title) = LOWER(:query) LIMIT 1")
            result_orig = conn.execute(query_db_orig, {"query": job_title_query.lower()}).fetchone()
            if result_orig:
                logger.debug(f"Found '{job_title_query}' (as job_title) in DB: SOC {result_orig[0]}, Title: {result_orig[1]}, Category: {result_orig[2]}")
                return result_orig[0], result_orig[1], result_orig[2]
    except SQLAlchemyError as e:
        logger.error(f"Database error during SOC code lookup for '{job_title_query}': {e}", exc_info=True)

    # 3. Use BLS API search as a last resort
    logger.debug(f"Job title '{job_title_query}' not found in static map or DB, querying BLS API.")
    api_matches = bls_connector.search_occupations(job_title_query)
    if api_matches:
        best_match = api_matches[0]
        soc_code = best_match["code"]
        matched_title = best_match["title"]
        category = get_job_category_from_soc(soc_code)
        logger.info(f"BLS API match for '{job_title_query}': SOC {soc_code}, Title: '{matched_title}', Category: {category}")
        # Add to static map for future in-session lookups
        JOB_TITLE_TO_SOC_STATIC[standardized_query] = soc_code
        JOB_TITLE_TO_SOC_STATIC[standardize_job_title(matched_title)] = soc_code
        return soc_code, matched_title, category

    logger.warning(f"Could not find SOC code for '{job_title_query}'. Returning default.")
    return "00-0000", job_title_query, "General"


# --- Main Data Processing Functions ---
def get_bls_data_from_db(occupation_code: str, engine: sqlalchemy.engine.Engine) -> Optional[Dict[str, Any]]:
    """Retrieves BLS data from the database if fresh."""
    if not occupation_code: return None
    try:
        with engine.connect() as conn:
            query = text("SELECT * FROM bls_job_data WHERE occupation_code = :code LIMIT 1")
            result = conn.execute(query, {"code": occupation_code}).fetchone()
            if result:
                data = dict(result._mapping) # type: ignore
                last_fetch_str = data.get("last_api_fetch")
                if last_fetch_str:
                    try:
                        last_fetch = datetime.datetime.strptime(last_fetch_str, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
                        if (datetime.datetime.now(datetime.timezone.utc) - last_fetch).days < 90: # Data is fresh if less than 90 days old
                            logger.info(f"Using cached BLS data for SOC {occupation_code} from database (fetched {last_fetch_str}).")
                            return data
                        else:
                            logger.info(f"Cached BLS data for SOC {occupation_code} is stale (fetched {last_fetch_str}).")
                    except ValueError as ve:
                        logger.warning(f"Could not parse last_api_fetch date '{last_fetch_str}' for SOC {occupation_code}: {ve}. Assuming stale.")
                else:
                    logger.warning(f"last_api_fetch is null for SOC {occupation_code}. Assuming stale.")
    except SQLAlchemyError as e:
        logger.error(f"Database error retrieving BLS data for SOC {occupation_code}: {e}", exc_info=True)
    return None

def save_bls_data_to_db(data_to_save: Dict[str, Any], engine: sqlalchemy.engine.Engine) -> bool:
    """Saves or updates BLS data in the database."""
    if not data_to_save or 'occupation_code' not in data_to_save:
        logger.warning("Attempted to save BLS data without occupation_code.")
        return False

    soc_code = data_to_save['occupation_code']
    logger.debug(f"Preparing to save data for SOC {soc_code}: {data_to_save}")

    # Ensure date fields are strings in YYYY-MM-DD format
    for date_field in ['last_api_fetch', 'last_updated']:
        if date_field in data_to_save and isinstance(data_to_save[date_field], datetime.datetime):
            data_to_save[date_field] = data_to_save[date_field].strftime('%Y-%m-%d')
        elif date_field in data_to_save and data_to_save[date_field] is None and bls_job_data_table.columns[date_field].nullable is False:
             data_to_save[date_field] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d') # Ensure non-nullable dates have a value


    # Filter data to match table columns and handle potential None for nullable numeric fields
    table_columns = {col.name for col in bls_job_data_table.columns}
    filtered_data_to_save = {k: (v if v is not None else sqlalchemy.null()) for k, v in data_to_save.items() if k in table_columns}


    # Ensure all required non-nullable fields are present or have defaults
    for col in bls_job_data_table.columns:
        if not col.nullable and col.name not in filtered_data_to_save and col.default is None and not col.primary_key:
            logger.error(f"Missing non-nullable value for column '{col.name}' when saving SOC {soc_code}.")
            return False # Or raise an error, or provide a sensible default if appropriate

    # Ensure 'last_updated' is always set
    filtered_data_to_save['last_updated'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')


    try:
        with engine.connect() as conn:
            stmt = pg_insert(bls_job_data_table).values(**filtered_data_to_save)
            update_dict = {k: getattr(stmt.excluded, k) for k in filtered_data_to_save if k != 'id' and k != 'occupation_code'}
            update_dict['last_updated'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d') # Ensure last_updated is updated on conflict

            stmt = stmt.on_conflict_do_update(
                index_elements=['occupation_code'],
                set_=update_dict
            )
            conn.execute(stmt)
            conn.commit()
        logger.info(f"Successfully saved/updated BLS data for SOC {soc_code}.")
        return True
    except IntegrityError as ie: # Handle specific integrity errors like unique constraints
        logger.error(f"Integrity error saving BLS data for SOC {soc_code}: {ie}", exc_info=True)
        # Potentially attempt an update if insert failed due to existing key, though on_conflict_do_update should handle this.
    except SQLAlchemyError as e:
        logger.error(f"Database error saving BLS data for SOC {soc_code}: {e}", exc_info=True)
    return False


def fetch_and_process_soc_data(soc_code: str, job_title_query: str, engine: sqlalchemy.engine.Engine, force_api_fetch: bool = False) -> Optional[Dict[str, Any]]:
    """Fetches data for a SOC code, from DB if fresh, else from BLS API, then processes and stores it."""
    logger.info(f"Fetching data for SOC: {soc_code}, Job Query: '{job_title_query}', Force API: {force_api_fetch}")

    if not force_api_fetch:
        db_data = get_bls_data_from_db(soc_code, engine)
        if db_data:
            return format_database_row_to_app_schema(db_data)

    logger.info(f"No fresh data in DB or API fetch forced for SOC {soc_code}. Fetching from BLS API.")
    current_time = datetime.datetime.now(datetime.timezone.utc)
    last_api_fetch_str = current_time.strftime('%Y-%m-%d')

    oes_data_raw = bls_connector.get_oes_data_for_soc(soc_code)
    time.sleep(0.5) # Respect API rate limits
    ep_data_raw = bls_connector.get_ep_data_for_soc(soc_code)

    if oes_data_raw.get("status") != "success" or ep_data_raw.get("status") != "success":
        logger.error(f"BLS API fetch failed for SOC {soc_code}. OES: {oes_data_raw.get('message', 'N/A')}, EP: {ep_data_raw.get('message', 'N/A')}")
        return None # Strict: if API fails, no data.

    # Process and combine data
    processed_data = {
        "occupation_code": soc_code,
        "job_title": job_title_query, # Use the original query for this field initially
        "standardized_title": oes_data_raw.get("occupation_name", job_title_query),
        "job_category": get_job_category_from_soc(soc_code),
        "current_employment": oes_data_raw.get("employment"),
        "projected_employment": ep_data_raw.get("projected_employment"),
        "employment_change_numeric": ep_data_raw.get("employment_change_numeric"),
        "percent_change": ep_data_raw.get("percent_change"),
        "annual_job_openings": ep_data_raw.get("annual_job_openings"),
        "median_wage": oes_data_raw.get("median_wage"),
        "mean_wage": oes_data_raw.get("mean_wage"),
        "oes_data_year": oes_data_raw.get("data_year"),
        "ep_base_year": ep_data_raw.get("base_year"),
        "ep_proj_year": ep_data_raw.get("projection_year"),
        "raw_oes_data_json": json.dumps(oes_data_raw),
        "raw_ep_data_json": json.dumps(ep_data_raw),
        "last_api_fetch": last_api_fetch_str,
        "last_updated": last_api_fetch_str # Set last_updated to last_api_fetch on new/updated data
    }

    if save_bls_data_to_db(processed_data, engine):
        return format_database_row_to_app_schema(processed_data)
    else:
        logger.error(f"Failed to save processed data for SOC {soc_code} to database.")
        # Return the processed data anyway if saving failed, but log the error.
        # This allows the app to function with fresh data even if DB save fails.
        return format_database_row_to_app_schema(processed_data)


def format_database_row_to_app_schema(db_row: Dict[str, Any]) -> Dict[str, Any]:
    """Formats a database row (as dict) to the schema expected by the application."""
    if not db_row: return {}

    job_category = db_row.get('job_category', get_job_category_from_soc(db_row.get('occupation_code', '')))
    risk_scores = calculate_ai_risk_from_category(job_category, db_row.get('occupation_code', ''))

    # Generate employment trend from current to projected
    current_emp = db_row.get('current_employment')
    projected_emp = db_row.get('projected_employment')
    trend_years_count = 11 # For a 10-year projection (e.g., 2022-2032)
    
    trend_employment = []
    trend_years = []

    if current_emp is not None and projected_emp is not None:
        base_year_str = db_row.get('ep_base_year', str(datetime.datetime.now().year - 1))
        proj_year_str = db_row.get('ep_proj_year', str(int(base_year_str) + 10))
        try:
            base_year = int(base_year_str)
            proj_year = int(proj_year_str)
            num_projection_years = proj_year - base_year
            if num_projection_years > 0:
                trend_years = list(range(base_year, proj_year + 1))
                trend_employment = [int(current_emp + (projected_emp - current_emp) * (i / num_projection_years)) for i in range(num_projection_years + 1)]
            else: # If years are same or invalid, just show current and projected
                trend_years = [base_year, proj_year] if base_year != proj_year else [base_year]
                trend_employment = [current_emp, projected_emp] if base_year != proj_year else [current_emp]

        except ValueError:
             logger.warning(f"Could not parse ep_base_year ('{base_year_str}') or ep_proj_year ('{proj_year_str}') as integers.")
             trend_years = list(range(datetime.datetime.now().year -1, datetime.datetime.now().year + 10)) # Default 10 year span
             trend_employment = generate_employment_trend(current_emp, projected_emp, len(trend_years)) if current_emp and projected_emp else []
    elif current_emp is not None: # Only current employment available
        base_year_str = db_row.get('ep_base_year', str(datetime.datetime.now().year - 1))
        try:
            base_year = int(base_year_str)
            trend_years = [base_year]
            trend_employment = [current_emp]
        except ValueError:
             logger.warning(f"Could not parse ep_base_year ('{base_year_str}') as integer.")


    return {
        "job_title": db_row.get('standardized_title', db_row.get('job_title', 'N/A')),
        "occupation_code": db_row.get('occupation_code'),
        "job_category": job_category,
        "bls_data": { # Nesting BLS specific fields
            "employment": db_row.get('current_employment'),
            "projected_employment": db_row.get('projected_employment'),
            "employment_change_numeric": db_row.get('employment_change_numeric'),
            "employment_change_percent": db_row.get('percent_change'),
            "annual_job_openings": db_row.get('annual_job_openings'),
            "median_wage": db_row.get('median_wage'),
            "mean_wage": db_row.get('mean_wage'),
            "oes_data_year": db_row.get('oes_data_year'),
            "ep_base_year": db_row.get('ep_base_year'),
            "ep_proj_year": db_row.get('ep_proj_year'),
            "last_api_fetch": db_row.get('last_api_fetch'), # YYYY-MM-DD string
            "last_updated_in_db": db_row.get('last_updated') # YYYY-MM-DD string
        },
        "risk_scores": {
            "year_1": risk_scores.get("year_1_risk"),
            "year_5": risk_scores.get("year_5_risk"),
        },
        "risk_category": risk_scores.get("risk_category"),
        "risk_factors": risk_scores.get("risk_factors"),
        "protective_factors": risk_scores.get("protective_factors"),
        "analysis": risk_scores.get("analysis"),
        "trend_data": {
            "years": trend_years,
            "employment": trend_employment
        },
        "source": "bls_database" # Indicate data came from local DB cache
    }

def calculate_ai_risk_from_category(job_category: str, occupation_code: str) -> Dict[str, Any]:
    """Calculates AI displacement risk based on job category and specific SOC patterns."""
    # Default risk if no specific category matches
    risk_profile = {
        "year_1_risk": 20.0, "year_5_risk": 40.0, "risk_category": "Moderate",
        "risk_factors": ["General task automation", "AI-driven efficiency improvements"],
        "protective_factors": ["Complex problem-solving", "Human interaction and empathy"]
    }

    # More granular risk profiles based on SOC prefixes and keywords in category
    if job_category:
        cat_lower = job_category.lower()
        if "computer" in cat_lower or "mathematical" in cat_lower or occupation_code.startswith("15-"):
            risk_profile = {"year_1_risk": 25.0, "year_5_risk": 45.0, "risk_category": "Moderate",
                            "risk_factors": ["Routine coding automation", "AI-assisted data analysis", "Automated testing"],
                            "protective_factors": ["Complex system architecture", "Novel algorithm design", "Cybersecurity expertise"]}
        elif "healthcare practitioners" in cat_lower or occupation_code.startswith("29-"):
            risk_profile = {"year_1_risk": 10.0, "year_5_risk": 25.0, "risk_category": "Low",
                            "risk_factors": ["AI diagnostic assistance", "Automated record keeping", "Robotic surgery assistance"],
                            "protective_factors": ["Direct patient care & empathy", "Complex clinical decision-making", "Ethical medical judgments"]}
        elif "education" in cat_lower or occupation_code.startswith("25-"):
            risk_profile = {"year_1_risk": 15.0, "year_5_risk": 30.0, "risk_category": "Low",
                            "risk_factors": ["AI tutoring systems", "Automated grading", "Online content delivery"],
                            "protective_factors": ["In-person mentorship", "Social-emotional development", "Curriculum innovation"]}
        elif "administrative support" in cat_lower or occupation_code.startswith("43-"):
            risk_profile = {"year_1_risk": 50.0, "year_5_risk": 75.0, "risk_category": "Very High",
                            "risk_factors": ["Data entry automation", "AI scheduling assistants", "Automated customer correspondence"],
                            "protective_factors": ["Complex office management", "Handling sensitive interpersonal issues", "Executive-level support"]}
        elif "transportation" in cat_lower or occupation_code.startswith("53-"):
            risk_profile = {"year_1_risk": 40.0, "year_5_risk": 70.0, "risk_category": "High",
                            "risk_factors": ["Autonomous driving technology", "Drone delivery systems", "AI logistics optimization"],
                            "protective_factors": ["Handling unexpected road conditions", "Last-mile delivery complexities", "Passenger interaction (for some roles)"]}
        elif "production" in cat_lower or occupation_code.startswith("51-"):
             risk_profile = {"year_1_risk": 45.0, "year_5_risk": 70.0, "risk_category": "High",
                            "risk_factors": ["Robotics in assembly lines", "Automated quality control", "AI-driven process optimization"],
                            "protective_factors": ["Complex machinery maintenance", "Custom fabrication", "Supervising automated systems"]}
        elif "sales" in cat_lower or occupation_code.startswith("41-"):
             risk_profile = {"year_1_risk": 35.0, "year_5_risk": 60.0, "risk_category": "High",
                            "risk_factors": ["E-commerce and online sales platforms", "AI-powered recommendation engines", "Automated customer outreach"],
                            "protective_factors": ["Complex B2B sales negotiations", "Building long-term client relationships", "High-value consultative selling"]}


    # Basic analysis text
    risk_profile["analysis"] = f"The role, categorized under '{job_category}', faces a {risk_profile['risk_category'].lower()} risk. Key factors include {', '.join(risk_profile['risk_factors'][:2])}, while protective elements involve {', '.join(risk_profile['protective_factors'][:2])}."
    return risk_profile

def generate_employment_trend(current_emp: Optional[int], projected_emp: Optional[int], num_years: int) -> List[int]:
    """Generates a list of employment numbers for a trend line."""
    if current_emp is None or projected_emp is None or num_years <= 1:
        return [val for val in [current_emp, projected_emp] if val is not None]

    # Ensure numeric types for calculation
    try:
        current = int(current_emp)
        projected = int(projected_emp)
    except (ValueError, TypeError):
        logger.warning(f"Invalid employment numbers for trend generation: current='{current_emp}', projected='{projected_emp}'")
        return []

    # Linear interpolation
    trend = [int(current + (projected - current) * i / (num_years - 1)) for i in range(num_years)]
    return trend

def get_all_soc_codes_from_db(engine: sqlalchemy.engine.Engine) -> List[Tuple[str, str]]:
    """Retrieves all unique SOC codes and their primary titles from the database."""
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT occupation_code, standardized_title FROM bls_job_data ORDER BY occupation_code")
            result = conn.execute(query)
            return [(row[0], row[1]) for row in result.fetchall()]
    except SQLAlchemyError as e:
        logger.error(f"Error fetching all SOC codes from database: {e}", exc_info=True)
        return []

def get_job_titles_for_autocomplete(engine: sqlalchemy.engine.Engine) -> List[Dict[str, Any]]:
    """
    Retrieves job titles for autocomplete, prioritizing standardized titles.
    """
    try:
        with engine.connect() as conn:
            # Fetch standardized titles first, then other job titles
            query = text("""
                SELECT standardized_title AS title, occupation_code, TRUE as is_primary
                FROM bls_job_data
                GROUP BY standardized_title, occupation_code
                UNION ALL
                SELECT job_title AS title, occupation_code, FALSE as is_primary
                FROM bls_job_data
                WHERE job_title != standardized_title
                ORDER BY title
            """)
            result = conn.execute(query)
            # Use a set to ensure unique titles if a job_title happens to be a standardized_title of another SOC
            seen_titles = set()
            job_titles = []
            for row in result:
                if row.title.lower() not in seen_titles:
                    job_titles.append({"title": row.title, "soc_code": row.occupation_code, "is_primary": row.is_primary})
                    seen_titles.add(row.title.lower())
            return job_titles
    except SQLAlchemyError as e:
        logger.error(f"Error fetching job titles for autocomplete: {e}", exc_info=True)
        return []

if __name__ == "__main__":
    # Example usage (requires DATABASE_URL to be set)
    logging.basicConfig(level=logging.INFO)
    logger.setLevel(logging.DEBUG) # Enable debug for this test
    try:
        test_engine = get_db_engine()
        logger.info("Database engine obtained.")

        # Test find_soc_code_and_title
        soc_code, std_title, category = find_soc_code_and_title("Software Developer", test_engine)
        logger.info(f"Found for 'Software Developer': SOC={soc_code}, Title='{std_title}', Category='{category}'")

        soc_code_cook, std_title_cook, category_cook = find_soc_code_and_title("Cook", test_engine)
        logger.info(f"Found for 'Cook': SOC={soc_code_cook}, Title='{std_title_cook}', Category='{category_cook}'")


        # Test fetch_and_process_soc_data (will call BLS API if not fresh in DB)
        logger.info("\nTesting fetch_and_process_soc_data for 'Registered Nurse' (SOC: 29-1141)")
        rn_data = fetch_and_process_soc_data("29-1141", "Registered Nurse", test_engine, force_api_fetch=False)
        if rn_data:
            logger.info(f"Registered Nurse Data: Title='{rn_data.get('job_title')}', Risk='{rn_data.get('risk_category')}', Employment='{rn_data.get('bls_data', {}).get('employment')}'")
            logger.debug(f"Full RN Data: {json.dumps(rn_data, indent=2)}")
        else:
            logger.error("Failed to get data for Registered Nurse.")

        logger.info("\nTesting fetch_and_process_soc_data for 'Data Entry Keyer' (SOC: 43-9021) with forced API fetch")
        de_data = fetch_and_process_soc_data("43-9021", "Data Entry Keyer", test_engine, force_api_fetch=True)
        if de_data:
            logger.info(f"Data Entry Keyer Data: Title='{de_data.get('job_title')}', Risk='{de_data.get('risk_category')}', Employment='{de_data.get('bls_data', {}).get('employment')}'")
        else:
            logger.error("Failed to get data for Data Entry Keyer.")

        # Test get_bls_data_from_db directly
        logger.info("\nTesting get_bls_data_from_db for SOC '15-1252'")
        db_sw_dev_data = get_bls_data_from_db("15-1252", test_engine)
        if db_sw_dev_data:
            logger.info(f"DB Data for 15-1252 (Software Developers): Fetched on {db_sw_dev_data.get('last_api_fetch')}, Updated in DB on {db_sw_dev_data.get('last_updated')}")
            formatted_for_app = format_database_row_to_app_schema(db_sw_dev_data)
            logger.info(f"Formatted App Data: Risk Category='{formatted_for_app.get('risk_category')}', 5-Year Risk='{formatted_for_app.get('risk_scores',{}).get('year_5')}%'")

        else:
            logger.info("No fresh data for 15-1252 in DB, would require API call in app.")

        logger.info("\nTesting get_all_soc_codes_from_db")
        all_codes = get_all_soc_codes_from_db(test_engine)
        logger.info(f"Found {len(all_codes)} unique SOC codes in DB. First 5: {all_codes[:5]}")

        logger.info("\nTesting get_job_titles_for_autocomplete")
        autocomplete_titles = get_job_titles_for_autocomplete(test_engine)
        logger.info(f"Found {len(autocomplete_titles)} titles for autocomplete. First 5: {autocomplete_titles[:5]}")


    except ValueError as ve:
        logger.critical(f"Test script failed due to configuration error: {ve}")
    except SQLAlchemyError as se:
        logger.critical(f"Test script failed due to database error: {se}", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected error occurred in the test script: {e}", exc_info=True)

