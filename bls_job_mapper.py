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
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, MetaData, inspect, Text, TIMESTAMP
from sqlalchemy.exc import SQLAlchemyError

# Attempt to import the custom BLS API connector
try:
    import bls_connector
except ImportError:
    logging.critical("bls_connector.py not found. This module is essential for fetching BLS data.")
    # Define a stub if bls_connector is missing so the application can at least report this critical error.
    class bls_connector_stub:
        @staticmethod
        def get_occupation_data(*args, **kwargs) -> Dict[str, Any]:
            return {"status": "error", "message": "bls_connector module not found."}
        @staticmethod
        def get_bls_data(*args, **kwargs) -> Dict[str, Any]: # Added for projection attempts
            return {"status": "error", "message": "bls_connector module not found."}
        @staticmethod
        def search_occupations(*args, **kwargs) -> List[Dict[str, str]]:
            return []
    bls_connector = bls_connector_stub() # type: ignore

# Configure logging
logger = logging.getLogger(__name__)
if not logger.handlers: # Ensure logger is configured more robustly
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        try:
            import streamlit as st
            database_url = st.secrets.get("database", {}).get("DATABASE_URL")
        except (ImportError, AttributeError):
            pass

    if not database_url:
        logger.error("DATABASE_URL environment variable or secret not set. Cannot connect to database.")
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
        engine = create_engine(database_url, connect_args=connect_args, pool_pre_ping=True, pool_recycle=1800)
        metadata.create_all(engine, checkfirst=True)
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}", exc_info=True)
        raise

# --- Constants and Mappings ---
JOB_TITLE_TO_SOC: Dict[str, str] = {
    "software developer": "15-1252", "software engineer": "15-1252", "programmer": "15-1251",
    "web developer": "15-1254", "registered nurse": "29-1141", "nurse": "29-1141",
    "teacher": "25-2021", "elementary school teacher": "25-2021", "high school teacher": "25-2031",
    "lawyer": "23-1011", "attorney": "23-1011", "doctor": "29-1221", "physician": "29-1221",
    "accountant": "13-2011", "project manager": "13-1199",
    "product manager": "11-2021", "marketing manager": "11-2021", "retail salesperson": "41-2031",
    "cashier": "41-2011", "customer service representative": "43-4051", "truck driver": "53-3032",
    "receptionist": "43-4171", "data scientist": "15-2051", "data analyst": "15-2051",
    "business analyst": "13-1111", "financial analyst": "13-2051", "human resources specialist": "13-1071",
    "graphic designer": "27-1024", "police officer": "33-3051", "chef": "35-1011", "cook": "35-2014",
    "waiter": "35-3031", "waitress": "35-3031", "janitor": "37-2011",
    "administrative assistant": "43-6011", "executive assistant": "43-6011",
    "dental hygienist": "29-1292", "electrician": "47-2111", "plumber": "47-2152",
    "carpenter": "47-2031", "construction worker": "47-2061", "mechanic": "49-3023",
    "automotive mechanic": "49-3023", "taxi driver": "53-3054", "uber driver": "53-3054",
    "journalist": "27-3023", "reporter": "27-3023", "writer": "27-3042", "editor": "27-3041",
    "photographer": "27-4021", "court reporter": "23-2011", "stenographer": "23-2011",
    "digital court reporter": "23-2011", "travel agent": "41-3041"
}

TARGET_SOC_CODES = [
    {"soc_code": "15-1252", "title": "Software Developers"},
    {"soc_code": "15-1251", "title": "Computer Programmers"},
    {"soc_code": "15-1254", "title": "Web Developers"},
    {"soc_code": "29-1141", "title": "Registered Nurses"},
    {"soc_code": "25-2021", "title": "Elementary School Teachers, Except Special Education"},
    {"soc_code": "25-2031", "title": "Secondary School Teachers, Except Special and Career/Technical Education"},
    {"soc_code": "23-1011", "title": "Lawyers"},
    {"soc_code": "29-1221", "title": "Physicians, All Other"},
    {"soc_code": "13-2011", "title": "Accountants and Auditors"},
    {"soc_code": "13-1199", "title": "Business Operations Specialists, All Other"},
    {"soc_code": "11-2021", "title": "Marketing Managers"},
    {"soc_code": "41-2031", "title": "Retail Salespersons"},
    {"soc_code": "41-2011", "title": "Cashiers"},
    {"soc_code": "43-4051", "title": "Customer Service Representatives"},
    {"soc_code": "53-3032", "title": "Heavy and Tractor-Trailer Truck Drivers"},
    {"soc_code": "43-4171", "title": "Receptionists"},
    {"soc_code": "15-2051", "title": "Data Scientists"},
    {"soc_code": "13-1111", "title": "Management Analysts"},
    {"soc_code": "13-2051", "title": "Financial Analysts"},
    {"soc_code": "13-1071", "title": "Human Resources Specialists"},
    {"soc_code": "27-1024", "title": "Graphic Designers"},
    {"soc_code": "33-3051", "title": "Police and Sheriff's Patrol Officers"},
    {"soc_code": "35-1011", "title": "Chefs and Head Cooks"},
    {"soc_code": "35-2014", "title": "Cooks, Restaurant"},
    {"soc_code": "35-3031", "title": "Waiters and Waitresses"},
    {"soc_code": "37-2011", "title": "Janitors and Cleaners, Except Maids and Housekeeping Cleaners"},
    {"soc_code": "43-6011", "title": "Secretaries and Administrative Assistants"},
    {"soc_code": "29-1292", "title": "Dental Hygienists"},
    {"soc_code": "47-2111", "title": "Electricians"},
    {"soc_code": "47-2152", "title": "Plumbers, Pipefitters, and Steamfitters"},
    {"soc_code": "47-2031", "title": "Carpenters"},
    {"soc_code": "49-3023", "title": "Automotive Service Technicians and Mechanics"},
    {"soc_code": "53-3054", "title": "Taxi Drivers"},
    {"soc_code": "27-3023", "title": "News Analysts, Reporters, and Journalists"},
    {"soc_code": "27-3042", "title": "Technical Writers"},
    {"soc_code": "27-3041", "title": "Editors"},
    {"soc_code": "27-4021", "title": "Photographers"},
    {"soc_code": "23-2011", "title": "Paralegals and Legal Assistants"},
    {"soc_code": "41-3041", "title": "Travel Agents"},
    {"soc_code": "17-2071", "title": "Electrical Engineers"},
    {"soc_code": "17-2141", "title": "Mechanical Engineers"},
    {"soc_code": "17-2051", "title": "Civil Engineers"},
    {"soc_code": "17-2199", "title": "Engineers, All Other"},
    {"soc_code": "15-1299", "title": "Computer Occupations, All Other"},
    {"soc_code": "11-9199", "title": "Managers, All Other"}
]

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

# --- Helper Functions ---
def standardize_job_title(title: str) -> str:
    """Standardize job title format for consistent mapping."""
    std_title = title.lower().strip()
    common_suffixes = [" i", " ii", " iii", " iv", " v", " specialist", " assistant", " associate", " senior", " junior", " lead"]
    for suffix in common_suffixes:
        if std_title.endswith(suffix):
            std_title = std_title[:-len(suffix)].strip()
            break
    return std_title

def get_job_category(occupation_code: str) -> str:
    """Get the job category based on SOC code prefix."""
    for prefix, category in SOC_TO_CATEGORY.items():
        if occupation_code.startswith(prefix):
            return category
    return "General Occupations"

def find_occupation_code(job_title: str) -> Tuple[Optional[str], str, str]:
    """Find SOC occupation code for a job title, using local map and BLS API search."""
    std_title_input = standardize_job_title(job_title)
    if std_title_input in JOB_TITLE_TO_SOC:
        soc_code = JOB_TITLE_TO_SOC[std_title_input]
        category = get_job_category(soc_code)
        # Try to get a more official title for this SOC code if possible
        official_title_entry = next((item["title"] for item in TARGET_SOC_CODES if item["soc_code"] == soc_code), job_title)
        logger.info(f"Found SOC '{soc_code}' for '{job_title}' via hardcoded map. Official: '{official_title_entry}'. Category: {category}")
        return soc_code, official_title_entry, category

    logger.info(f"No direct map for '{job_title}'. Querying BLS API for SOC code.")
    matches = bls_connector.search_occupations(job_title)
    if matches:
        best_match = matches[0]
        soc_code = best_match["code"]
        official_title = best_match["title"]
        category = get_job_category(soc_code)
        JOB_TITLE_TO_SOC[std_title_input] = soc_code # Cache for this session
        logger.info(f"Found SOC '{soc_code}' for '{job_title}' via BLS API. Official: '{official_title}'. Category: {category}")
        return soc_code, official_title, category

    logger.warning(f"Could not find SOC code for '{job_title}' via API. Returning generic.")
    return None, job_title, "General Occupations"

# --- Database Interaction ---
def get_bls_data_from_db(occupation_code: str, engine) -> Optional[Dict[str, Any]]:
    """Get BLS data from database if available and fresh."""
    if not engine: return None
    try:
        with engine.connect() as conn:
            stmt = bls_job_data_table.select().where(bls_job_data_table.c.occupation_code == occupation_code)
            result = conn.execute(stmt).fetchone()
            if result:
                data = dict(result._mapping) # type: ignore
                last_fetch_str = data.get('last_api_fetch')
                if last_fetch_str:
                    # Ensure last_fetch_str is a datetime object or a valid ISO string before parsing
                    if isinstance(last_fetch_str, str):
                        last_api_fetch = datetime.datetime.fromisoformat(last_fetch_str.replace('Z', '+00:00'))
                    elif isinstance(last_fetch_str, datetime.datetime):
                        last_api_fetch = last_fetch_str
                    else:
                        logger.warning(f"last_api_fetch for SOC {occupation_code} is not a valid datetime string or object: {last_fetch_str}")
                        return None # Treat as stale or invalid

                    # Ensure last_api_fetch is timezone-aware for comparison
                    if last_api_fetch.tzinfo is None:
                        last_api_fetch = last_api_fetch.replace(tzinfo=datetime.timezone.utc)

                    if (datetime.datetime.now(datetime.timezone.utc) - last_api_fetch).days < 30: # Cache for 30 days
                        logger.info(f"Found fresh cached data in DB for SOC {occupation_code}")
                        return data
                    else:
                        logger.info(f"Cached data for SOC {occupation_code} is stale (older than 30 days).")
                else:
                    logger.warning(f"last_api_fetch timestamp missing for SOC {occupation_code} in DB.")
    except SQLAlchemyError as e:
        logger.error(f"DB error fetching data for SOC {occupation_code}: {e}", exc_info=True)
    except ValueError as e: # Catches fromisoformat errors
        logger.error(f"Error parsing timestamp for SOC {occupation_code}: {e}", exc_info=True)
    return None

def save_bls_data_to_db(data_to_save: Dict[str, Any], engine) -> bool:
    """Save or update BLS data in the database."""
    if not engine: return False
    soc_code = data_to_save.get("occupation_code")
    logger.info(f"Attempting to save/update SOC {soc_code}.")
    logger.info(f"Keys in data_to_save for SOC {soc_code}: {list(data_to_save.keys())}")

    try:
        with engine.connect() as conn:
            # Check existing columns in the database for debugging
            inspector = inspect(engine)
            db_columns = [col['name'] for col in inspector.get_columns('bls_job_data')]
            logger.info(f"Perceived columns in 'bls_job_data' from DB for SOC {soc_code}: {db_columns}")

            # Ensure all keys in data_to_save exist as columns in bls_job_data_table.c
            # This is a defensive check; the actual insert/update will use the table definition.
            table_column_names = {col.name for col in bls_job_data_table.c}
            for key in data_to_save.keys():
                if key not in table_column_names:
                    logger.warning(f"Key '{key}' from data_to_save is not in bls_job_data_table definition. Skipping this key.")
            
            # Filter data_to_save to only include keys that are actual columns
            filtered_data_to_save = {k: v for k, v in data_to_save.items() if k in table_column_names}


            # Check if record exists
            select_stmt = text("SELECT id FROM bls_job_data WHERE occupation_code = :code")
            existing_id = conn.execute(select_stmt, {"code": soc_code}).scalar_one_or_none()

            if existing_id:
                logger.info(f"Updating existing BLS data in DB for SOC {soc_code}.")
                stmt = bls_job_data_table.update().where(bls_job_data_table.c.occupation_code == soc_code).values(**filtered_data_to_save)
            else:
                logger.info(f"Inserting new BLS data into DB for SOC {soc_code}.")
                stmt = bls_job_data_table.insert().values(**filtered_data_to_save)
            
            conn.execute(stmt)
            conn.commit()
            logger.info(f"Successfully saved/updated data for SOC {soc_code} in DB.")
            return True
    except SQLAlchemyError as e:
        logger.error(f"DB error saving data for SOC {soc_code}: {e}", exc_info=True)
        if conn: # type: ignore
            conn.rollback() # type: ignore
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error saving data for SOC {soc_code}: {e}", exc_info=True)
        if conn: # type: ignore
            conn.rollback() # type: ignore
    return False


# --- Main Data Fetching Logic ---
def fetch_and_process_soc_data(soc_code_info: Dict[str, str], engine, original_job_title_search: str) -> Dict[str, Any]:
    """Fetches, processes, and stores data for a single SOC code."""
    soc_code = soc_code_info["soc_code"]
    representative_title = soc_code_info["title"] # This is the official/common title for the SOC

    # Try to get data from DB cache first
    cached_data = get_bls_data_from_db(soc_code, engine)
    if cached_data:
        return cached_data # Already formatted for app use

    logger.info(f"No fresh cache for SOC {soc_code}. Fetching from BLS API.")
    
    # Fetch OES (Occupational Employment and Wage Statistics) data
    oes_data_raw = bls_connector.get_occupation_data(soc_code) # This now returns a dict
    
    # Fetch EP (Employment Projections) data
    # Determine current and projection years dynamically
    current_api_year = datetime.datetime.now().year -1 # BLS data is usually one year behind
    projection_end_year = current_api_year + 10
    
    ep_data_raw = bls_connector.get_bls_data(
        series_ids=bls_connector.construct_ep_series_ids(soc_code),
        start_year=str(current_api_year), # Use current year for base
        end_year=str(projection_end_year)   # Project 10 years out
    )
    logger.info(f"EP API call for SOC {soc_code} succeeded (may still have no data for specific series).")

    # Parse and combine data
    oes_parsed = bls_connector.parse_oes_series_response(oes_data_raw, soc_code)
    ep_parsed = bls_connector.parse_ep_series_response(ep_data_raw, soc_code)

    job_category = get_job_category(soc_code)

    data_to_save = {
        "median_wage": oes_parsed.get("annual_median_wage"),
        "mean_wage": oes_parsed.get("annual_mean_wage"),
        "current_employment": oes_parsed.get("employment"), # OES employment is current
        "raw_oes_data_json": json.dumps(oes_data_raw) if oes_data_raw else None,
        "projected_employment": ep_parsed.get("projected_employment"),
        "employment_change_numeric": ep_parsed.get("employment_change_numeric"),
        "percent_change": ep_parsed.get("percent_change"),
        "annual_job_openings": ep_parsed.get("annual_job_openings"),
        "ep_base_year": ep_parsed.get("base_year"),
        "ep_proj_year": ep_parsed.get("projection_year"),
        "raw_ep_data_json": json.dumps(ep_data_raw) if ep_data_raw else None,
        "occupation_code": soc_code,
        "job_title": original_job_title_search, # Store the original search term
        "standardized_title": representative_title, # Store the official/common title for the SOC
        "job_category": job_category,
        "last_api_fetch": datetime.datetime.now(datetime.timezone.utc), # Use datetime object
        "last_updated_in_db": datetime.datetime.now(datetime.timezone.utc) # Use datetime object
    }
    
    # Add OES data year if available
    if oes_parsed.get("data_year"):
        data_to_save["oes_data_year"] = oes_parsed["data_year"]

    if save_bls_data_to_db(data_to_save, engine):
        logger.info(f"Successfully fetched and saved data for SOC {soc_code}.")
        # Return the saved data (which is now also the cached data)
        return get_bls_data_from_db(soc_code, engine) or data_to_save # Fallback to data_to_save if DB read fails immediately
    else:
        logger.error(f"Failed to save fetched BLS data for SOC {soc_code} to DB.")
        # Return the fetched data even if DB save failed, but mark it as not from DB
        data_to_save["source"] = "bls_api_fetch_error_or_db_save_failed" 
        return data_to_save

def format_job_data_for_app(db_data: Dict[str, Any], original_job_title: str) -> Dict[str, Any]:
    """Formats data from DB (or direct fetch) into the structure expected by the Streamlit app."""
    job_category = db_data.get("job_category", "General Occupations")
    occupation_code = db_data.get("occupation_code", "00-0000")
    
    # Calculate AI risk based on job category
    risk_data = calculate_ai_risk_from_category(job_category, occupation_code) # Pass SOC for potential future use
    
    # Generate employment trend from current to projected
    current_emp = db_data.get("current_employment")
    projected_emp = db_data.get("projected_employment")
    ep_base_year_str = db_data.get("ep_base_year")
    ep_proj_year_str = db_data.get("ep_proj_year")

    trend_years = []
    trend_employment = []

    if current_emp is not None and projected_emp is not None and ep_base_year_str and ep_proj_year_str:
        try:
            base_year = int(ep_base_year_str)
            proj_year = int(ep_proj_year_str)
            if proj_year > base_year:
                num_projection_years = proj_year - base_year
                trend_years = list(range(base_year, proj_year + 1))
                # Linear interpolation for trend
                annual_change = (projected_emp - current_emp) / num_projection_years
                trend_employment = [int(current_emp + i * annual_change) for i in range(num_projection_years + 1)]
            else: # If years are same or invalid, just use current
                 trend_years = [base_year] if base_year else []
                 trend_employment = [current_emp] if current_emp else []
        except ValueError: # Handle case where years are not valid integers
            logger.warning(f"Invalid year format for SOC {occupation_code}: base='{ep_base_year_str}', proj='{ep_proj_year_str}'")
            trend_employment = []
            trend_years = []
    elif current_emp is not None: # Only current employment available
        trend_years = [int(db_data.get("oes_data_year", datetime.datetime.now().year -1 ))] if db_data.get("oes_data_year") else []
        trend_employment = [current_emp]
        
    formatted_data = {
        "job_title": db_data.get("standardized_title", original_job_title),
        "occupation_code": occupation_code,
        "job_category": job_category,
        "source": db_data.get("source", "bls_database_cache"), # Indicate source
        
        "bls_data": { # Nest BLS specific fields for clarity
            "current_employment": current_emp,
            "projected_employment": projected_emp,
            "employment_change_numeric": db_data.get("employment_change_numeric"),
            "employment_change_percent": db_data.get("percent_change"),
            "annual_job_openings": db_data.get("annual_job_openings"),
            "median_wage": db_data.get("median_wage"),
            "mean_wage": db_data.get("mean_wage"),
            "oes_data_year": db_data.get("oes_data_year"),
            "ep_base_year": ep_base_year_str,
            "ep_proj_year": ep_proj_year_str,
            "raw_oes_data_json": db_data.get("raw_oes_data_json"),
            "raw_ep_data_json": db_data.get("raw_ep_data_json")
        },
        
        "risk_scores": { # Nest risk scores
            "year_1": risk_data["year_1_risk"],
            "year_5": risk_data["year_5_risk"]
        },
        "risk_category": risk_data["risk_category"],
        "risk_factors": risk_data["risk_factors"],
        "protective_factors": risk_data["protective_factors"],
        "analysis": risk_data["analysis"],
        
        "trend_data": {
            "years": trend_years,
            "employment": trend_employment
        },
        "last_api_fetch": db_data.get("last_api_fetch").isoformat() if isinstance(db_data.get("last_api_fetch"), datetime.datetime) else db_data.get("last_api_fetch"),
        "last_updated_in_db": db_data.get("last_updated_in_db").isoformat() if isinstance(db_data.get("last_updated_in_db"), datetime.datetime) else db_data.get("last_updated_in_db")
    }
    return formatted_data

# --- Risk Calculation (Simplified - to be expanded with LLM/NLP later) ---
def calculate_ai_risk_from_category(job_category: str, occupation_code: str) -> Dict[str, Any]:
    """Calculate AI displacement risk based on job category and specific SOC if needed."""
    # Base risk scores by category (1-year and 5-year risks)
    # These are illustrative and should be refined with real research.
    category_risk_map = {
        "Computer and Mathematical Occupations": (20.0, 45.0, ["Routine coding tasks", "Data processing", "Automated testing"], ["Complex problem-solving", "System architecture", "AI/ML development"]),
        "Management Occupations": (10.0, 25.0, ["Administrative tasks", "Basic reporting", "Scheduling"], ["Strategic decision-making", "Leadership", "Complex negotiation"]),
        "Business and Financial Operations Occupations": (25.0, 50.0, ["Data entry", "Standard financial analysis", "Report generation"], ["Strategic financial planning", "Client advisory", "Regulatory compliance"]),
        "Architecture and Engineering Occupations": (15.0, 35.0, ["Drafting and modeling", "Routine calculations", "Component design"], ["Innovative design", "Complex project management", "System integration"]),
        "Life, Physical, and Social Science Occupations": (10.0, 25.0, ["Lab data collection", "Literature reviews", "Statistical analysis"], ["Experimental design", "Interpretation of complex data", "Novel research"]),
        "Community and Social Service Occupations": (5.0, 15.0, ["Record keeping", "Information dissemination"], ["Empathy and counseling", "Crisis intervention", "Community outreach"]),
        "Legal Occupations": (30.0, 55.0, ["Document review", "Legal research", "Case file management"], ["Litigation strategy", "Client representation", "Complex negotiation"]),
        "Educational Instruction and Library Occupations": (10.0, 25.0, ["Grading standardized tests", "Content delivery (basic)", "Administrative tasks"], ["Curriculum development", "Student mentorship", "Adaptive teaching"]),
        "Arts, Design, Entertainment, Sports, and Media Occupations": (25.0, 50.0, ["Basic graphic design", "Content generation (simple)", "Media scheduling"], ["Original creative concepts", "Performance art", "Strategic brand development"]),
        "Healthcare Practitioners and Technical Occupations": (10.0, 20.0, ["Medical record updates", "Preliminary diagnostic support", "Scheduling"], ["Complex diagnosis", "Surgical procedures", "Patient interaction and empathy"]),
        "Healthcare Support Occupations": (20.0, 40.0, ["Patient data entry", "Appointment scheduling", "Medical supply management"], ["Direct patient assistance", "Emotional support", "Specialized care tasks"]),
        "Protective Service Occupations": (5.0, 15.0, ["Surveillance monitoring", "Report filing", "Patrol scheduling"], ["Crisis response", "Investigation skills", "Community interaction"]),
        "Food Preparation and Serving Related Occupations": (40.0, 65.0, ["Order taking (simple)", "Basic food assembly", "Payment processing"], ["Culinary creativity", "Customer experience management", "Specialty cooking"]),
        "Building and Grounds Cleaning and Maintenance Occupations": (30.0, 50.0, ["Routine cleaning tasks", "Automated floor cleaning", "Waste disposal systems"], ["Specialized cleaning techniques", "Equipment maintenance", "Problem-solving for repairs"]),
        "Personal Care and Service Occupations": (15.0, 30.0, ["Appointment booking", "Basic grooming tasks (automated tools)", "Information provision"], ["Personalized consultations", "Complex hairstyling/treatments", "Empathy and client relations"]),
        "Sales and Related Occupations": (35.0, 60.0, ["Online order processing", "Basic product information", "Inventory tracking"], ["Complex sales negotiations", "Relationship building", "Strategic account management"]),
        "Office and Administrative Support Occupations": (50.0, 75.0, ["Data entry and typing", "Scheduling meetings", "Document filing and retrieval"], ["Complex office management", "Executive support with high discretion", "Problem-solving for logistical issues"]),
        "Farming, Fishing, and Forestry Occupations": (25.0, 45.0, ["Automated harvesting (some crops)", "GPS-guided machinery operation", "Environmental monitoring"], ["Specialized crop/animal husbandry", "Ecosystem management", "Decision-making based on variable conditions"]),
        "Construction and Extraction Occupations": (20.0, 40.0, ["Repetitive assembly tasks", "Automated bricklaying/welding (emerging)", "Site surveying with drones"], ["Skilled trades (plumbing, electrical)", "Project oversight and problem-solving", "Operating complex machinery in variable environments"]),
        "Installation, Maintenance, and Repair Occupations": (25.0, 45.0, ["Diagnostic checks (automated)", "Routine maintenance scheduling", "Parts ordering"], ["Complex troubleshooting and repair", "Customer interaction and explanation", "Adapting to novel equipment issues"]),
        "Production Occupations": (45.0, 70.0, ["Assembly line work", "Quality control inspection (visual AI)", "Material handling with robotics"], ["Overseeing automated systems", "Complex machine setup and maintenance", "Adapting production processes"]),
        "Transportation and Material Moving Occupations": (40.0, 70.0, ["Long-haul truck driving (autonomous)", "Warehouse picking and packing (robotics)", "Route optimization"], ["Last-mile delivery in complex urban areas", "Handling of specialized/hazardous materials", "Customer interaction during delivery"]),
        "General Occupations": (30.0, 50.0, ["Generic routine tasks", "Basic information processing"], ["Adaptability", "Human interaction", "Unstructured problem solving"])
    }
    
    year_1_risk, year_5_risk, risk_factors_list, protective_factors_list = category_risk_map.get(job_category, category_risk_map["General Occupations"])

    # Adjust risk based on specific SOC codes if needed (example)
    if occupation_code == "15-1252": # Software Developers
        year_5_risk = min(year_5_risk + 5, 95) # Slightly higher due to advanced coding AI
    elif occupation_code == "29-1141": # Registered Nurses
        year_5_risk = max(year_5_risk - 5, 5)  # Slightly lower due to high human interaction

    if year_5_risk < 30: risk_cat = "Low"
    elif year_5_risk < 50: risk_cat = "Moderate"
    elif year_5_risk < 70: risk_cat = "High"
    else: risk_cat = "Very High"

    analysis_text = f"The role of '{occupation_code}' in the {job_category} category faces a {risk_cat.lower()} risk of AI displacement over the next 5 years. Key drivers include {', '.join(risk_factors_list[:2])}. However, factors such as {', '.join(protective_factors_list[:2])} provide some resilience."

    return {
        "year_1_risk": round(year_1_risk, 1),
        "year_5_risk": round(year_5_risk, 1),
        "risk_category": risk_cat,
        "risk_factors": risk_factors_list,
        "protective_factors": protective_factors_list,
        "analysis": analysis_text
    }

# --- Public API ---
def get_job_data_from_db_or_api(job_title: str) -> Dict[str, Any]:
    """
    Main function to get job data.
    It tries DB cache first, then BLS API, then formats for app.
    This version is for the main application, ensuring data is always fetched if not cached.
    """
    engine = get_db_engine() # Ensure engine is initialized
    soc_code, standardized_title, job_category = find_occupation_code(job_title)

    if not soc_code or soc_code == "00-0000":
        logger.warning(f"Could not determine SOC code for '{job_title}'. Cannot fetch BLS data.")
        # Return a structure indicating data is unavailable, consistent with other error returns
        return {
            "error": f"Could not identify a Standard Occupational Classification (SOC) code for '{job_title}'. Please try a different or more specific job title.",
            "job_title": job_title,
            "source": "soc_lookup_failed"
        }

    # Try to get data from database first
    db_data = get_bls_data_from_db(soc_code, engine)
    if db_data:
        logger.info(f"Using cached BLS data for {standardized_title} (SOC: {soc_code}) from database.")
        return format_job_data_for_app(db_data, job_title) # Pass original title for context

    # If not in DB or stale, fetch from BLS API and store
    logger.info(f"No fresh cache for SOC {soc_code}. Fetching from BLS API for job: '{job_title}' (maps to: '{standardized_title}').")
    
    # This function now handles fetching from API AND saving to DB.
    # It's designed to be called when data is needed and not found/stale in cache.
    # The original_job_title_search is passed to ensure the job_title column in DB reflects the user's search
    # if this is the first time this SOC code's data is being populated due to this specific search term.
    # If the SOC code already exists, its job_title field (original search term) might be updated if we decide so,
    # or we might keep the first search term that led to its creation.
    # For now, fetch_and_process_soc_data will use original_job_title_search when inserting a new SOC.
    
    # Create the soc_code_info dict as expected by fetch_and_process_soc_data
    soc_code_info = {"soc_code": soc_code, "title": standardized_title}
    
    fetched_and_saved_data = fetch_and_process_soc_data(soc_code_info, engine, job_title)

    if "error" in fetched_and_saved_data:
        logger.error(f"Error fetching or saving data for {job_title} (SOC: {soc_code}): {fetched_and_saved_data['error']}")
        return fetched_and_saved_data # Return the error structure

    logger.info(f"Successfully fetched and stored data for {job_title} (SOC: {soc_code}).")
    return format_job_data_for_app(fetched_and_saved_data, job_title)

# --- Functions for Admin Tool ---
def get_all_soc_data_from_db(engine) -> List[Dict[str, Any]]:
    """Fetches all records from the bls_job_data table."""
    if not engine: return []
    try:
        with engine.connect() as conn:
            result = conn.execute(bls_job_data_table.select()).fetchall()
            return [dict(row._mapping) for row in result] # type: ignore
    except SQLAlchemyError as e:
        logger.error(f"Error fetching all SOC data from DB: {e}", exc_info=True)
        return []

def get_soc_codes_to_process(engine, target_soc_list: List[Dict[str,str]]) -> List[Dict[str,str]]:
    """
    Determines which SOC codes from the target list need to be processed.
    A SOC code needs processing if it's not in the DB or its data is stale.
    """
    if not engine: return target_soc_list # Process all if no DB

    processed_socs = []
    try:
        with engine.connect() as conn:
            for soc_info in target_soc_list:
                soc_code = soc_info["soc_code"]
                cached_data = get_bls_data_from_db(soc_code, engine) # Checks for freshness
                if not cached_data:
                    processed_socs.append(soc_info)
                else:
                    logger.info(f"SOC {soc_code} already has fresh data in DB. Skipping API fetch.")
        return processed_socs
    except SQLAlchemyError as e:
        logger.error(f"DB error checking SOCs to process: {e}. Will attempt to process all targets.", exc_info=True)
        return target_soc_list # Fallback to processing all if DB check fails

def get_all_job_titles_from_db(engine) -> List[Dict[str, Any]]:
    """Loads all distinct job titles and their primary SOC codes from the database."""
    if not engine:
        logger.error("Database engine not available for loading job titles.")
        return []
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT DISTINCT standardized_title as title, occupation_code as soc_code, true as is_primary
                FROM bls_job_data 
                WHERE standardized_title IS NOT NULL AND occupation_code IS NOT NULL
                ORDER BY standardized_title
            """)
            result = conn.execute(query)
            job_titles = [{"title": row.title, "soc_code": row.soc_code, "is_primary": row.is_primary} for row in result]
            logger.info(f"Loaded {len(job_titles)} distinct job titles from database.")
            return job_titles
    except SQLAlchemyError as e:
        logger.error(f"Error loading job titles from database: {e}", exc_info=True)
        return []

if __name__ == "__main__":
    # Example usage (requires DATABASE_URL and BLS_API_KEY to be set)
    # Note: This main block is for testing and won't run in Streamlit.
    logging.basicConfig(level=logging.INFO)
    logger.info("Running bls_job_mapper.py directly for testing.")
    
    test_engine = None
    try:
        test_engine = get_db_engine()
        logger.info("Database engine created successfully for testing.")
        
        # Test fetching data for a specific job
        job_to_test = "Software Developer"
        logger.info(f"\n--- Testing get_job_data_from_db_or_api for: {job_to_test} ---")
        data = get_job_data_from_db_or_api(job_to_test)
        if "error" in data:
            logger.error(f"Error for '{job_to_test}': {data['error']}")
        else:
            logger.info(f"Data for '{data.get('job_title')}':")
            logger.info(f"  SOC Code: {data.get('occupation_code')}")
            logger.info(f"  Category: {data.get('job_category')}")
            logger.info(f"  Employment: {data.get('bls_data', {}).get('current_employment')}")
            logger.info(f"  Median Wage: {data.get('bls_data', {}).get('median_wage')}")
            logger.info(f"  5-Year Risk: {data.get('risk_scores', {}).get('year_5')}% ({data.get('risk_category')})")
            logger.info(f"  Source: {data.get('source')}")
            logger.info(f"  Last API Fetch: {data.get('last_api_fetch')}")

        # Test fetching all SOC data (if any exists)
        logger.info("\n--- Testing get_all_soc_data_from_db ---")
        all_data = get_all_soc_data_from_db(test_engine)
        logger.info(f"Found {len(all_data)} records in bls_job_data table.")
        if all_data:
            logger.info(f"First record: {all_data[0]['occupation_code']} - {all_data[0]['standardized_title']}")

        # Test populating a specific SOC code (example)
        # This would typically be run by the admin tool.
        # test_soc_info = {"soc_code": "17-2071", "title": "Electrical Engineers"} # Example SOC
        # logger.info(f"\n--- Testing fetch_and_process_soc_data for: {test_soc_info['title']} ({test_soc_info['soc_code']}) ---")
        # fetched_data = fetch_and_process_soc_data(test_soc_info, test_engine, "Electrical Engineer Test Search")
        # if "error" in fetched_data:
        #     logger.error(f"Error processing {test_soc_info['soc_code']}: {fetched_data['error']}")
        # else:
        #     logger.info(f"Processed data for {test_soc_info['soc_code']}: {fetched_data.get('standardized_title')}, Risk: {fetched_data.get('risk_category')}")
        #     # Verify it's in the DB
        #     db_check = get_bls_data_from_db(test_soc_info['soc_code'], test_engine)
        #     if db_check:
        #         logger.info(f"Verified {test_soc_info['soc_code']} is now in DB and fresh.")
        #     else:
        #         logger.error(f"Failed to verify {test_soc_info['soc_code']} in DB after processing.")

    except ValueError as ve:
        logger.error(f"Configuration error: {ve}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during testing: {e}", exc_info=True)
    finally:
        if test_engine:
            test_engine.dispose()
            logger.info("Test database engine disposed.")

