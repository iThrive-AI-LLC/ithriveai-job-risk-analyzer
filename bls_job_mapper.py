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
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, MetaData, inspect
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
if not logger.handlers: # Ensure logger is configured
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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
    Column('employment_change_numeric', Integer, nullable=True), # Added for more detail
    Column('percent_change', Float, nullable=True),
    Column('annual_job_openings', Integer, nullable=True),
    Column('median_wage', Float, nullable=True), # OES Annual Median Wage
    Column('mean_wage', Float, nullable=True),   # OES Annual Mean Wage
    Column('oes_data_year', String(4), nullable=True), # Year of the OES data
    Column('ep_base_year', String(4), nullable=True),   # Base year for projections
    Column('ep_proj_year', String(4), nullable=True),   # Projection year for projections
    Column('raw_oes_data_json', Text, nullable=True), # Store raw OES API response
    Column('raw_ep_data_json', Text, nullable=True),  # Store raw EP API response
    Column('last_api_fetch', String(20), nullable=False), # ISO datetime of last successful API fetch
    Column('last_updated_in_db', String(20), nullable=False, default=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()) # ISO datetime of DB record update
)

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        try:
            # Try to get from Streamlit secrets if available
            import streamlit as st
            database_url = st.secrets.get("database", {}).get("DATABASE_URL")
        except (ImportError, AttributeError): # Streamlit not available or secrets not configured
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
        # Create table if it doesn't exist
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
    "accountant": "13-2011", "project manager": "13-1199", # Changed from 11-3021 to 13-1199 (Proj Mgt Specialists)
    "product manager": "11-2021", "marketing manager": "11-2021", "retail salesperson": "41-2031",
    "cashier": "41-2011", "customer service representative": "43-4051", "truck driver": "53-3032",
    "receptionist": "43-4171", "data scientist": "15-2051", "data analyst": "15-2051", # Grouped Data Analyst with Data Scientist
    "business analyst": "13-1111", "financial analyst": "13-2051", "human resources specialist": "13-1071",
    "graphic designer": "27-1024", "police officer": "33-3051", "chef": "35-1011", "cook": "35-2014",
    "waiter": "35-3031", "waitress": "35-3031", "janitor": "37-2011",
    "administrative assistant": "43-6011", "executive assistant": "43-6011",
    "dental hygienist": "29-1292", "electrician": "47-2111", "plumber": "47-2152",
    "carpenter": "47-2031", "construction worker": "47-2061", "mechanic": "49-3023",
    "automotive mechanic": "49-3023", "taxi driver": "53-3054", "uber driver": "53-3054", # Grouped
    "journalist": "27-3023", "reporter": "27-3023", "writer": "27-3042", "editor": "27-3041",
    "photographer": "27-4021", "court reporter": "23-2011", "stenographer": "23-2011",
    "digital court reporter": "23-2011", "travel agent": "41-3041" # Corrected SOC for Travel Agents
}

SOC_TO_CATEGORY: Dict[str, str] = {
    "11-": "Management Occupations", "13-": "Business and Financial Operations Occupations",
    "15-": "Computer and Mathematical Occupations", "17-": "Architecture and Engineering Occupations",
    "19-": "Life, Physical, and Social Science Occupations", "21-": "Community and Social Service Occupations",
    "23-": "Legal Occupations", "25-": "Educational Instruction and Library Occupations",
    "27-": "Arts, Design, Entertainment, Sports, and Media Occupations",
    "29-": "Healthcare Practitioners and Technical Occupations", "31-": "Healthcare Support Occupations",
    "33-": "Protective Service Occupations", "35-": "Food Preparation and Serving Related Occupations",
    "37-": "Building and Grounds Cleaning and Maintenance Occupations", "39-": "Personal Care and Service Occupations",
    "41-": "Sales and Related Occupations", "43-": "Office and Administrative Support Occupations",
    "45-": "Farming, Fishing, and Forestry Occupations", "47-": "Construction and Extraction Occupations",
    "49-": "Installation, Maintenance, and Repair Occupations", "51-": "Production Occupations",
    "53-": "Transportation and Material Moving Occupations", "00-": "Generic or Unclassified"
}

# Base risk scores (0-100) for categories - informed estimates
CATEGORY_BASE_RISK: Dict[str, int] = {
    "Management Occupations": 25, "Business and Financial Operations Occupations": 40,
    "Computer and Mathematical Occupations": 35, "Architecture and Engineering Occupations": 30,
    "Life, Physical, and Social Science Occupations": 20, "Community and Social Service Occupations": 20,
    "Legal Occupations": 35, "Educational Instruction and Library Occupations": 25,
    "Arts, Design, Entertainment, Sports, and Media Occupations": 40,
    "Healthcare Practitioners and Technical Occupations": 15, "Healthcare Support Occupations": 30,
    "Protective Service Occupations": 20, "Food Preparation and Serving Related Occupations": 60,
    "Building and Grounds Cleaning and Maintenance Occupations": 55, "Personal Care and Service Occupations": 45,
    "Sales and Related Occupations": 50, "Office and Administrative Support Occupations": 65,
    "Farming, Fishing, and Forestry Occupations": 50, "Construction and Extraction Occupations": 30,
    "Installation, Maintenance, and Repair Occupations": 35, "Production Occupations": 60,
    "Transportation and Material Moving Occupations": 55, "Generic or Unclassified": 45
}
DATA_STALENESS_THRESHOLD_DAYS = 90

# --- Helper Functions ---
def get_job_category(occupation_code: str) -> str:
    for prefix, category in SOC_TO_CATEGORY.items():
        if occupation_code.startswith(prefix):
            return category
    return SOC_TO_CATEGORY.get("00-", "Generic or Unclassified")

def standardize_job_title(title: str) -> str:
    return title.lower().strip()

# --- Core Functions ---
def find_occupation_code(job_title: str, engine) -> Tuple[Optional[str], str, str]:
    """Finds SOC code for a job title. Returns (SOC code, standardized title for SOC, job category)."""
    std_input_title = standardize_job_title(job_title)
    soc_code = JOB_TITLE_TO_SOC.get(std_input_title)
    official_title_for_soc = std_input_title # Default if no better one found

    if soc_code:
        category = get_job_category(soc_code)
        # Try to get a more official title from DB if this SOC is already cached
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT standardized_title FROM bls_job_data WHERE occupation_code = :soc LIMIT 1"), {"soc": soc_code}).fetchone()
                if res and res[0]:
                    official_title_for_soc = res[0]
        except SQLAlchemyError as e:
            logger.warning(f"DB error trying to get official title for {soc_code}: {e}")
        logger.info(f"Found SOC '{soc_code}' for '{std_input_title}' via hardcoded map. Official: '{official_title_for_soc}'. Category: {category}")
        return soc_code, official_title_for_soc, category

    # If not in hardcoded map, try searching DB cache by original or standardized title
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT occupation_code, standardized_title, job_category
                FROM bls_job_data
                WHERE LOWER(job_title) = :title OR LOWER(standardized_title) = :title
                LIMIT 1
            """)
            result = conn.execute(query, {"title": std_input_title}).fetchone()
            if result:
                soc_code, official_title_for_soc, category = result[0], result[1], result[2]
                logger.info(f"Found SOC '{soc_code}' for '{std_input_title}' via DB cache. Official: '{official_title_for_soc}'. Category: {category}")
                return soc_code, official_title_for_soc, category
    except SQLAlchemyError as e:
        logger.warning(f"DB error during find_occupation_code for '{std_input_title}': {e}")

    # If still not found, use bls_connector.search_occupations (placeholder in connector context)
    # In a real scenario, this would hit an API or a comprehensive local SOC list.
    logger.info(f"'{std_input_title}' not in map or cache, trying bls_connector.search_occupations.")
    matches = bls_connector.search_occupations(job_title) # Use original job_title for search
    if matches:
        best_match = matches[0] # Assuming first is best
        soc_code = best_match["code"]
        official_title_for_soc = best_match["title"] # This is the official title from search
        category = get_job_category(soc_code)
        logger.info(f"Found SOC '{soc_code}' for '{job_title}' via bls_connector.search. Official: '{official_title_for_soc}'. Category: {category}")
        # Optionally, cache this new mapping if desired (e.g., in a separate job_title_aliases table)
        return soc_code, official_title_for_soc, category

    logger.warning(f"Could not find SOC code for job title: '{job_title}'. Assigning generic code.")
    return "00-0000", job_title, get_job_category("00-0000")


def get_bls_data_from_db(occupation_code: str, engine) -> Optional[Dict[str, Any]]:
    """Retrieves cached BLS data from the database if recent."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM bls_job_data WHERE occupation_code = :code"),
                               {"code": occupation_code}).fetchone()
            if row:
                data = dict(row._mapping) # type: ignore
                last_fetch_str = data.get("last_api_fetch")
                if last_fetch_str:
                    last_fetch_date = datetime.datetime.fromisoformat(last_fetch_str)
                    if (datetime.datetime.now(datetime.timezone.utc) - last_fetch_date).days < DATA_STALENESS_THRESHOLD_DAYS:
                        logger.info(f"Found fresh cached BLS data in DB for SOC {occupation_code}.")
                        # Deserialize JSON fields
                        data['raw_oes_data_json'] = json.loads(data['raw_oes_data_json']) if data.get('raw_oes_data_json') else None
                        data['raw_ep_data_json'] = json.loads(data['raw_ep_data_json']) if data.get('raw_ep_data_json') else None
                        return data
                    else:
                        logger.info(f"Cached data for SOC {occupation_code} is stale (older than {DATA_STALENESS_THRESHOLD_DAYS} days).")
                else:
                    logger.warning(f"last_api_fetch timestamp missing for SOC {occupation_code} in DB.")
    except SQLAlchemyError as e:
        logger.error(f"DB error retrieving data for SOC {occupation_code}: {e}", exc_info=True)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from DB for SOC {occupation_code}: {e}", exc_info=True)
    return None

def save_bls_data_to_db(data: Dict[str, Any], engine) -> bool:
    """Saves or updates BLS data in the database."""
    required_keys = ["occupation_code", "job_title", "standardized_title", "last_api_fetch"]
    if not all(key in data for key in required_keys):
        logger.error(f"Missing required keys for saving to DB. Data: {data.keys()}")
        return False

    # Ensure JSON fields are strings
    data_to_save = data.copy()
    if data_to_save.get('raw_oes_data_json') is not None and not isinstance(data_to_save['raw_oes_data_json'], str):
        data_to_save['raw_oes_data_json'] = json.dumps(data_to_save['raw_oes_data_json'])
    if data_to_save.get('raw_ep_data_json') is not None and not isinstance(data_to_save['raw_ep_data_json'], str):
        data_to_save['raw_ep_data_json'] = json.dumps(data_to_save['raw_ep_data_json'])
    
    data_to_save['last_updated_in_db'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    try:
        with engine.connect() as conn:
            # Check if record exists
            exists = conn.execute(text("SELECT 1 FROM bls_job_data WHERE occupation_code = :occupation_code"),
                                  {"occupation_code": data['occupation_code']}).fetchone()
            if exists:
                stmt = bls_job_data_table.update().where(bls_job_data_table.c.occupation_code == data['occupation_code']).values(**data_to_save)
                logger.info(f"Updating existing BLS data in DB for SOC {data['occupation_code']}.")
            else:
                stmt = bls_job_data_table.insert().values(**data_to_save)
                logger.info(f"Inserting new BLS data into DB for SOC {data['occupation_code']}.")
            conn.execute(stmt)
            conn.commit()
            return True
    except SQLAlchemyError as e:
        logger.error(f"DB error saving data for SOC {data['occupation_code']}: {e}", exc_info=True)
        # conn.rollback() # Handled by context manager
    return False

def _parse_oes_api_response(oes_response: Dict[str, Any], soc_code: str) -> Dict[str, Any]:
    """Helper to parse data from bls_connector.get_occupation_data response."""
    parsed = {}
    if oes_response.get("status") == "success" and "data" in oes_response:
        oes_data = oes_response["data"]
        parsed["median_wage"] = oes_data.get("annual_median_wage")
        parsed["mean_wage"] = oes_data.get("annual_mean_wage")
        parsed["current_employment"] = oes_data.get("employment")
        # Assuming OES data is annual, find the latest year.
        if oes_data.get("employment_trend"):
            latest_employment_year_data = max(oes_data["employment_trend"], key=lambda x: x.get("year", "0"), default=None)
            if latest_employment_year_data:
                parsed["oes_data_year"] = latest_employment_year_data.get("year")
    else:
        logger.warning(f"OES data fetch failed or was empty for SOC {soc_code}: {oes_response.get('message', 'No message')}")
    return parsed

def _fetch_ep_data_series(soc_code_no_hyphen: str, engine) -> Dict[str, Any]:
    """
    Attempts to fetch Employment Projections (EP) data using illustrative series IDs.
    This is a placeholder for a more robust EP series ID construction logic.
    """
    # Illustrative EP Series IDs (these are examples and may need adjustment based on current BLS EP tables)
    # EPU<SOC>001311 - Base employment
    # EPU<SOC>001321 - Projected employment
    # EPU<SOC>001331 - Employment change, numeric
    # EPU<SOC>001341 - Employment change, percent
    # EPU<SOC>001371 - Occupational openings
    ep_series_suffixes = {
        "ep_base_employment": "001311",
        "ep_projected_employment": "001321",
        "ep_employment_change_numeric": "001331",
        "ep_employment_change_percent": "001341",
        "ep_annual_job_openings": "001371"
    }
    series_ids_to_fetch = [f"EPU{soc_code_no_hyphen}{suffix}" for suffix in ep_series_suffixes.values()]
    
    # Determine projection years (typically a 10-year span, e.g., 2022-2032)
    # This needs to align with the latest BLS EP release. For now, use a recent typical span.
    # Ideally, this would be dynamically determined.
    current_year = datetime.datetime.now().year
    proj_start_year = str(current_year - 2) # Example: if 2024, use 2022 as base
    proj_end_year = str(current_year + 8)   # Example: if 2024, use 2032 as projection

    logger.info(f"Attempting to fetch EP data for SOC {soc_code_no_hyphen} with series: {series_ids_to_fetch} for years {proj_start_year}-{proj_end_year}")
    
    ep_raw_response = bls_connector.get_bls_data(series_ids_to_fetch, proj_start_year, proj_end_year)
    
    ep_data = {"raw_ep_data_json": ep_raw_response} # Store the raw response
    if ep_raw_response.get("status") == "REQUEST_SUCCEEDED":
        logger.info(f"EP API call for SOC {soc_code_no_hyphen} succeeded (may still have no data for specific series).")
        for series_result in ep_raw_response.get("Results", {}).get("series", []):
            series_id = series_result.get("seriesID")
            data_points = series_result.get("data")
            if data_points: # Take the first (latest) data point value
                value_str = data_points[0].get("value")
                try:
                    value = float(value_str) if '.' in value_str else int(value_str)
                    # Map back from series ID suffix to a meaningful key
                    for key, suffix in ep_series_suffixes.items():
                        if series_id.endswith(suffix):
                            ep_data[key] = value
                            # Try to get base and projection years from data if possible
                            ep_data["ep_base_year"] = data_points[0].get("year") # Assuming first point is base
                            ep_data["ep_proj_year"] = data_points[0].get("year") # This needs better logic for proj year
                            break
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse value '{value_str}' for EP series {series_id}: {e}")
    else:
        logger.warning(f"EP data fetch failed for SOC {soc_code_no_hyphen}: {ep_raw_response.get('message', 'No message')}")
    return ep_data


def fetch_and_store_bls_data(job_title_searched: str, occupation_code: str, standardized_soc_title: str, job_category: str, engine) -> Optional[Dict[str, Any]]:
    """Fetches data from BLS API (OES and EP), combines, and stores it in DB."""
    logger.info(f"Fetching new BLS data from API for SOC {occupation_code} ('{standardized_soc_title}').")
    
    # 1. Fetch OES Data (Employment, Wages)
    oes_response = bls_connector.get_occupation_data(occupation_code)
    db_entry = _parse_oes_api_response(oes_response, occupation_code)
    db_entry["raw_oes_data_json"] = oes_response # Store raw OES response

    # 2. Fetch Employment Projections (EP) Data
    soc_code_no_hyphen = occupation_code.replace("-", "")
    if len(soc_code_no_hyphen) == 6: # Valid for EP series construction
        ep_data_fetched = _fetch_ep_data_series(soc_code_no_hyphen, engine)
        # Merge fetched EP data into db_entry
        db_entry["projected_employment"] = ep_data_fetched.get("ep_projected_employment")
        db_entry["employment_change_numeric"] = ep_data_fetched.get("ep_employment_change_numeric")
        db_entry["percent_change"] = ep_data_fetched.get("ep_employment_change_percent")
        db_entry["annual_job_openings"] = ep_data_fetched.get("ep_annual_job_openings")
        db_entry["ep_base_year"] = ep_data_fetched.get("ep_base_year")
        db_entry["ep_proj_year"] = ep_data_fetched.get("ep_proj_year")
        db_entry["raw_ep_data_json"] = ep_data_fetched.get("raw_ep_data_json") # Store raw EP response
    else:
        logger.warning(f"SOC code {occupation_code} not valid for EP series construction. Skipping EP data fetch.")

    # Consolidate current_employment: OES latest is usually more up-to-date for "current"
    # EP base year employment might be older. If OES current_employment is missing, consider EP base.
    if db_entry.get("current_employment") is None and ep_data_fetched.get("ep_base_employment") is not None:
        db_entry["current_employment"] = ep_data_fetched["ep_base_employment"]
        logger.info(f"Using EP base employment as current_employment for SOC {occupation_code} as OES current was missing.")


    # Finalize common fields for DB
    db_entry["occupation_code"] = occupation_code
    db_entry["job_title"] = job_title_searched # The title user actually searched for
    db_entry["standardized_title"] = standardized_soc_title
    db_entry["job_category"] = job_category
    db_entry["last_api_fetch"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if save_bls_data_to_db(db_entry, engine):
        logger.info(f"Successfully fetched and saved new BLS data for SOC {occupation_code}.")
        return db_entry
    else:
        logger.error(f"Failed to save fetched BLS data for SOC {occupation_code} to DB.")
        # Return the fetched data anyway, but indicate it's not from DB cache
        db_entry["_db_save_failed"] = True 
        return db_entry

def _generate_dynamic_risk_factors(job_category: str, bls_data: Dict[str, Any]) -> List[str]:
    factors = []
    # Generic
    factors.append("Routine and predictable tasks are susceptible to AI-driven automation.")
    # Category-specific
    if job_category == "Office and Administrative Support Occupations":
        factors.append("Data entry, scheduling, and document processing are increasingly automated by AI tools.")
    elif job_category == "Computer and Mathematical Occupations":
        factors.append("AI-assisted code generation and data analysis tools are becoming more prevalent.")
    elif job_category == "Production Occupations":
        factors.append("Robotics and AI in manufacturing are automating assembly and quality control tasks.")
    # Data-driven
    pc = bls_data.get("percent_change")
    if pc is not None and pc < 0:
        factors.append(f"BLS projects a {pc:.1f}% decline in employment, potentially exacerbated by AI adoption.")
    elif pc is not None and pc < 5: # Slow growth
        factors.append(f"Slow projected employment growth ({pc:.1f}%) may offer limited new opportunities as AI evolves.")
    
    return factors[:4] # Max 4 factors

def _generate_dynamic_protective_factors(job_category: str, bls_data: Dict[str, Any]) -> List[str]:
    factors = []
    # Generic
    factors.append("Complex problem-solving, critical thinking, and creativity remain strong human advantages.")
    factors.append("Interpersonal skills, emotional intelligence, and effective communication are hard to automate.")
    # Category-specific
    if job_category == "Healthcare Practitioners and Technical Occupations":
        factors.append("Direct patient care, empathy, and complex diagnostic judgment require human expertise.")
    elif job_category == "Educational Instruction and Library Occupations":
        factors.append("Mentorship, fostering critical thinking, and adapting to diverse student needs are key human roles.")
    elif job_category == "Management Occupations":
        factors.append("Strategic decision-making, leadership, and managing complex human dynamics are vital.")
    # Data-driven
    pc = bls_data.get("percent_change")
    ao = bls_data.get("annual_job_openings")
    if pc is not None and pc > 10: # Strong growth
        factors.append(f"Strong BLS projected growth ({pc:.1f}%) indicates robust demand for this occupation.")
    if ao is not None and ao > (bls_data.get("current_employment", 0) * 0.05): # High turnover/openings
        factors.append(f"A high number of annual job openings ({ao:,}) suggests ongoing opportunities.")
        
    return factors[:4] # Max 4 factors

def calculate_ai_risk(job_title: str, job_category: str, bls_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calculates AI risk based on category and BLS data."""
    base_risk = CATEGORY_BASE_RISK.get(job_category, 45) # Default base risk
    
    # Modifier for projected percent change (PC)
    pc_modifier = 0
    pc = bls_data.get("percent_change")
    if pc is not None:
        if pc > 15: pc_modifier = -15      # Very strong growth
        elif pc > 5: pc_modifier = -10     # Strong growth
        elif pc > 0: pc_modifier = -5      # Moderate growth
        elif pc < -15: pc_modifier = +15   # Very strong decline
        elif pc < -5: pc_modifier = +10    # Strong decline
        elif pc < 0: pc_modifier = +5      # Moderate decline
        
    # Modifier for median wage (MW) - higher wage often implies more complex, less automatable tasks
    mw_modifier = 0
    mw = bls_data.get("median_wage")
    # Assuming national median wage around $50-60k for context. This should be dynamic if possible.
    if mw is not None:
        if mw > 100000: mw_modifier = -10   # High wage
        elif mw > 70000: mw_modifier = -5   # Above average
        elif mw < 40000: mw_modifier = +10  # Low wage
        elif mw < 55000: mw_modifier = +5   # Below average

    year_5_risk = min(max(base_risk + pc_modifier + mw_modifier, 5), 95) # Clamp between 5% and 95%
    
    # 1-year risk is a fraction of 5-year, more influenced by current tech state (base_risk reflects this partly)
    # For simplicity, let's use a factor, but this could be more nuanced.
    year_1_risk_factor = 0.4 + (base_risk / 250) # Ranges roughly 0.4 to 0.8
    year_1_risk = round(year_5_risk * year_1_risk_factor, 1)
    year_5_risk = round(year_5_risk, 1)

    risk_category = "Low"
    if year_5_risk >= 70: risk_category = "Very High"
    elif year_5_risk >= 50: risk_category = "High"
    elif year_5_risk >= 30: risk_category = "Moderate"

    analysis_parts = [
        f"The role of '{job_title}' (SOC: {bls_data.get('occupation_code', 'N/A')}), categorized under '{job_category}', shows a {risk_category.lower()} AI displacement risk over the next 5 years ({year_5_risk}%).",
        f"The 1-year projected risk is {year_1_risk}%."
    ]
    if pc is not None:
        analysis_parts.append(f"BLS projects an employment change of {pc:.1f}% for this occupation group by {bls_data.get('ep_proj_year', 'the projection year')}.")
    if mw is not None:
        analysis_parts.append(f"The median annual wage is approximately ${mw:,.0f} (as of {bls_data.get('oes_data_year', 'latest OES data')}).")
    
    analysis_parts.append("Factors influencing this assessment include general automation trends for the category, BLS employment projections, and wage levels.")
    if risk_category in ["High", "Very High"]:
        analysis_parts.append("Roles with significant routine tasks, lower complexity, or in sectors with rapid AI adoption face higher risks. Upskilling and focusing on uniquely human skills is advised.")
    elif risk_category == "Moderate":
        analysis_parts.append("This occupation will likely see changes due to AI, with some tasks being automated. Continuous learning and adaptation are important.")
    else: # Low
        analysis_parts.append("This occupation appears relatively resilient to AI displacement in the near term, likely due to its reliance on complex human judgment, creativity, or interpersonal skills.")

    return {
        "year_1_risk": year_1_risk,
        "year_5_risk": year_5_risk,
        "risk_category": risk_category,
        "risk_factors": _generate_dynamic_risk_factors(job_category, bls_data),
        "protective_factors": _generate_dynamic_protective_factors(job_category, bls_data),
        "analysis": " ".join(analysis_parts)
    }

def get_complete_job_data(job_title: str) -> Dict[str, Any]:
    """Main public function to get comprehensive job data (BLS stats + AI risk)."""
    try:
        engine = get_db_engine() # Get engine instance
    except ValueError as e: # Raised if DATABASE_URL is not configured
        logger.critical(f"Cannot proceed with get_complete_job_data: {e}")
        return {"error": str(e), "job_title": job_title, "source": "system_error_db_config"}

    occupation_code, standardized_soc_title, job_category = find_occupation_code(job_title, engine)

    if not occupation_code or occupation_code == "00-0000":
        logger.warning(f"No specific SOC code found for '{job_title}'. Cannot provide detailed BLS data.")
        return {
            "error": f"Job title '{job_title}' could not be definitively mapped to a specific BLS occupation. Try a more standard title.",
            "job_title": job_title, "occupation_code": "00-0000", "job_category": job_category,
            "source": "error_soc_mapping"
        }

    # Try to get from DB first
    bls_data = get_bls_data_from_db(occupation_code, engine)
    source = "bls_database_cache"

    if not bls_data:
        logger.info(f"No fresh cache for SOC {occupation_code}. Fetching from BLS API.")
        bls_data = fetch_and_store_bls_data(job_title, occupation_code, standardized_soc_title, job_category, engine)
        source = "bls_api_live"
        if not bls_data or "_db_save_failed" in bls_data : # If fetch or save failed
            source = "bls_api_fetch_error_or_db_save_failed"


    if not bls_data: # If fetch_and_store also failed to return data
        logger.error(f"Failed to obtain BLS data for SOC {occupation_code} ('{job_title}') from API.")
        return {
            "error": f"Unable to fetch required BLS data for occupation {standardized_soc_title} (SOC: {occupation_code}). The BLS API might be temporarily unavailable or the occupation data may be limited.",
            "job_title": job_title, "occupation_code": occupation_code, "job_category": job_category,
            "source": "error_bls_api_fetch"
        }

    # Calculate AI risk using the fetched BLS data
    ai_risk_assessment = calculate_ai_risk(standardized_soc_title, job_category, bls_data)

    # Combine all data into the final structure
    # Ensure all keys expected by job_api_integration_database_only.py and app_production.py are present
    # The bls_data dict from DB/API should already contain most BLS fields.
    complete_data = {
        "job_title": standardized_soc_title,
        "occupation_code": occupation_code,
        "job_category": job_category,
        "source": source,
        "employment": bls_data.get("current_employment"), # Direct access for app
        "projected_employment": bls_data.get("projected_employment"), # Direct access
        "employment_change_percent": bls_data.get("percent_change"), # Direct access
        "annual_job_openings": bls_data.get("annual_job_openings"), # Direct access
        "median_wage": bls_data.get("median_wage"), # Direct access
        "bls_data": { # Nested for potential future expansion, but provide direct access too
            "occupation_code": occupation_code,
            "standardized_title": standardized_soc_title,
            "job_category": job_category,
            "current_employment": bls_data.get("current_employment"),
            "projected_employment": bls_data.get("projected_employment"),
            "employment_change_numeric": bls_data.get("employment_change_numeric"),
            "percent_change": bls_data.get("percent_change"),
            "annual_job_openings": bls_data.get("annual_job_openings"),
            "median_wage": bls_data.get("median_wage"),
            "mean_wage": bls_data.get("mean_wage"),
            "oes_data_year": bls_data.get("oes_data_year"),
            "ep_base_year": bls_data.get("ep_base_year"),
            "ep_proj_year": bls_data.get("ep_proj_year"),
            "raw_oes_data_json": bls_data.get("raw_oes_data_json"), # For debugging or advanced use
            "raw_ep_data_json": bls_data.get("raw_ep_data_json")   # For debugging or advanced use
        },
        "last_updated": bls_data.get("last_api_fetch"), # When data was fetched from BLS
        **ai_risk_assessment # Merges all keys from risk assessment
    }
    return complete_data


if __name__ == '__main__':
    # Example usage for testing this module directly
    # Ensure DATABASE_URL and BLS_API_KEY are set in your environment for this to work.
    logger.info("Running bls_job_mapper.py direct tests...")
    
    test_titles = ["Software Developer", "Registered Nurse", "Truck Driver", "NonExistentJobXYZ123", "Accountant", "Chief Executive"]
    
    for title in test_titles:
        logger.info(f"\n--- Testing job title: '{title}' ---")
        try:
            data = get_complete_job_data(title)
            if "error" in data:
                logger.error(f"Error for '{title}': {data['error']}")
            else:
                logger.info(f"Data for '{data['job_title']}' (SOC: {data['occupation_code']}, Cat: {data['job_category']}):")
                logger.info(f"  Source: {data['source']}, Last API Fetch: {data['last_updated']}")
                logger.info(f"  Employment: Current={data.get('employment')}, Projected={data.get('projected_employment')}, Change%={data.get('employment_change_percent')}")
                logger.info(f"  Wages: Median=${data.get('median_wage')}")
                logger.info(f"  AI Risk: 1Y={data['year_1_risk']}%, 5Y={data['year_5_risk']}% ({data['risk_category']})")
                logger.info(f"  Risk Factors: {data['risk_factors']}")
                logger.info(f"  Protective Factors: {data['protective_factors']}")
                # logger.info(f"  Analysis: {data['analysis']}") # Can be long
        except Exception as e:
            logger.error(f"Critical exception during test for '{title}': {e}", exc_info=True)

    logger.info("\nbls_job_mapper.py direct tests complete.")
