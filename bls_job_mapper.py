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
    class bls_connector_stub:
        @staticmethod
        def get_bls_data(*args, **kwargs) -> Dict[str, Any]:
            return {"status": "error", "message": "bls_connector module not found."}
        @staticmethod
        def get_oes_data_for_soc(*args, **kwargs) -> Dict[str, Any]:
            return {"status": "error", "message": "bls_connector module not found."}
        @staticmethod
        def get_ep_data_for_soc(*args, **kwargs) -> Dict[str, Any]:
            return {"status": "error", "message": "bls_connector module not found."}
        @staticmethod
        def search_occupations(*args, **kwargs) -> List[Dict[str, str]]:
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
        logger.error(f"""Failed to create database engine: {e}""", exc_info=True)
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
    {"soc_code": "13-1199", "title": "Business Operations Specialists, All Other"}, # Used for Project Manager
    {"soc_code": "11-2021", "title": "Marketing Managers"}, # Also for Product Manager
    {"soc_code": "41-2031", "title": "Retail Salespersons"},
    {"soc_code": "41-2011", "title": "Cashiers"},
    {"soc_code": "43-4051", "title": "Customer Service Representatives"},
    {"soc_code": "53-3032", "title": "Heavy and Tractor-Trailer Truck Drivers"},
    {"soc_code": "43-4171", "title": "Receptionists and Information Clerks"},
    {"soc_code": "15-2051", "title": "Data Scientists"}, # Used for Data Analyst too
    {"soc_code": "13-1111", "title": "Management Analysts"}, # Used for Business Analyst
    {"soc_code": "13-2051", "title": "Financial Analysts"},
    {"soc_code": "13-1071", "title": "Human Resources Specialists"},
    {"soc_code": "27-1024", "title": "Graphic Designers"},
    {"soc_code": "33-3051", "title": "Police and Sheriff's Patrol Officers"},
    {"soc_code": "35-1011", "title": "Chefs and Head Cooks"},
    {"soc_code": "35-2014", "title": "Cooks, Restaurant"},
    {"soc_code": "35-3031", "title": "Waiters and Waitresses"},
    {"soc_code": "37-2011", "title": "Janitors and Cleaners, Except Maids and Housekeeping Cleaners"},
    {"soc_code": "43-6011", "title": "Executive Secretaries and Executive Administrative Assistants"}, # For Admin/Exec Assistant
    {"soc_code": "29-1292", "title": "Dental Hygienists"},
    {"soc_code": "47-2111", "title": "Electricians"},
    {"soc_code": "47-2152", "title": "Plumbers, Pipefitters, and Steamfitters"},
    {"soc_code": "47-2031", "title": "Carpenters"},
    {"soc_code": "47-2061", "title": "Construction Laborers"},
    {"soc_code": "49-3023", "title": "Automotive Service Technicians and Mechanics"},
    {"soc_code": "53-3054", "title": "Taxi Drivers"}, # Used for Uber Driver too
    {"soc_code": "27-3023", "title": "News Analysts, Reporters, and Journalists"},
    {"soc_code": "27-3042", "title": "Technical Writers"},
    {"soc_code": "27-3041", "title": "Editors"},
    {"soc_code": "27-4021", "title": "Photographers"},
    {"soc_code": "23-2011", "title": "Paralegals and Legal Assistants"}, # Used for Court Reporter/Stenographer
    {"soc_code": "41-3041", "title": "Travel Agents"}
]

SOC_TO_CATEGORY: Dict[str, str] = {
    "11-": "Management Occupations", "13-": "Business and Financial Operations Occupations",
    "15-": "Computer and Mathematical Occupations", "17-": "Architecture and Engineering Occupations",
    "19-": "Life, Physical, and Social Science Occupations", "21-": "Community and Social Service Occupations",
    "23-": "Legal Occupations", "25-": "Educational Instruction and Library Occupations",
    "27-": "Arts, Design, Entertainment, Sports, and Media Occupations",
    "29-": "Healthcare Practitioners and Technical Occupations", "31-": "Healthcare Support Occupations",
    "33-": "Protective Service Occupations", "35-": "Food Preparation and Serving Related Occupations",
    "37-": "Building and Grounds Cleaning and Maintenance Occupations",
    "39-": "Personal Care and Service Occupations", "41-": "Sales and Related Occupations",
    "43-": "Office and Administrative Support Occupations", "45-": "Farming, Fishing, and Forestry Occupations",
    "47-": "Construction and Extraction Occupations", "49-": "Installation, Maintenance, and Repair Occupations",
    "51-": "Production Occupations", "53-": "Transportation and Material Moving Occupations"
}

# --- Helper Functions ---
def get_job_category(occupation_code: str) -> str:
    """Gets the job category based on SOC code prefix."""
    for prefix, category in SOC_TO_CATEGORY.items():
        if occupation_code.startswith(prefix):
            return category
    return "General Occupations"

def standardize_job_title(title: str) -> str:
    """Standardizes job title format for consistent mapping."""
    standardized = title.lower().strip()
    suffixes = [" i", " ii", " iii", " iv", " v", " specialist", " assistant", " associate", " senior", " junior", " lead", " intern"]
    for suffix in suffixes:
        if standardized.endswith(suffix):
            standardized = standardized[:-len(suffix)].strip()
            break
    return standardized

def find_occupation_code(job_title_query: str) -> Tuple[str, str, str]:
    """
    Finds SOC occupation code, standardized title, and category for a job title.
    First checks local mapping, then BLS API if available.
    """
    std_query_title = standardize_job_title(job_title_query)
    
    # Check local mapping first
    if std_query_title in JOB_TITLE_TO_SOC:
        soc_code = JOB_TITLE_TO_SOC[std_query_title]
        # Try to find a more official title from TARGET_SOC_CODES for this SOC
        official_title = next((item["title"] for item in TARGET_SOC_CODES if item["soc_code"] == soc_code), job_title_query)
        category = get_job_category(soc_code)
        logger.info(f"Found SOC {soc_code} for '{job_title_query}' (standardized: '{std_query_title}') in local map. Official: '{official_title}', Category: {category}")
        return soc_code, official_title, category

    # If not in local map, try searching with BLS API (via bls_connector)
    logger.info(f"'{std_query_title}' not in local map. Querying BLS API via bls_connector...")
    matches = bls_connector.search_occupations(job_title_query) # This is a placeholder in bls_connector
    if matches:
        best_match = matches[0]
        soc_code = best_match["code"]
        official_title = best_match["title"]
        category = get_job_category(soc_code)
        logger.info(f"BLS API found SOC {soc_code} ('{official_title}') for '{job_title_query}'. Category: {category}")
        # Optionally, update JOB_TITLE_TO_SOC here for future local lookups if desired
        JOB_TITLE_TO_SOC[std_query_title] = soc_code 
        return soc_code, official_title, category

    logger.warning(f"No SOC code found for '{job_title_query}'. Using generic '00-0000'.")
    return "00-0000", job_title_query, "General Occupations"

# --- Database Interaction Functions ---
def get_bls_data_from_db(soc_code: str, engine) -> Optional[Dict[str, Any]]:
    """Gets BLS data from database if available and fresh."""
    if not engine: return None
    try:
        with engine.connect() as conn:
            query = text("SELECT * FROM bls_job_data WHERE occupation_code = :code ORDER BY last_api_fetch DESC LIMIT 1")
            result = conn.execute(query, {"code": soc_code})
            row = result.fetchone()
            if row:
                data = dict(row._mapping) # Convert row to dict
                last_fetch = data.get("last_api_fetch")
                if last_fetch:
                    if isinstance(last_fetch, str): # Handle if timestamp is string
                        last_fetch = datetime.datetime.fromisoformat(last_fetch)
                    if last_fetch.tzinfo is None: # Make timezone-aware if naive
                        last_fetch = last_fetch.replace(tzinfo=datetime.timezone.utc)
                    
                    days_since_fetch = (datetime.datetime.now(datetime.timezone.utc) - last_fetch).days
                    if days_since_fetch < 90: # Data is considered fresh if less than 90 days old
                        logger.info(f"Found fresh data for SOC {soc_code} in DB (fetched {days_since_fetch} days ago).")
                        return data
                    else:
                        logger.info(f"Stale data for SOC {soc_code} in DB (fetched {days_since_fetch} days ago). Will refresh.")
                else:
                    logger.warning(f"last_api_fetch timestamp missing for SOC {soc_code} in DB.")
        return None
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching data for SOC {soc_code}: {e}", exc_info=True)
        return None
    except Exception as e: # Catch other potential errors like datetime parsing
        logger.error(f"Unexpected error fetching data for SOC {soc_code} from DB: {e}", exc_info=True)
        return None

def save_bls_data_to_db(data_to_save: Dict[str, Any], engine) -> bool:
    """Saves or updates BLS data in the database."""
    if not engine: return False
    
    required_keys = ["occupation_code", "job_title", "standardized_title", "last_api_fetch"]
    if not all(key in data_to_save for key in required_keys):
        logger.error(f"Missing required keys for saving data to DB. Data: {data_to_save.keys()}")
        return False

    try:
        with engine.connect() as conn:
            # Check if record exists
            stmt_select = text("SELECT id FROM bls_job_data WHERE occupation_code = :occupation_code")
            existing = conn.execute(stmt_select, {"occupation_code": data_to_save["occupation_code"]}).fetchone()

            data_to_save["last_updated_in_db"] = datetime.datetime.now(datetime.timezone.utc)

            # Prepare data for insertion/update, ensuring all keys from the table are present or defaulted
            db_record = {col.name: data_to_save.get(col.name) for col in bls_job_data_table.columns if col.name != 'id'}
            
            # Ensure numeric fields are correctly typed or None
            for field in ['current_employment', 'projected_employment', 'employment_change_numeric', 
                          'percent_change', 'annual_job_openings', 'median_wage', 'mean_wage']:
                if db_record.get(field) is not None:
                    try:
                        if field in ['percent_change', 'median_wage', 'mean_wage']:
                            db_record[field] = float(db_record[field])
                        else:
                            db_record[field] = int(db_record[field])
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert {field} value '{db_record[field]}' to numeric for SOC {db_record.get('occupation_code')}. Setting to None.")
                        db_record[field] = None
            
            # Ensure JSON fields are strings
            if db_record.get("raw_oes_data_json") is not None and not isinstance(db_record["raw_oes_data_json"], str):
                db_record["raw_oes_data_json"] = json.dumps(db_record["raw_oes_data_json"])
            if db_record.get("raw_ep_data_json") is not None and not isinstance(db_record["raw_ep_data_json"], str):
                db_record["raw_ep_data_json"] = json.dumps(db_record["raw_ep_data_json"])


            if existing:
                # Update
                update_cols = [f"{col} = :{col}" for col in db_record.keys() if col != "occupation_code"]
                stmt_update = text(f"UPDATE bls_job_data SET {', '.join(update_cols)} WHERE occupation_code = :occupation_code")
                conn.execute(stmt_update, db_record)
                logger.info(f"Updated data for SOC {data_to_save['occupation_code']} in DB.")
            else:
                # Insert
                stmt_insert = bls_job_data_table.insert().values(**db_record)
                conn.execute(stmt_insert)
                logger.info(f"Inserted new data for SOC {data_to_save['occupation_code']} into DB.")
            conn.commit()
            return True
    except SQLAlchemyError as e:
        logger.error(f"Database error saving data for SOC {data_to_save.get('occupation_code', 'UNKNOWN')}: {e}", exc_info=True)
        return False
    except Exception as e: # Catch other potential errors
        logger.error(f"Unexpected error saving data for SOC {data_to_save.get('occupation_code', 'UNKNOWN')} to DB: {e}", exc_info=True)
        return False

# --- Main Data Fetching and Processing Logic ---
def fetch_and_process_soc_data(soc_code_info: Dict[str, str], engine, original_job_title_search: str) -> Dict[str, Any]:
    """
    Fetches OES and EP data for a given SOC, processes it, and stores it in the database.
    This is the primary function used by the admin data population tool.
    """
    soc_code = soc_code_info["soc_code"]
    representative_title = soc_code_info["title"] # Official or common title for this SOC
    job_category = get_job_category(soc_code)
    
    logger.info(f"Fetching and processing data for SOC: {soc_code} ('{representative_title}')")

    # Define year range for OES data (typically last few available years)
    current_year = datetime.datetime.now().year
    oes_start_year = str(current_year - 4)  # e.g., 2020 if current is 2024
    oes_end_year = str(current_year - 1)    # e.g., 2023 if current is 2024 (OES data has lag)

    # Fetch OES data (Employment and Wages)
    oes_data_raw = bls_connector.get_oes_data_for_soc(soc_code, oes_start_year, oes_end_year)
    if oes_data_raw.get("status") == "error":
        logger.error(f"Failed to fetch OES data for SOC {soc_code}: {oes_data_raw.get('message')}")
        return {"error": f"OES API Error: {oes_data_raw.get('message')}", "source": "bls_api_fetch_error_or_db_save_failed"}
    
    # Fetch EP data (Employment Projections)
    ep_data_raw = bls_connector.get_ep_data_for_soc(soc_code)
    if ep_data_raw.get("status") == "error":
        logger.error(f"Failed to fetch EP data for SOC {soc_code}: {ep_data_raw.get('message')}")
        return {"error": f"EP API Error: {ep_data_raw.get('message')}", "source": "bls_api_fetch_error_or_db_save_failed"}

    # Prepare data for database insertion
    data_to_save = {
        "occupation_code": soc_code,
        "job_title": original_job_title_search, # The title that led to this SOC, could be an alias
        "standardized_title": representative_title, # The official/common title for this SOC
        "job_category": job_category,
        
        # From OES data
        "current_employment": oes_data_raw.get("employment"), # OES employment is more recent snapshot
        "median_wage": oes_data_raw.get("annual_median_wage"),
        "mean_wage": oes_data_raw.get("annual_mean_wage"),
        "oes_data_year": oes_data_raw.get("data_year"),
        "raw_oes_data_json": json.dumps(oes_data_raw), # Store the parsed OES data
        
        # From EP data
        "projected_employment": ep_data_raw.get("projections", {}).get("projected_employment"),
        "employment_change_numeric": ep_data_raw.get("projections", {}).get("employment_change_numeric"), # Added
        "percent_change": ep_data_raw.get("projections", {}).get("percent_change"),
        "annual_job_openings": ep_data_raw.get("projections", {}).get("annual_job_openings"),
        "ep_base_year": ep_data_raw.get("projections", {}).get("base_year"),
        "ep_proj_year": ep_data_raw.get("projections", {}).get("projection_year"),
        "raw_ep_data_json": json.dumps(ep_data_raw), # Store the parsed EP data
        
        "last_api_fetch": datetime.datetime.now(datetime.timezone.utc)
    }
    
    # If OES employment is missing, try to use EP base year employment
    if data_to_save["current_employment"] is None:
        data_to_save["current_employment"] = ep_data_raw.get("projections", {}).get("current_employment")
        if data_to_save["current_employment"] is not None:
            logger.info(f"Using EP base year employment for SOC {soc_code} as OES employment was missing.")


    if save_bls_data_to_db(data_to_save, engine):
        logger.info(f"Successfully fetched and stored data for SOC {soc_code}.")
        # Return the data that was saved, which is also suitable for formatting for the app
        return data_to_save 
    else:
        logger.error(f"Failed to save data for SOC {soc_code} to database.")
        return {"error": "Failed to save data to database after API fetch.", "source": "bls_api_fetch_error_or_db_save_failed"}

def format_job_data_for_app(db_data: Dict[str, Any], original_search_title: str) -> Dict[str, Any]:
    """
    Formats data (from DB or fresh fetch) into the structure expected by the Streamlit app.
    """
    job_category = db_data.get("job_category", "General Occupations")
    occupation_code = db_data.get("occupation_code", "00-0000")
    
    # Calculate AI risk based on the job category and potentially other BLS stats
    risk_data = calculate_ai_risk_from_category(job_category, occupation_code, db_data)
    
    # Generate employment trend data for the chart
    # Prefer EP data if available, otherwise use OES if it has a trend (less common for OES)
    current_emp = db_data.get("current_employment")
    projected_emp = db_data.get("projected_employment")
    ep_base_year_str = db_data.get("ep_base_year")
    ep_proj_year_str = db_data.get("ep_proj_year")
    oes_data_year_str = db_data.get("oes_data_year")

    trend_years = []
    trend_employment = []

    if current_emp is not None and projected_emp is not None and ep_base_year_str and ep_proj_year_str:
        try:
            base_y = int(ep_base_year_str)
            proj_y = int(ep_proj_year_str)
            if proj_y > base_y:
                num_projection_years = proj_y - base_y
                trend_years = list(range(base_y, proj_y + 1))
                trend_employment = generate_employment_trend(current_emp, projected_emp, num_projection_years + 1)
            else: # Fallback if years are not logical
                 trend_years = [int(oes_data_year_str)] if oes_data_year_str else [datetime.datetime.now().year -1]
                 trend_employment = [current_emp] if current_emp is not None else []
        except ValueError: # Handle case where years are not valid integers
            logger.warning(f"Could not parse EP years for trend: base '{ep_base_year_str}', proj '{ep_proj_year_str}' for SOC {occupation_code}")
            trend_years = [int(oes_data_year_str)] if oes_data_year_str and oes_data_year_str.isdigit() else [datetime.datetime.now().year -1]
            trend_employment = [current_emp] if current_emp is not None else []
    elif current_emp is not None and oes_data_year_str: # Fallback to OES single point if EP is incomplete
        try:
            trend_years = [int(oes_data_year_str)]
            trend_employment = [current_emp]
        except ValueError:
            trend_years = [datetime.datetime.now().year -1]
            trend_employment = [current_emp] if current_emp is not None else []


    formatted_data = {
        "job_title": db_data.get("standardized_title", original_search_title),
        "occupation_code": occupation_code,
        "job_category": job_category,
        "source": "bls_database" if "last_updated_in_db" in db_data else "bls_api_direct", # Indicate source
        
        "bls_data": { # Nesting BLS specific fields
            "current_employment": db_data.get("current_employment"),
            "projected_employment": db_data.get("projected_employment"),
            "employment_change_numeric": db_data.get("employment_change_numeric"),
            "employment_change_percent": db_data.get("percent_change"),
            "annual_job_openings": db_data.get("annual_job_openings"),
            "median_wage": db_data.get("median_wage"),
            "mean_wage": db_data.get("mean_wage"),
            "oes_data_year": db_data.get("oes_data_year"),
            "ep_base_year": db_data.get("ep_base_year"),
            "ep_proj_year": db_data.get("ep_proj_year")
        },
        
        "risk_scores": { # Nesting risk scores
            "year_1": risk_data["year_1_risk"],
            "year_5": risk_data["year_5_risk"]
        },
        "risk_category": risk_data["risk_category"],
        "risk_factors": risk_data["risk_factors"],
        "protective_factors": risk_data["protective_factors"],
        "analysis": risk_data["analysis"],
        "summary": risk_data.get("summary", risk_data["analysis"]), # Ensure summary exists
        
        "trend_data": {
            "years": trend_years,
            "employment": trend_employment
        },
        "similar_jobs": risk_data.get("similar_jobs", []) # Include similar jobs if generated by risk assessment
    }
    return formatted_data

def get_job_data(job_title_query: str) -> Dict[str, Any]:
    """
    Main function to get job data.
    Tries DB cache first, then fetches from BLS API if needed or if data is stale.
    Strictly uses real data; no synthetic fallbacks for job stats.
    """
    engine = get_db_engine() # Ensure engine is initialized
    soc_code, standardized_title, job_category = find_occupation_code(job_title_query)

    if soc_code == "00-0000": # Unmappable job title
        logger.warning(f"Job title '{job_title_query}' could not be mapped to a SOC code.")
        return {"error": f"Job title '{job_title_query}' not found or could not be mapped to a standard occupation. Please try a different title or check BLS OOH for official titles.", "job_title": job_title_query, "source": "mapping_error"}

    # Try to get data from the database
    db_data = get_bls_data_from_db(soc_code, engine)
    
    if db_data:
        logger.info(f"Using cached data from DB for SOC {soc_code} ('{db_data.get('standardized_title', standardized_title)}').")
        return format_job_data_for_app(db_data, job_title_query)
    else:
        logger.info(f"No fresh cache for SOC {soc_code}. Fetching from BLS API.")
        # If not in DB or stale, fetch fresh data (fetch_and_process_soc_data handles API calls and DB saving)
        # We pass the representative title for the SOC code as the 'original_job_title_search' for the purpose of populating the job_title field in the DB
        # if this is the first time we're seeing this SOC code via the admin tool.
        # If get_job_data is called by a user search, job_title_query is the user's original search.
        fresh_data_from_api = fetch_and_process_soc_data({"soc_code": soc_code, "title": standardized_title}, engine, job_title_query)
        
        if "error" in fresh_data_from_api:
            logger.error(f"Failed to fetch or process data for SOC {soc_code}: {fresh_data_from_api['error']}")
            return fresh_data_from_api # Return the error structure
        
        logger.info(f"Successfully fetched and processed data for SOC {soc_code} via API.")
        return format_job_data_for_app(fresh_data_from_api, job_title_query)

# --- AI Risk Calculation ---
def calculate_ai_risk_from_category(job_category: str, occupation_code: str, bls_stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Calculates AI displacement risk based on job category and BLS statistics.
    This version uses more nuanced logic based on real data patterns.
    """
    # Base risk profiles (base_risk_1yr, base_risk_5yr, growth_sensitivity, wage_sensitivity, automation_ceiling)
    # Growth sensitivity: positive means higher growth = lower risk, negative means higher growth = higher risk (e.g. for easily automatable growth roles)
    # Wage sensitivity: positive means higher wage = lower risk
    RISK_PROFILES = {
        "Computer and Mathematical Occupations":          (15, 35,  0.2,  0.1, 70),
        "Management Occupations":                         (10, 25,  0.1,  0.2, 60),
        "Business and Financial Operations Occupations":  (20, 45,  0.15, 0.15, 75),
        "Architecture and Engineering Occupations":       (10, 30,  0.1,  0.15, 65),
        "Life, Physical, and Social Science Occupations": ( 5, 20,  0.05, 0.2, 50),
        "Community and Social Service Occupations":       ( 5, 15,  0.05, 0.1, 40),
        "Legal Occupations":                              (20, 40,  0.1,  0.15, 70),
        "Educational Instruction and Library Occupations":(10, 25,  0.1,  0.1, 50),
        "Arts, Design, Entertainment, Sports, and Media Occupations": (25, 55, 0.0, 0.05, 80), # Creative but tools are powerful
        "Healthcare Practitioners and Technical Occupations": (5, 15, 0.1, 0.2, 40),
        "Healthcare Support Occupations":                 (15, 35,  0.05, 0.05, 60),
        "Protective Service Occupations":                 (10, 25,  0.0,  0.1, 50),
        "Food Preparation and Serving Related Occupations": (40, 70, -0.1, -0.1, 85), # Negative: growth might be in automatable areas
        "Building and Grounds Cleaning and Maintenance Occupations": (30, 60, -0.05, -0.05, 75),
        "Personal Care and Service Occupations":          (20, 45,  0.0,  0.05, 65),
        "Sales and Related Occupations":                  (35, 65, -0.1, -0.05, 80),
        "Office and Administrative Support Occupations":  (50, 80, -0.15, -0.1, 90),
        "Farming, Fishing, and Forestry Occupations":     (25, 50, -0.05, 0.0, 70),
        "Construction and Extraction Occupations":        (15, 35,  0.05, 0.05, 60),
        "Installation, Maintenance, and Repair Occupations": (20, 40, 0.05, 0.1, 65),
        "Production Occupations":                         (45, 75, -0.1, -0.05, 85),
        "Transportation and Material Moving Occupations": (40, 75, -0.15, -0.1, 90),
        "General Occupations":                            (30, 55,  0.0,  0.0, 75) # Default
    }
    
    profile = RISK_PROFILES.get(job_category, RISK_PROFILES["General Occupations"])
    base_1yr, base_5yr, growth_sens, wage_sens, ceiling = profile

    # Adjust risk based on BLS stats if available
    if bls_stats:
        # Growth rate adjustment (normalize percent_change, typically -10% to +30%)
        # A positive growth_sens means high job growth reduces AI risk
        growth_rate = bls_stats.get("percent_change")
        if growth_rate is not None:
            # Normalize growth_rate to a -1 to 1 scale (approx)
            norm_growth = (growth_rate - 5) / 25 # Assuming avg 5%, range -20 to +30
            adjustment = -norm_growth * growth_sens * 10 # Max adjustment of +/- 10 for growth
            base_1yr += adjustment
            base_5yr += adjustment * 1.5 # More impact on 5yr
            logger.debug(f"SOC {occupation_code}: Growth adjustment: {adjustment:.2f} (Rate: {growth_rate}%)")

        # Median wage adjustment (normalize median_wage, e.g. $20k to $200k)
        # A positive wage_sens means higher wage reduces AI risk
        median_wage = bls_stats.get("median_wage")
        if median_wage is not None:
            # Normalize wage to a 0 to 1 scale (approx)
            norm_wage = (median_wage - 20000) / 180000 
            adjustment = - (norm_wage - 0.5) * wage_sens * 10 # Max adjustment of +/- 5 for wage
            base_1yr += adjustment
            base_5yr += adjustment
            logger.debug(f"SOC {occupation_code}: Wage adjustment: {adjustment:.2f} (Wage: ${median_wage})")

    # Apply ceiling and floor
    year_1_risk = max(5, min(base_1yr, ceiling - 5))
    year_5_risk = max(10, min(base_5yr, ceiling))
    
    # Ensure 5-year risk is not less than 1-year risk
    year_5_risk = max(year_5_risk, year_1_risk)

    # Determine risk category string
    if year_5_risk < 30: risk_cat_str = "Low"
    elif year_5_risk < 50: risk_cat_str = "Moderate"
    elif year_5_risk < 70: risk_cat_str = "High"
    else: risk_cat_str = "Very High"

    # Generate textual factors (these are illustrative and should be enhanced)
    risk_factors_list = generate_risk_factors(standardize_job_title(bls_stats.get("standardized_title", "") if bls_stats else ""), job_category)
    protective_factors_list = generate_protective_factors(standardize_job_title(bls_stats.get("standardized_title", "") if bls_stats else ""), job_category)
    
    analysis_text = f"The role of '{bls_stats.get('standardized_title', 'this role') if bls_stats else 'this role'}' within the '{job_category}' category faces a {risk_cat_str.lower()} risk of AI displacement over the next 5 years. "
    if risk_cat_str in ["Low", "Moderate"]:
        analysis_text += "Key protective factors include tasks requiring complex human judgment, creativity, and interpersonal skills. "
    else:
        analysis_text += "This is primarily driven by the potential automation of routine tasks and advancements in AI capabilities relevant to this field. "
    analysis_text += "Continuous upskilling and focusing on uniquely human competencies will be crucial for career resilience."

    return {
        "year_1_risk": round(year_1_risk, 1),
        "year_5_risk": round(year_5_risk, 1),
        "risk_category": risk_cat_str,
        "risk_factors": risk_factors_list,
        "protective_factors": protective_factors_list,
        "analysis": analysis_text,
        "summary": analysis_text # Can be made more concise later
    }

# --- Functions for generating risk/protective factors (can be expanded) ---
def generate_risk_factors(job_title_std: str, job_category: str) -> List[str]:
    factors = [
        "Routine and repetitive tasks are susceptible to automation.",
        "Data processing and pattern recognition can be performed by AI.",
        "Standardized communication or content generation may be automated."
    ]
    if "Administrative Support" in job_category: factors.append("Scheduling and data entry tasks are highly automatable.")
    if "Production" in job_category or "Material Moving" in job_category : factors.append("Physical tasks in controlled environments are targets for robotics.")
    if "Sales" in job_category: factors.append("Basic customer interactions and lead qualification can be automated.")
    if "Computer" in job_category: factors.append("AI-powered code generation and testing tools are emerging.")
    return factors[:4] 

def generate_protective_factors(job_title_std: str, job_category: str) -> List[str]:
    factors = [
        "Requires complex critical thinking and strategic decision-making.",
        "Involves significant interpersonal interaction and empathy.",
        "Demands creativity and novel problem-solving.",
        "Requires adaptability in unpredictable environments."
    ]
    if "Management" in job_category: factors.append("Leadership, team building, and mentoring are key human skills.")
    if "Healthcare" in job_category: factors.append("Direct patient care and complex diagnostics require human oversight.")
    if "Education" in job_category: factors.append("Inspiring students and managing classroom dynamics are human-centric.")
    if "Arts" in job_category: factors.append("Original artistic expression and interpretation are uniquely human.")
    return factors[:4]

# --- Employment Trend Generation ---
def generate_employment_trend(current_emp: Optional[int], projected_emp: Optional[int], num_years: int) -> List[int]:
    """Generates a list of employment numbers for a trend line."""
    if current_emp is None or projected_emp is None or num_years <= 1:
        return [current_emp] if current_emp is not None else []
    
    trend = []
    try:
        # Ensure numeric types for calculation
        current_emp_val = int(current_emp)
        projected_emp_val = int(projected_emp)
        
        # Calculate annual change, ensuring num_years-1 is not zero
        annual_change = (projected_emp_val - current_emp_val) / (num_years -1) if num_years > 1 else 0
        
        for i in range(num_years):
            trend.append(int(current_emp_val + (annual_change * i)))
    except (ValueError, TypeError) as e:
        logger.error(f"Error generating employment trend: current={current_emp}, projected={projected_emp}, years={num_years}. Error: {e}")
        return [current_emp] if current_emp is not None else [] # Fallback to current employment if possible
    return trend

# --- Main Public Functions ---
def get_all_soc_titles_from_db(engine) -> List[Dict[str, str]]:
    """Fetches all distinct SOC codes and their primary titles from the database."""
    if not engine: return []
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT occupation_code, standardized_title FROM bls_job_data ORDER BY standardized_title")
            result = conn.execute(query)
            return [{"soc_code": row[0], "title": row[1]} for row in result.fetchall()]
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching all SOC titles: {e}", exc_info=True)
        return []

def get_all_job_titles_for_autocomplete(engine) -> List[Dict[str, Any]]:
    """Fetches all job titles (standardized and aliases) for autocomplete."""
    if not engine: return []
    # This should ideally also query an aliases table if one exists.
    # For now, it's the same as get_all_soc_titles_from_db for simplicity.
    # Adding 'is_primary' to distinguish.
    soc_titles = get_all_soc_titles_from_db(engine)
    return [{"title": item["title"], "soc_code": item["soc_code"], "is_primary": True} for item in soc_titles]

def get_job_display_name(soc_code: str, engine) -> str:
    """Gets the standardized title for a SOC code from the database."""
    if not engine: return f"SOC {soc_code}"
    try:
        with engine.connect() as conn:
            query = text("SELECT standardized_title FROM bls_job_data WHERE occupation_code = :soc_code LIMIT 1")
            result = conn.execute(query, {"soc_code": soc_code}).scalar_one_or_none()
            return result if result else f"SOC {soc_code}"
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching display name for SOC {soc_code}: {e}", exc_info=True)
        return f"SOC {soc_code}"

# This is the main entry point for the Streamlit app
def get_job_data_for_app(job_title_query: str) -> Dict[str, Any]:
    """
    Main function called by the Streamlit app to get all data for a job title.
    It ensures data is fetched from BLS if not in DB or stale, then formats for UI.
    """
    logger.info(f"get_job_data_for_app called for: '{job_title_query}'")
    engine = get_db_engine() # Ensure engine is initialized within this function's scope if needed by callees
    
    soc_code, standardized_title, job_category = find_occupation_code(job_title_query)

    if soc_code == "00-0000":
        logger.warning(f"Job title '{job_title_query}' could not be mapped to a known SOC code.")
        return {
            "error": f"Job title '{job_title_query}' not found or could not be mapped to a standard occupation. Please try a different title or check BLS OOH for official titles.",
            "job_title": job_title_query,
            "source": "mapping_error"
        }

    # Try to get data from the database
    db_data = get_bls_data_from_db(soc_code, engine)
    
    if db_data:
        logger.info(f"Using cached data from DB for SOC {soc_code} ('{db_data.get('standardized_title', standardized_title)}').")
        return format_job_data_for_app(db_data, job_title_query)
    else:
        logger.info(f"No fresh cache for SOC {soc_code}. Attempting to fetch from BLS API and store.")
        # If not in DB or stale, fetch fresh data.
        # fetch_and_process_soc_data handles API calls and DB saving.
        # We pass the standardized_title as the 'representative_title' for the SOC.
        # original_job_title_search is the user's query.
        fresh_data_from_api = fetch_and_process_soc_data(
            soc_code_info={"soc_code": soc_code, "title": standardized_title}, 
            engine=engine, 
            original_job_title_search=job_title_query
        )
        
        if "error" in fresh_data_from_api:
            logger.error(f"Failed to fetch or process data for SOC {soc_code} via API: {fresh_data_from_api['error']}")
            # Return the error but still include the job title for context in the UI
            return {
                "error": fresh_data_from_api['error'],
                "job_title": job_title_query,
                "occupation_code": soc_code,
                "standardized_title": standardized_title,
                "job_category": job_category,
                "source": fresh_data_from_api.get("source", "api_processing_error")
            }
        
        logger.info(f"Successfully fetched and processed data for SOC {soc_code} ('{fresh_data_from_api.get('standardized_title', standardized_title)}') via API.")
        return format_job_data_for_app(fresh_data_from_api, job_title_query)

if __name__ == "__main__":
    # Example usage for testing (requires DATABASE_URL and BLS_API_KEY to be set)
    logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(logging.DEBUG) # Ensure mapper's logger is also debug
    
    try:
        test_engine = get_db_engine()
        logger.info("Database engine created successfully for testing.")

        # Test with a known SOC code that might need fetching
        test_soc_info = {"soc_code": "15-1252", "title": "Software Developers"} # Software Developers
        logger.info(f"\n--- Testing fetch_and_process_soc_data for {test_soc_info['title']} ---")
        data = fetch_and_process_soc_data(test_soc_info, test_engine, "Software Engineer Test")
        if "error" in data:
            logger.error(f"Error: {data['error']}")
        else:
            logger.info("Data fetched/processed successfully:")
            logger.info(json.dumps(data, indent=2, default=str))

            logger.info(f"\n--- Testing get_job_data_for_app for '{test_soc_info['title']}' (should use cache now) ---")
            app_data = get_job_data_for_app(test_soc_info['title'])
            if "error" in app_data:
                 logger.error(f"Error: {app_data['error']}")
            else:
                logger.info("App data formatted successfully:")
                logger.info(json.dumps(app_data, indent=2, default=str))

        # Test with a job title that needs mapping
        logger.info("\n--- Testing get_job_data_for_app for 'Project Lead' (needs mapping) ---")
        app_data_pm = get_job_data_for_app("Project Lead")
        if "error" in app_data_pm:
            logger.error(f"Error for 'Project Lead': {app_data_pm['error']}")
        else:
            logger.info(f"Data for 'Project Lead' (mapped to SOC {app_data_pm.get('occupation_code')}):")
            logger.info(json.dumps(app_data_pm, indent=2, default=str))

        # Test with a non-existent job title
        logger.info("\n--- Testing get_job_data_for_app for 'Galactic Viceroy of Noodle Quality' (non-existent) ---")
        app_data_fake = get_job_data_for_app("Galactic Viceroy of Noodle Quality")
        if "error" in app_data_fake:
            logger.info(f"Correctly handled non-existent job: {app_data_fake['error']}")
        else:
            logger.error(f"Unexpectedly found data for non-existent job: {app_data_fake}")

    except ValueError as ve:
        logger.critical(f"Test environment not configured: {ve}")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during testing: {e}", exc_info=True)

    logger.info("bls_job_mapper.py tests complete.")
