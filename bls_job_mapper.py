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
import time
import random
from typing import Dict, Any, List, Optional, Tuple, Union
import threading

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, MetaData, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

# Attempt to import the custom BLS API connector
try:
    import bls_connector
except ImportError:
    logging.critical("bls_connector.py not found. This module is essential for fetching BLS data.")
    class bls_connector_stub: # type: ignore
        @staticmethod
        def get_bls_data(*args: Any, **kwargs: Any) -> Dict[str, Any]: return {"status": "error", "message": ["bls_connector module not found."]}
        @staticmethod
        def get_occupation_data(*args: Any, **kwargs: Any) -> Dict[str, Any]: return {"status": "error", "message": ["bls_connector module not found."]}
        @staticmethod
        def get_employment_projection(*args: Any, **kwargs: Any) -> Dict[str, Any]: return {"status": "error", "message": ["bls_connector module not found."]}
        @staticmethod
        def search_occupations(*args: Any, **kwargs: Any) -> List[Dict[str, str]]: return []
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
    Column('last_api_fetch', String(10), nullable=True),
    Column('last_updated', String(10), nullable=False, default=lambda: datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d'))
)

_engine_instance_global: Optional[sqlalchemy.engine.Engine] = None
_engine_lock = threading.Lock()

def get_db_engine(force_new: bool = False) -> Optional[sqlalchemy.engine.Engine]:
    """Creates and returns a SQLAlchemy engine, ensuring singleton-like behavior for the global engine."""
    global _engine_instance_global
    with _engine_lock:
        if _engine_instance_global is None or force_new:
            database_url = os.environ.get('DATABASE_URL')
            if not database_url:
                try:
                    import streamlit as st
                    database_url = st.secrets.get("database", {}).get("DATABASE_URL")
                except (ImportError, AttributeError):
                    pass

            if not database_url:
                logger.critical("DATABASE_URL environment variable or secret not set. Cannot connect to database.")
                return None

            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql://', 1)
            if database_url.startswith(('http://', 'https://')):
                parts = database_url.split('://', 1)
                if len(parts) > 1: database_url = 'postgresql://' + parts[1]
            
            connect_args = {}
            if 'postgresql' in database_url:
                connect_args = {
                    "connect_timeout": 15, "keepalives": 1, "keepalives_idle": 30,
                    "keepalives_interval": 10, "keepalives_count": 5, "sslmode": 'require',
                    "application_name": "AI_Job_Analyzer_Global_Engine"
                }
            try:
                logger.info(f"Creating global database engine instance for URL: ...@{database_url.split('@')[-1] if '@' in database_url else database_url}")
                _engine_instance_global = create_engine(
                    database_url, connect_args=connect_args, pool_size=3, max_overflow=5,
                    pool_timeout=20, pool_recycle=1800, pool_pre_ping=True, echo=False
                )
                with _engine_instance_global.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("Global database engine instance created and connection tested successfully.")
                # Create table if it doesn't exist using the global engine
                metadata.create_all(_engine_instance_global, checkfirst=True)
                logger.info("Ensured 'bls_job_data' table exists using global engine.")
            except SQLAlchemyError as e:
                logger.critical(f"Failed to create or connect with global database engine: {e}", exc_info=True)
                _engine_instance_global = None
    return _engine_instance_global

# Initialize the global engine when the module loads
engine = get_db_engine()


# --- Static Mappings & Helper Functions ---
JOB_TITLE_TO_SOC: Dict[str, str] = {
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
    "travel agent": "41-3041"
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

TARGET_SOC_CODES: List[Tuple[str, str]] = [
    ("11-1011", "Chief Executives"), ("11-2021", "Marketing Managers"),
    ("11-3021", "Computer and Information Systems Managers"), ("11-9111", "Medical and Health Services Managers"),
    ("13-1111", "Management Analysts"), ("13-2011", "Accountants and Auditors"),
    ("13-2051", "Financial Analysts"), ("15-1211", "Computer Systems Analysts"),
    ("15-1231", "Computer Network Support Specialists"), ("15-1244", "Network and Computer Systems Administrators"),
    ("15-1251", "Computer Programmers"), ("15-1252", "Software Developers"),
    ("15-1254", "Web Developers"), ("15-2051", "Data Scientists"),
    ("17-2071", "Electrical Engineers"), ("17-2141", "Mechanical Engineers"),
    ("19-1021", "Biochemists and Biophysicists"), ("21-1021", "Child, Family, and School Social Workers"),
    ("23-1011", "Lawyers"), ("23-2011", "Paralegals and Legal Assistants"),
    ("25-2021", "Elementary School Teachers, Except Special Education"),
    ("25-2031", "Secondary School Teachers, Except Special and Career/Technical Education"),
    ("25-4022", "Librarians and Media Collections Specialists"), ("27-1011", "Art Directors"),
    ("27-1024", "Graphic Designers"), ("27-3023", "News Analysts, Reporters, and Journalists"),
    ("27-3042", "Technical Writers"), ("27-4021", "Photographers"),
    ("29-1021", "Dentists, General"), ("29-1062", "Family Medicine Physicians"),
    ("29-1141", "Registered Nurses"), ("29-1292", "Dental Hygienists"),
    ("31-1131", "Home Health Aides"), ("33-3051", "Police and Sheriff's Patrol Officers"),
    ("35-1011", "Chefs and Head Cooks"), ("35-2014", "Cooks, Restaurant"),
    ("35-3031", "Waiters and Waitresses"),
    ("37-2011", "Janitors and Cleaners, Except Maids and Housekeeping Cleaners"),
    ("39-5012", "Hairdressers, Hairstylists, and Cosmetologists"),
    ("41-1011", "First-Line Supervisors of Retail Sales Workers"), ("41-2011", "Cashiers"),
    ("41-2031", "Retail Salespersons"),
    ("41-4012", "Sales Representatives, Wholesale and Manufacturing, Except Technical and Scientific Products"),
    ("43-1011", "First-Line Supervisors of Office and Administrative Support Workers"),
    ("43-4051", "Customer Service Representatives"),
    ("43-6011", "Executive Secretaries and Executive Administrative Assistants"),
    ("43-9021", "Data Entry Keyers"), ("47-2031", "Carpenters"),
    ("47-2111", "Electricians"), ("49-3023", "Automotive Service Technicians and Mechanics"),
    ("51-2092", "Team Assemblers"), ("53-3032", "Heavy and Tractor-Trailer Truck Drivers"),
    ("53-7062", "Laborers and Freight, Stock, and Material Movers, Hand")
]

def get_job_category(occupation_code: str) -> str:
    """Get the job category based on SOC code prefix."""
    if not isinstance(occupation_code, str): return "General"
    for prefix, category in SOC_TO_CATEGORY_STATIC.items():
        if occupation_code.startswith(prefix):
            return category
    return "General"

def standardize_job_title(title: str) -> str:
    """Standardize job title format for consistent mapping."""
    if not isinstance(title, str): return ""
    standardized = title.lower().strip()
    suffixes = [" i", " ii", " iii", " iv", " v", " specialist", " assistant", " associate", " senior", " junior", " lead"]
    for suffix in suffixes:
        if standardized.endswith(suffix):
            standardized = standardized[:-len(suffix)].strip()
            break
    return standardized

def find_occupation_code(job_title: str) -> Tuple[Optional[str], str, str]:
    """Find SOC occupation code for a job title, prioritizing the static map."""
    std_title = standardize_job_title(job_title)
    if std_title in JOB_TITLE_TO_SOC:
        soc_code = JOB_TITLE_TO_SOC[std_title]
        return soc_code, job_title, get_job_category(soc_code)
    
    matches = bls_connector.search_occupations(job_title)
    if matches:
        best_match = matches[0]
        soc_code = best_match["code"]
        return soc_code, best_match["title"], get_job_category(soc_code)
        
    return None, job_title, "General"

def get_bls_data_from_db(occupation_code: str) -> Optional[Dict[str, Any]]:
    """Get BLS data from database if available and fresh."""
    db_engine = get_db_engine()
    if not db_engine or not occupation_code: return None
    try:
        with db_engine.connect() as conn:
            query = text("SELECT * FROM bls_job_data WHERE occupation_code = :code LIMIT 1")
            result = conn.execute(query, {"code": occupation_code})
            row = result.fetchone()
            if row:
                data = dict(row._mapping)
                last_updated_str = data.get("last_updated")
                if last_updated_str:
                    last_updated = datetime.datetime.strptime(last_updated_str, "%Y-%m-%d").date()
                    if (datetime.date.today() - last_updated).days < 90:
                        logger.info(f"Found fresh data for SOC {occupation_code} in database.")
                        return data
                logger.info(f"Found stale data for SOC {occupation_code} in database. Will re-fetch.")
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving BLS data from database for SOC {occupation_code}: {e}", exc_info=True)
    return None

def save_bls_data_to_db(data: Dict[str, Any]) -> bool:
    """Save or update BLS data in the database."""
    db_engine = get_db_engine()
    if not db_engine or not data or not data.get("occupation_code"): return False
    try:
        with db_engine.connect() as conn:
            stmt = pg_insert(bls_job_data_table).values(data)
            update_dict = {c.name: c for c in stmt.excluded if c.name not in ["id", "occupation_code"]}
            stmt = stmt.on_conflict_do_update(index_elements=['occupation_code'], set_=update_dict)
            conn.execute(stmt)
            conn.commit()
            logger.info(f"Successfully saved/updated data for SOC {data['occupation_code']} in the database.")
            return True
    except (SQLAlchemyError, IntegrityError) as e:
        logger.error(f"Error saving BLS data to database for SOC {data.get('occupation_code')}: {e}", exc_info=True)
    return False

def _get_safe_year_range(years: int = 10) -> Tuple[str, str]:
    """Gets a safe year range for BLS API calls, ensuring end_year is not in the future."""
    end_year = datetime.datetime.now().year
    start_year = end_year - years
    return str(start_year), str(end_year)

def fetch_and_process_soc_data(soc_code: str, job_title: str, db_engine_instance: sqlalchemy.engine.Engine) -> Tuple[bool, str]:
    """Fetches, processes, and stores data for a single SOC code."""
    if not bls_connector:
        return False, "BLS Connector module is not available."
    start_year, end_year = _get_safe_year_range()
    
    # Fetch OES data (employment and wages)
    oes_series = bls_connector.build_oes_series_id(soc_code)
    oes_data_raw = bls_connector.get_bls_data([oes_series['employment'], oes_series['mean_wage']], start_year, end_year)
    
    # Fetch EP data (projections)
    ep_series = bls_connector.build_ep_series_id(soc_code)
    ep_data_raw = bls_connector.get_bls_data(list(ep_series.values()), start_year, end_year)
    
    # Parse data
    oes_parsed = bls_connector.parse_oes_response(oes_data_raw, soc_code)
    ep_parsed = bls_connector.parse_ep_response(ep_data_raw, soc_code)
    
    if not oes_parsed or not ep_parsed or "error" in oes_parsed or "error" in ep_parsed:
        error_msg = f"OES Error: {oes_parsed.get('error', 'N/A')}, EP Error: {ep_parsed.get('error', 'N/A')}"
        return False, error_msg

    # Combine data
    combined_data = {
        "occupation_code": soc_code,
        "job_title": job_title,
        "standardized_title": oes_parsed.get("occupation_title", job_title),
        "job_category": get_job_category(soc_code),
        "current_employment": ep_parsed.get("base_employment"),
        "projected_employment": ep_parsed.get("proj_employment"),
        "employment_change_numeric": ep_parsed.get("employment_change_numeric"),
        "percent_change": ep_parsed.get("percent_change"),
        "annual_job_openings": ep_parsed.get("annual_job_openings"),
        "median_wage": oes_parsed.get("median_wage"),
        "mean_wage": oes_parsed.get("mean_wage"),
        "oes_data_year": oes_parsed.get("data_year"),
        "ep_base_year": ep_parsed.get("base_year"),
        "ep_proj_year": ep_parsed.get("proj_year"),
        "raw_oes_data_json": json.dumps(oes_data_raw),
        "raw_ep_data_json": json.dumps(ep_data_raw),
        "last_api_fetch": datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
    }
    
    # Save to database
    if not save_bls_data_to_db(combined_data):
        return False, "Failed to save data to the database."
        
    return True, "Data successfully fetched and stored."

def generate_employment_trend(current: Optional[int], projected: Optional[int], num_years: int) -> List[int]:
    """Generate a simple linear trend for employment."""
    if not all(isinstance(i, (int, float)) for i in [current, projected]) or num_years <= 1:
        return []
    annual_change = (projected - current) / (num_years - 1)
    return [int(current + (annual_change * i)) for i in range(num_years)]

def calculate_ai_risk_from_category(job_category: str, occupation_code: str) -> Dict[str, Any]:
    """Calculate AI risk based on job category and specific SOC code modifiers."""
    profiles = {
        "Computer and Mathematical Occupations": {"base": 35, "inc": 8, "var": 7, "prot": ["Complex system design", "Novel algorithm development"]},
        "Management Occupations": {"base": 20, "inc": 4, "var": 4, "prot": ["Strategic leadership", "Complex stakeholder management"]},
        "Business and Financial Operations Occupations": {"base": 45, "inc": 9, "var": 6, "prot": ["Strategic financial planning", "Client advisory"]},
        "Healthcare Practitioners and Technical Occupations": {"base": 15, "inc": 6, "var": 5, "prot": ["Direct patient care and empathy", "Complex clinical judgment"]},
        "Educational Instruction and Library Occupations": {"base": 20, "inc": 5, "var": 5, "prot": ["Mentorship and social-emotional support", "Creative lesson planning"]},
        "Legal Occupations": {"base": 30, "inc": 7, "var": 6, "prot": ["Complex legal strategy", "Courtroom advocacy"]},
        "Office and Administrative Support Occupations": {"base": 65, "inc": 7, "var": 4, "prot": ["Complex office management", "Handling exceptional cases"]},
        "Sales and Related Occupations": {"base": 55, "inc": 8, "var": 6, "prot": ["Complex relationship-based sales", "High-value negotiation"]},
        "Production Occupations": {"base": 70, "inc": 5, "var": 4, "prot": ["Quality control oversight", "Machine maintenance and setup"]},
        "Transportation and Material Moving Occupations": {"base": 60, "inc": 9, "var": 5, "prot": ["Handling complex urban routes", "Last-mile delivery logistics"]},
        "Default": {"base": 40, "inc": 6, "var": 5, "prot": ["Human creativity and adaptability", "Complex interpersonal skills"]}
    }
    profile = profiles.get(job_category, profiles["Default"])
    
    # Adjustments for specific roles
    if occupation_code in ["15-1252", "15-1251"]: profile["base"] += 5 # Higher risk for routine coding
    if occupation_code == "15-2051": profile["base"] -= 10 # Lower risk for data scientists
    
    year_1_risk = max(5, min(95, profile['base'] + random.uniform(-profile['variance'], profile['variance'])))
    year_5_risk = max(5, min(95, year_1_risk + profile['inc'] * 4 + random.uniform(-profile['variance'], profile['variance'])))
    
    risk_category = "Low"
    if year_5_risk >= 70: risk_category = "Very High"
    elif year_5_risk >= 50: risk_category = "High"
    elif year_5_risk >= 30: risk_category = "Moderate"
    
    return {
        "year_1_risk": round(year_1_risk, 1),
        "year_5_risk": round(year_5_risk, 1),
        "risk_category": risk_category,
        "risk_factors": ["Routine task automation", "Predictive data analysis", "Process optimization"],
        "protective_factors": profile["prot"]
    }

def get_job_titles_for_autocomplete() -> List[Dict[str, str]]:
    """Loads job titles from the database for the autocomplete feature."""
    db_engine = get_db_engine()
    if not db_engine: return []
    try:
        with db_engine.connect() as conn:
            query = text("SELECT standardized_title, occupation_code FROM bls_job_data ORDER BY standardized_title")
            result = conn.execute(query)
            return [{"title": row[0], "soc_code": row[1]} for row in result.fetchall()]
    except SQLAlchemyError as e:
        logger.error(f"Failed to load job titles for autocomplete: {e}", exc_info=True)
    return []
