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
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, MetaData, inspect, Text # Added Text import
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
    Column('last_api_fetch', String(30), nullable=False), # ISO datetime of last successful API fetch (increased length)
    Column('last_updated_in_db', String(30), nullable=False, default=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()) # ISO datetime of DB record update
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
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT standardized_title FROM bls_job_data WHERE occupation_code = :soc LIMIT 1"), {"soc": soc_code}).fetchone()
                if res and res[0]:
                    official_title_for_soc = res[0]
        except SQLAlchemyError as e:
            logger.warning(f"DB error trying to get official title for {soc_code}: {e}")
        logger.info(f"Found SOC '{soc_code}' for '{std_input_title}' via hardcoded map. Official: '{official_title_for_soc}'. Category: {category}")
