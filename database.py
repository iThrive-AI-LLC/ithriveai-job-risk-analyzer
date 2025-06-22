import os
import datetime
import sqlalchemy
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, MetaData, text
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession # Renamed to avoid conflict
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import QueuePool # Import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DisconnectionError # Import specific exceptions
import logging
import time
from typing import List, Dict, Any, Optional, Callable


# Configure logging
# Ensure logger is configured at the top level so it's available throughout the module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s'
)
logger = logging.getLogger(__name__) # Use __name__ for the logger to identify the module

# Initialize Session (the sessionmaker factory) to None at the very top
Session: Optional[sessionmaker] = None

# Base must be defined before models that inherit from it
Base = declarative_base()
metadata = Base.metadata # Use Base.metadata

# Define model for job searches (must be after Base and before main try block)
class JobSearch(Base):
    __tablename__ = 'job_searches'
    
    id = Column(Integer, primary_key=True, autoincrement=True) # Ensure autoincrement for primary key
    job_title = Column(String(255), nullable=False, index=True) # Add index for faster lookups
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, index=True) # Add index
    year_1_risk = Column(Float)
    year_5_risk = Column(Float)
    risk_category = Column(String(50))
    job_category = Column(String(50))
    
    def __repr__(self):
        return f"<JobSearch(job_title='{self.job_title}', risk_category='{self.risk_category}')>"

# Global engine instance, initialized to None
engine: Optional[sqlalchemy.engine.Engine] = None

try:
    # Attempt to initialize database globals
    database_url_env = os.environ.get("DATABASE_URL")
    if database_url_env is None:
        try:
            import streamlit as st # Local import for secrets
            if hasattr(st, 'secrets'):
                # Check if st.secrets is a dict (older Streamlit) or an object with .get (newer)
                if isinstance(st.secrets, dict): 
                    database_url_env = st.secrets.get("database", {}).get("DATABASE_URL")
                elif callable(getattr(st.secrets, "get", None)): # Check if st.secrets has a 'get' method
                     database_url_env = st.secrets.get("database", {}).get("DATABASE_URL")

            if database_url_env:
                logger.info("Using DATABASE_URL from Streamlit secrets.")
                print("Using DATABASE_URL from Streamlit secrets.") # Keep print for Replit console
        except (ImportError, AttributeError) as e:
            # Streamlit might not be available in all contexts, or secrets might not be set up
            logger.info(f"Streamlit secrets not available or error accessing them: {e}. DATABASE_URL must be in environment.")
            print(f"Streamlit secrets not available or error accessing them: {e}. DATABASE_URL must be in environment.")
            pass # Continue, database_url_env might still be None

    if database_url_env:
        logger.info("Attempting to initialize database engine and session factory.")
        print("Attempting to initialize database engine and session factory.")
        
        # Define create_db_engine here, as it uses 'engine' global
        def create_db_engine_internal(url: str, max_retries: int = 3) -> Optional[sqlalchemy.engine.Engine]:
            logger.info(f"Attempting to create database engine with URL: {url}")
            print(f"Attempting to create database engine with URL: {url}")
            retry_count = 0
            last_error: Optional[Exception] = None
            
            if not url: # Check if URL is empty or None
                logger.error("Database URL is not provided. Cannot create engine.")
                print("Database URL is not provided. Cannot create engine.")
                return None

            while retry_count < max_retries:
                try:
                    # Ensure correct URL format for SQLAlchemy
                    if url.startswith('postgres://'):
                        url = url.replace('postgres://', 'postgresql://', 1)
                        logger.info("Converted postgres:// URL to postgresql://")
                        print("Converted postgres:// URL to postgresql://")
                    
                    # Handle common incorrect URL prefixes from some environments
                    if url.startswith(('http://', 'https://')):
                        parts = url.split('://', 1)
                        if len(parts) > 1:
                            url = 'postgresql://' + parts[1]
                            logger.info("Corrected http(s):// URL to postgresql://")
                            print("Corrected http(s):// URL to postgresql://")

                    connect_args = {}
                    if 'postgresql' in url: # Specific args for PostgreSQL
                        connect_args = {
                            "connect_timeout": 10,             # 10 seconds timeout for new connections
                            "keepalives": 1,                   # Enable TCP keepalives
                            "keepalives_idle": 30,             # Seconds of inactivity before sending a keepalive
                            "keepalives_interval": 10,         # Seconds between keepalive retransmissions
                            "keepalives_count": 5,             # Max number of keepalive retransmissions
                            "sslmode": 'require', # Enforce SSL for Neon
                            "application_name": "AI_Job_Analyzer_App_DB_Module" # Helpful for DB logs
                        }
                    
                    engine_instance = sqlalchemy.create_engine( # Use sqlalchemy.create_engine
                        url, 
                        connect_args=connect_args,
                        poolclass=QueuePool, # Explicitly use QueuePool
                        pool_size=3,                         # Reduced pool size for typical Streamlit app
                        max_overflow=5,                      # Allow some overflow
                        pool_timeout=20,                     # Timeout for getting a connection from pool
                        pool_recycle=1800,                   # Recycle connections every 30 minutes
                        pool_pre_ping=True,                  # Check connection liveness
                        echo_pool=False # Set to True for debugging pool behavior
                    )
                    
                    # Test connection immediately
                    with engine_instance.connect() as conn:
                        conn.execute(text("SELECT 1")) # Simple query to test
                    db_host_info = url.split('@')[-1] if '@' in url else url # Avoid logging credentials
                    logger.info(f"Successfully connected to database: {db_host_info}")
                    print(f"Successfully connected to database: {db_host_info}")
                    return engine_instance
                        
                except Exception as e: # Catch broader exceptions during engine creation/connection
                    last_error = e
                    retry_count += 1
                    wait_time = min(2 ** retry_count + (retry_count * 0.1), 15) # Exponential backoff with jitter
                    logger.warning(f"Database connection attempt {retry_count}/{max_retries} failed: {str(e)}. Retrying in {wait_time:.1f} seconds...")
                    print(f"Database connection attempt {retry_count}/{max_retries} failed: {str(e)}. Retrying in {wait_time:.1f} seconds...")
                    time.sleep(wait_time)
            
            logger.error(f"Failed to connect to database after {max_retries} attempts. Last error: {str(last_error)}", exc_info=True)
            print(f"Failed to connect to database after {max_retries} attempts. Last error: {str(last_error)}")
            return None

        engine = create_db_engine_internal(database_url_env) # Assigns to global engine
        if engine:
            Session = sessionmaker(autocommit=False, autoflush=False, bind=engine) # Assigns to global Session (sessionmaker)
            logger.info("Database engine and Session factory (sessionmaker) initialized.")
            print("Database engine and Session factory (sessionmaker) initialized.")
            try:
                Base.metadata.create_all(engine) # Create tables defined in this Base
                logger.info("Database tables (JobSearch) created or verified successfully.")
                print("Database tables (JobSearch) created or verified successfully.")
            except Exception as e_tables:
                logger.error(f"Error creating database tables: {str(e_tables)}", exc_info=True)
                print(f"Error creating database tables: {str(e_tables)}")
                engine = None # Nullify on critical error
                Session = None # Nullify on critical error
        else:
            # Engine creation failed after retries
            logger.critical("Database engine could not be initialized. All database functionality will be disabled.")
            print("Database engine could not be initialized. All database functionality will be disabled.")
            engine = None # Ensure it's None if create_db_engine_internal returned None
            Session = None # Ensure Session factory is also None
    else:
        # database_url_env was None
        logger.warning("DATABASE_URL not found. Database functionality will be disabled.")
        print("DATABASE_URL not found. Database functionality will be disabled.")
        engine = None
        Session = None

except Exception as e_module_load: # Catch any other unexpected error during module load
    logger.critical(f"CRITICAL ERROR during database.py module execution: {e_module_load}", exc_info=True)
    print(f"CRITICAL ERROR during database.py module execution: {e_module_load}")
    engine = None # Ensure globals are None if setup fails
    Session = None

# --- Helper function to create session ---
def _create_session() -> Optional[SQLAlchemySession]: # Use the renamed SQLAlchemySession
    if Session is None: # Check if the sessionmaker factory is initialized
        logger.error("Session factory (sessionmaker) is not initialized. Cannot create database session.")
        print("Session factory (sessionmaker) is not initialized. Cannot create database session.")
        return None
    try:
        session = Session() # Create a new session instance
        return session
    except Exception as e:
        logger.error(f"Error creating database session instance: {str(e)}", exc_info=True)
        print(f"Error creating database session instance: {str(e)}")
        return None

# --- Retry decorator for database operations ---
def execute_with_retry(operation: Callable, *args: Any, max_retries: int = 2, **kwargs: Any) -> Any:
    if Session is None: # If Session factory wasn't initialized, can't proceed
        logger.warning(f"Skipping operation '{operation.__name__}': Database not available (Session factory is None).")
        print(f"Skipping operation '{operation.__name__}': Database not available (Session factory is None).")
        return None # Or appropriate default for the operation

    retry_count = 0
    last_error: Optional[Exception] = None
    
    while retry_count < max_retries:
        session: Optional[SQLAlchemySession] = None
        try:
            session = _create_session()
            if session is None:
                # This case should ideally be caught by the Session is None check above,
                # but as a safeguard if _create_session itself fails.
                logger.error(f"Could not get a session for operation '{operation.__name__}'.")
                print(f"Could not get a session for operation '{operation.__name__}'.")
                return None # Or raise an error, or return a default value

            result = operation(session, *args, **kwargs)
            session.commit() # Commit on success
            return result
        except (OperationalError, DisconnectionError) as e: # Specific retryable errors
            last_error = e
            retry_count += 1
            logger.warning(f"Database operation '{operation.__name__}' failed (attempt {retry_count}/{max_retries}) due to connection issue: {str(e)}. Retrying in {1 * retry_count:.1f} seconds...")
            print(f"Database operation '{operation.__name__}' failed (attempt {retry_count}/{max_retries}) due to connection issue: {str(e)}. Retrying in {1 * retry_count:.1f} seconds...")
            if session:
                try:
                    session.rollback()
                except Exception as rb_exc: # Log error during rollback
                    logger.error(f"Error during rollback on retry: {rb_exc}", exc_info=True)
                    print(f"Error during rollback on retry: {rb_exc}")
            time.sleep(1 * retry_count) # Simple backoff
        except SQLAlchemyError as e: # Non-retryable SQLAlchemy errors
            last_error = e
            logger.error(f"SQLAlchemyError during '{operation.__name__}': {str(e)}", exc_info=True)
            print(f"SQLAlchemyError during '{operation.__name__}': {str(e)}")
            if session:
                try:
                    session.rollback()
                except Exception as rb_exc:
                    logger.error(f"Error during rollback: {rb_exc}", exc_info=True)
                    print(f"Error during rollback: {rb_exc}")
            break # Do not retry for general SQLAlchemy errors
        except Exception as e: # Catch any other exceptions
            last_error = e
            logger.error(f"Unexpected error during '{operation.__name__}': {str(e)}", exc_info=True)
            print(f"Unexpected error during '{operation.__name__}': {str(e)}")
            if session:
                try:
                    session.rollback()
                except Exception as rb_exc:
                    logger.error(f"Error during rollback: {rb_exc}", exc_info=True)
                    print(f"Error during rollback: {rb_exc}")
            break # Do not retry for unexpected errors
        finally:
            if session:
                session.close()
    
    logger.error(f"Operation '{operation.__name__}' failed after {max_retries} retries. Last error: {str(last_error)}")
    print(f"Operation '{operation.__name__}' failed after {max_retries} retries. Last error: {str(last_error)}")
    # Depending on desired behavior, re-raise the error or return a default
    # For now, returning None or an empty list based on typical function signatures
    if "get_" in operation.__name__: return []
    return False if "save_" in operation.__name__ else None


# --- ORM-based data access functions ---
def _save_job_search_impl(session: SQLAlchemySession, job_title: str, risk_data: Dict[str, Any]) -> bool:
    job_search = JobSearch(
        job_title=job_title,
        year_1_risk=risk_data.get('year_1_risk'),
        year_5_risk=risk_data.get('year_5_risk'),
        risk_category=risk_data.get('risk_category'),
        job_category=risk_data.get('job_category')
    )
    session.add(job_search)
    return True

def save_job_search(job_title: str, risk_data: Dict[str, Any]) -> bool:
    return execute_with_retry(_save_job_search_impl, job_title, risk_data)

def _get_popular_searches_impl(session: SQLAlchemySession, limit: int) -> List[Dict[str, Any]]:
    query = text("""
        SELECT job_title, COUNT(*) as count 
        FROM job_searches 
        GROUP BY job_title 
        ORDER BY count DESC 
        LIMIT :limit
    """)
    result = session.execute(query, {"limit": limit})
    return [{"job_title": row[0], "count": row[1]} for row in result]

def get_popular_searches(limit: int = 5) -> List[Dict[str, Any]]:
    return execute_with_retry(_get_popular_searches_impl, limit)

def _get_highest_risk_jobs_impl(session: SQLAlchemySession, limit: int) -> List[Dict[str, Any]]:
    query = text("""
        SELECT job_title, AVG(year_5_risk) as avg_risk 
        FROM job_searches 
        WHERE year_5_risk IS NOT NULL
        GROUP BY job_title 
        HAVING COUNT(*) > 1 
        ORDER BY avg_risk DESC 
        LIMIT :limit
    """)
    result = session.execute(query, {"limit": limit})
    return [{"job_title": row[0], "risk": float(row[1]) if row[1] is not None else 0.0} for row in result]

def get_highest_risk_jobs(limit: int = 5) -> List[Dict[str, Any]]:
    return execute_with_retry(_get_highest_risk_jobs_impl, limit)

def _get_lowest_risk_jobs_impl(session: SQLAlchemySession, limit: int) -> List[Dict[str, Any]]:
    query = text("""
        SELECT job_title, AVG(year_5_risk) as avg_risk 
        FROM job_searches 
        WHERE year_5_risk IS NOT NULL
        GROUP BY job_title 
        HAVING COUNT(*) > 1
        ORDER BY avg_risk ASC 
        LIMIT :limit
    """)
    result = session.execute(query, {"limit": limit})
    return [{"job_title": row[0], "risk": float(row[1]) if row[1] is not None else 0.0} for row in result]

def get_lowest_risk_jobs(limit: int = 5) -> List[Dict[str, Any]]:
    return execute_with_retry(_get_lowest_risk_jobs_impl, limit)

def _get_recent_searches_impl(session: SQLAlchemySession, limit: int) -> List[Dict[str, Any]]:
    recent_searches = session.query(JobSearch).order_by(
        JobSearch.timestamp.desc()
    ).limit(limit).all()
    
    results = []
    for search in recent_searches:
        results.append({
            "job_title": search.job_title,
            "year_1_risk": search.year_1_risk,
            "year_5_risk": search.year_5_risk,
            "timestamp": search.timestamp, # Keep as datetime object for now
            "risk_category": search.risk_category,
            "job_category": search.job_category
        })
    return results

def get_recent_searches(limit: int = 10) -> List[Dict[str, Any]]:
    return execute_with_retry(_get_recent_searches_impl, limit)

# --- New functions for health check and stats ---

def check_database_health(engine_instance: Optional[sqlalchemy.engine.Engine]) -> str:
    """
    Check the health of the database connection.
    """
    if not engine_instance:
        return "Not Configured"

    SessionLocal = sessionmaker(bind=engine_instance)
    session: Optional[SQLAlchemySession] = None
    try:
        session = SessionLocal()
        session.execute(text("SELECT 1"))
        session.commit()                      # ensure transaction is closed
        return "OK"
    except Exception as e:
        logger.error(f"Database health check failed: {e}", exc_info=True)
        if session:
            session.rollback()
        return "Error"
    finally:
        if session:
            session.close()

def get_database_stats(engine_instance: Optional[sqlalchemy.engine.Engine]) -> Dict[str, Any]:
    """
    Get basic statistics from the bls_job_data table.
    """
    if not engine_instance:
        return {}

    SessionLocal = sessionmaker(bind=engine_instance)
    session: Optional[SQLAlchemySession] = None
    try:
        session = SessionLocal()

        soc_count_query = text("SELECT COUNT(DISTINCT occupation_code) FROM bls_job_data")
        total_socs = session.execute(soc_count_query).scalar_one_or_none() or 0

        last_update_query = text("SELECT MAX(last_updated) FROM bls_job_data")
        last_update = session.execute(last_update_query).scalar_one_or_none()

        session.commit()
        return {
            "total_soc_codes": total_socs,
            "latest_update_time": str(last_update) if last_update else "N/A"
        }
    except Exception as e:
        logger.error(f"Failed to get database stats: {e}", exc_info=True)
        if session:
            session.rollback()
        return {
            "total_soc_codes": "Error",
            "latest_update_time": "Error"
        }
    finally:
        if session:
            session.close()

# --- Keep database schema updated ---
# This is called when the module is first imported if engine is successfully created.
# If engine is None (e.g. DATABASE_URL not set or connection failed), this part is skipped.
if engine:
    try:
        logger.info("Verifying 'job_searches' table schema...")
        Base.metadata.create_all(engine, checkfirst=True) # Ensures 'job_searches' table exists
        logger.info("'job_searches' table schema verified/created.")
    except Exception as e:
        logger.error(f"Failed to verify/create 'job_searches' table: {e}", exc_info=True)
        # Potentially set database_available to False here if this is critical
        # For now, we assume other parts might still work or fallback is active.

logger.info(f"database.py loaded. Database available: {engine is not None and Session is not None}")
print(f"database.py loaded. Database available: {engine is not None and Session is not None}")


# ------------------------------------------------------------------
# Back-compat helper
# ------------------------------------------------------------------
# A handful of legacy modules (e.g. older versions of admin dashboards)
# still import `database.get_db_engine()` to obtain the shared SQLAlchemy
# engine.  The current design exposes the singleton as the module-level
# variable `engine`, but to avoid refactoring every caller we provide
# this thin accessor that simply returns that instance (which may be
# `None` if the connection failed or is not configured).
# ------------------------------------------------------------------

def get_db_engine() -> Optional[sqlalchemy.engine.Engine]:
    """Return the singleton SQLAlchemy engine (may be ``None``)."""
    return engine

