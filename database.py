import os
import datetime
import logging
import time
from typing import List, Dict, Any, Optional, Callable
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, MetaData, text
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DisconnectionError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s'
)
logger = logging.getLogger("database")

# Global engine and session factory, initialized to None
engine: Optional[sqlalchemy.engine.Engine] = None
SessionLocal: Optional[sessionmaker] = None 
Base = declarative_base()
metadata = Base.metadata # Use Base.metadata

# Define model for job searches
class JobSearch(Base):
    __tablename__ = 'job_searches'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_title = Column(String(255), nullable=False, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, index=True)
    year_1_risk = Column(Float)
    year_5_risk = Column(Float)
    risk_category = Column(String(50))
    job_category = Column(String(50))
    
    def __repr__(self):
        return f"<JobSearch(job_title='{self.job_title}', risk_category='{self.risk_category}')>"

# Get database URL from environment
database_url = os.environ.get("DATABASE_URL")
if database_url is None:
    try:
        import streamlit as st
        # Check if st.secrets exists and is a dict-like object or a SecretsManager
        if hasattr(st, 'secrets'):
            if isinstance(st.secrets, dict): # For older Streamlit versions or direct dict provision
                database_url = st.secrets.get("database", {}).get("DATABASE_URL")
            elif callable(getattr(st.secrets, "get", None)): # For Streamlit SecretsManager
                 database_url = st.secrets.get("database", {}).get("DATABASE_URL")
        
        if database_url:
            logger.info("Using DATABASE_URL from Streamlit secrets.")
    except (ImportError, AttributeError):
        logger.info("Streamlit or st.secrets not available. DATABASE_URL must be in environment.")
        pass # Streamlit not available or secrets not configured

if database_url is None:
    logger.warning("DATABASE_URL not found in environment or Streamlit secrets. Database functionality will be disabled.")

# Maximum number of connection attempts
MAX_RETRIES = 3 # Reduced for quicker feedback in some environments

# Create database engine with retry logic
def create_db_engine(url: str, max_retries: int = MAX_RETRIES) -> Optional[sqlalchemy.engine.Engine]:
    """
    Create database engine with retry logic and optimized connection pooling.
    """
    retry_count = 0
    last_error: Optional[Exception] = None
    
    if not url: # Handle case where URL might be None or empty
        logger.error("Database URL is not provided. Cannot create engine.")
        return None

    while retry_count < max_retries:
        try:
            # Fix potential issue with URL format
            if url.startswith('postgres://'):
                url = url.replace('postgres://', 'postgresql://', 1)
                logger.info("Converted postgres:// URL to postgresql://")
            
            if url.startswith(('http://', 'https://')): # Should not happen with corrected logic
                parts = url.split('://', 1)
                if len(parts) > 1:
                    url = 'postgresql://' + parts[1]
                    logger.info("Corrected http(s):// URL to postgresql://")

            connect_args = {}
            if 'postgresql' in url: # Specific args for PostgreSQL
                connect_args = {
                    "connect_timeout": 10,
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                    "sslmode": 'require', 
                    "application_name": "AI_Job_Analyzer_App_DB_Module"
                }
            
            engine_instance = create_engine(
                url, 
                connect_args=connect_args,
                poolclass=QueuePool,
                pool_size=3, # Reduced pool size for potentially limited environments
                max_overflow=5,
                pool_timeout=20, # Reduced timeout
                pool_recycle=1800,
                pool_pre_ping=True,
                echo_pool=False 
            )
            
            # Test connection
            with engine_instance.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Successfully connected to database: {url.split('@')[-1] if '@' in url else url}")
            
            # Create tables defined in this Base
            try:
                Base.metadata.create_all(engine_instance)
                logger.info("Database tables (JobSearch) created or verified.")
            except Exception as e_tables:
                logger.error(f"Error creating database tables: {str(e_tables)}", exc_info=True)
                # Proceeding as tables might exist or other operations might still work

            return engine_instance
                
        except Exception as e:
            last_error = e
            retry_count += 1
            wait_time = min(2 ** retry_count + (retry_count * 0.1), 15) # Max wait 15s
            logger.warning(f"Database connection attempt {retry_count}/{max_retries} failed: {str(e)}. Retrying in {wait_time:.1f} seconds...")
            time.sleep(wait_time)
    
    logger.error(f"Failed to connect to database after {max_retries} attempts. Last error: {str(last_error)}", exc_info=True)
    return None

# Initialize global engine and SessionLocal
if database_url:
    engine = create_db_engine(database_url)
    if engine:
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        logger.info("Database engine and SessionLocal initialized.")
    else:
        logger.critical("Database engine could not be initialized. All database functionality will be disabled.")
        # engine is already None from create_db_engine returning None
        SessionLocal = None 
else:
    # This case is already logged by the database_url check above
    pass


def get_session() -> Optional[SQLAlchemySession]:
    """
    Get a database session.
    Returns SQLAlchemy session or None if session factory is not available.
    """
    if SessionLocal is None:
        logger.error("SessionLocal is not initialized. Cannot create database session.")
        return None
    try:
        session = SessionLocal()
        return session
    except Exception as e:
        logger.error(f"Error creating database session: {str(e)}", exc_info=True)
        return None

def execute_with_retry(operation: Callable, *args: Any, max_retries: int = 2, **kwargs: Any) -> Any:
    """
    Execute a database operation with retry logic for specific errors.
    """
    if SessionLocal is None:
        logger.warning(f"Skipping operation '{operation.__name__}': Database not available.")
        return None # Or appropriate default for the operation

    retry_count = 0
    last_error: Optional[Exception] = None
    
    while retry_count < max_retries:
        session: Optional[SQLAlchemySession] = None
        try:
            session = get_session()
            if session is None: # Could not get a session
                return None # Or appropriate default

            result = operation(session, *args, **kwargs)
            session.commit()
            return result
        except (OperationalError, DisconnectionError) as e: # Specific errors to retry
            last_error = e
            retry_count += 1
            logger.warning(f"Database operation '{operation.__name__}' failed (attempt {retry_count}/{max_retries}) due to connection issue: {str(e)}. Retrying...")
            if session:
                try:
                    session.rollback()
                except Exception as rb_exc:
                    logger.error(f"Error during rollback on retry: {rb_exc}", exc_info=True)
            time.sleep(1 * retry_count) # Simple backoff
        except SQLAlchemyError as e: # Other SQLAlchemy errors, don't retry
            last_error = e
            logger.error(f"SQLAlchemyError during '{operation.__name__}': {str(e)}", exc_info=True)
            if session:
                try:
                    session.rollback()
                except Exception as rb_exc:
                    logger.error(f"Error during rollback on SQLAlchemyError: {rb_exc}", exc_info=True)
            break # Exit retry loop
        except Exception as e: # Non-SQLAlchemy errors, don't retry
            last_error = e
            logger.error(f"Unexpected error during '{operation.__name__}': {str(e)}", exc_info=True)
            if session:
                try:
                    session.rollback()
                except Exception as rb_exc:
                    logger.error(f"Error during rollback on general exception: {rb_exc}", exc_info=True)
            break # Exit retry loop
        finally:
            if session:
                try:
                    session.close()
                except Exception as close_exc:
                    logger.error(f"Error closing session for '{operation.__name__}': {close_exc}", exc_info=True)
    
    logger.error(f"Database operation '{operation.__name__}' failed definitively after {retry_count} attempt(s). Last error: {str(last_error)}")
    return None # Or appropriate default for the operation

# Internal operations that expect a session
def _save_job_search_operation(session: SQLAlchemySession, job_title: str, risk_data: Dict[str, Any]) -> bool:
    job_search = JobSearch(
        job_title=job_title,
        year_1_risk=risk_data.get('year_1_risk'),
        year_5_risk=risk_data.get('year_5_risk'),
        risk_category=risk_data.get('risk_category'),
        job_category=risk_data.get('job_category')
    )
    session.add(job_search)
    return True

def _get_popular_searches_operation(session: SQLAlchemySession, limit: int) -> List[Dict[str, Any]]:
    query = text("SELECT job_title, COUNT(*) as count FROM job_searches GROUP BY job_title ORDER BY count DESC LIMIT :limit")
    result = session.execute(query, {"limit": limit})
    return [{"job_title": row[0], "count": row[1]} for row in result]

def _get_highest_risk_jobs_operation(session: SQLAlchemySession, limit: int) -> List[Dict[str, Any]]:
    query = text("SELECT job_title, AVG(year_5_risk) as avg_risk FROM job_searches GROUP BY job_title HAVING COUNT(*) > 1 ORDER BY avg_risk DESC LIMIT :limit")
    result = session.execute(query, {"limit": limit})
    return [{"job_title": row[0], "risk": float(row[1]) if row[1] is not None else 0.0} for row in result]

def _get_lowest_risk_jobs_operation(session: SQLAlchemySession, limit: int) -> List[Dict[str, Any]]:
    query = text("SELECT job_title, AVG(year_5_risk) as avg_risk FROM job_searches GROUP BY job_title HAVING COUNT(*) > 1 ORDER BY avg_risk ASC LIMIT :limit")
    result = session.execute(query, {"limit": limit})
    return [{"job_title": row[0], "risk": float(row[1]) if row[1] is not None else 0.0} for row in result]

def _get_recent_searches_operation(session: SQLAlchemySession, limit: int) -> List[Dict[str, Any]]:
    recent_searches = session.query(JobSearch).order_by(JobSearch.timestamp.desc()).limit(limit).all()
    results = []
    for search in recent_searches:
        results.append({
            "job_title": search.job_title, "year_1_risk": search.year_1_risk,
            "year_5_risk": search.year_5_risk, "risk_category": search.risk_category,
            "timestamp": search.timestamp # Keep as datetime for potential future use
        })
    return results

# Public-facing database functions
def save_job_search(job_title: str, risk_data: Dict[str, Any]) -> bool:
    if SessionLocal is None:
        logger.warning(f"Skipping save_job_search for '{job_title}': Database not available.")
        return False
    return execute_with_retry(_save_job_search_operation, job_title, risk_data) or False

def get_popular_searches(limit: int = 5) -> List[Dict[str, Any]]:
    if SessionLocal is None:
        logger.warning("Skipping get_popular_searches: Database not available.")
        return []
    return execute_with_retry(_get_popular_searches_operation, limit) or []

def get_highest_risk_jobs(limit: int = 5) -> List[Dict[str, Any]]:
    if SessionLocal is None:
        logger.warning("Skipping get_highest_risk_jobs: Database not available.")
        return []
    return execute_with_retry(_get_highest_risk_jobs_operation, limit) or []

def get_lowest_risk_jobs(limit: int = 5) -> List[Dict[str, Any]]:
    if SessionLocal is None:
        logger.warning("Skipping get_lowest_risk_jobs: Database not available.")
        return []
    return execute_with_retry(_get_lowest_risk_jobs_operation, limit) or []

def get_recent_searches(limit: int = 10) -> List[Dict[str, Any]]:
    if SessionLocal is None:
        logger.warning("Skipping get_recent_searches: Database not available.")
        return []
    
    searches = execute_with_retry(_get_recent_searches_operation, limit) or []
    
    # Format timestamp for display after fetching
    results_formatted = []
    for search in searches:
        formatted_search = search.copy()
        timestamp = search.get("timestamp")
        if timestamp:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            # Ensure timestamp is offset-aware for correct comparison
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
            
            delta = now_utc - timestamp
            if delta.days > 0: time_ago = f"{delta.days} days ago"
            elif delta.seconds // 3600 > 0: time_ago = f"{delta.seconds // 3600} hours ago"
            elif delta.seconds // 60 > 0: time_ago = f"{delta.seconds // 60} minutes ago"
            else: time_ago = "Just now"
            formatted_search["time_ago"] = time_ago
        else:
            formatted_search["time_ago"] = "Recently"
        results_formatted.append(formatted_search)
    return results_formatted

def check_database_health() -> Dict[str, Any]:
    """
    Performs a basic health check on the database.
    Returns a dictionary with 'status' and 'message'.
    """
    if engine is None or SessionLocal is None:
        return {"status": "error", "message": "Database engine or session factory not initialized."}
    
    session = get_session()
    if session is None:
        return {"status": "error", "message": "Failed to create a database session."}
        
    try:
        session.execute(text("SELECT 1"))
        return {"status": "ok", "message": "Database connection successful."}
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}", exc_info=True)
        return {"status": "error", "message": f"Database query failed: {str(e)}"}
    finally:
        if session:
            session.close()

def get_database_stats() -> Dict[str, Any]:
    """
    Retrieves basic statistics from the database.
    """
    if SessionLocal is None:
        logger.warning("Skipping get_database_stats: Database not available.")
        return {"total_searches": 0, "unique_jobs": 0}

    session = get_session()
    if session is None:
        return {"total_searches": 0, "unique_jobs": 0}
        
    try:
        total_searches = session.query(JobSearch).count()
        unique_jobs = session.query(JobSearch.job_title).distinct().count()
        return {"total_searches": total_searches, "unique_jobs": unique_jobs}
    except Exception as e:
        logger.error(f"Error getting database stats: {str(e)}", exc_info=True)
        return {"total_searches": 0, "unique_jobs": 0}
    finally:
        if session:
            session.close()

# Example usage (optional, for direct script execution testing)
if __name__ == "__main__":
    logger.info("Running database.py directly for testing.")
    
    if engine:
        logger.info("Engine created. Attempting to fetch popular searches.")
        popular = get_popular_searches()
        if popular:
            logger.info(f"Popular searches: {popular}")
        else:
            logger.info("No popular searches found or DB error.")
        
        health = check_database_health()
        logger.info(f"Database health: {health}")
        
        stats = get_database_stats()
        logger.info(f"Database stats: {stats}")
    else:
        logger.error("Database engine is None. Cannot perform tests.")

