import os
import datetime
import sqlalchemy
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from typing import List, Dict, Any, Optional, Callable
import logging
import threading # For _engine_lock

# Initialize Session (the sessionmaker factory) to None at the very top
Session: Optional[sessionmaker] = None

# Base must be defined before models that inherit from it
Base = declarative_base()
metadata = Base.metadata

# Configure logging (must be defined before the main try block to be available in its except clause)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define model for job searches (must be after Base and before main try block)
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

# Global engine instance, initialized to None
engine: Optional[sqlalchemy.engine.Engine] = None

try:
    # Imports specific to the database operations and setup
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session as SQLAlchemySession # Type alias for session instances
    from sqlalchemy.pool import QueuePool
    from sqlalchemy.exc import SQLAlchemyError, OperationalError, DisconnectionError
    import time

    _engine_lock = threading.Lock()

    def create_db_engine(url: str, max_retries: int = 3) -> Optional[sqlalchemy.engine.Engine]:
        """
        Create database engine with retry logic and optimized connection pooling.
        """
        logger.info(f"Attempting to create database engine with URL: {url}")
        print(f"Attempting to create database engine with URL: {url}") # Diagnostic print
        retry_count = 0
        last_error: Optional[Exception] = None
        
        if not url:
            logger.error("Database URL is not provided. Cannot create engine.")
            print("Database URL is not provided. Cannot create engine.") # Diagnostic print
            return None

        while retry_count < max_retries:
            try:
                if url.startswith('postgres://'):
                    url = url.replace('postgres://', 'postgresql://', 1)
                    logger.info("Converted postgres:// URL to postgresql://")
                    print("Converted postgres:// URL to postgresql://") # Diagnostic print
                
                if url.startswith(('http://', 'https://')):
                    parts = url.split('://', 1)
                    if len(parts) > 1:
                        url = 'postgresql://' + parts[1]
                        logger.info("Corrected http(s):// URL to postgresql://")
                        print("Corrected http(s):// URL to postgresql://") # Diagnostic print

                connect_args = {}
                if 'postgresql' in url:
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
                    pool_size=3,
                    max_overflow=5,
                    pool_timeout=20,
                    pool_recycle=1800,
                    pool_pre_ping=True,
                    echo_pool=False 
                )
                
                with engine_instance.connect() as conn:
                    conn.execute(text("SELECT 1"))
                db_host_info = url.split('@')[-1] if '@' in url else url
                logger.info(f"Successfully connected to database: {db_host_info}")
                print(f"Successfully connected to database: {db_host_info}") # Diagnostic print
                return engine_instance
                    
            except Exception as e:
                last_error = e
                retry_count += 1
                wait_time = min(2 ** retry_count + (retry_count * 0.1), 15)
                logger.warning(f"Database connection attempt {retry_count}/{max_retries} failed: {str(e)}. Retrying in {wait_time:.1f} seconds...")
                print(f"Database connection attempt {retry_count}/{max_retries} failed: {str(e)}. Retrying in {wait_time:.1f} seconds...") # Diagnostic print
                time.sleep(wait_time)
        
        logger.error(f"Failed to connect to database after {max_retries} attempts. Last error: {str(last_error)}", exc_info=True)
        print(f"Failed to connect to database after {max_retries} attempts. Last error: {str(last_error)}") # Diagnostic print
        return None

    # Attempt to initialize database globals
    database_url_env = os.environ.get("DATABASE_URL")
    if database_url_env is None:
        try:
            import streamlit as st # Local import
            if hasattr(st, 'secrets'):
                if isinstance(st.secrets, dict): 
                    database_url_env = st.secrets.get("database", {}).get("DATABASE_URL")
                elif callable(getattr(st.secrets, "get", None)): 
                     database_url_env = st.secrets.get("database", {}).get("DATABASE_URL")

            if database_url_env:
                logger.info("Using DATABASE_URL from Streamlit secrets.")
                print("Using DATABASE_URL from Streamlit secrets.")
        except (ImportError, AttributeError) as e:
            logger.info(f"Streamlit secrets not available or error accessing them: {e}. DATABASE_URL must be in environment.")
            print(f"Streamlit secrets not available or error accessing them: {e}. DATABASE_URL must be in environment.")
            pass

    if database_url_env:
        logger.info("Attempting to initialize database engine and session factory.")
        print("Attempting to initialize database engine and session factory.")
        engine = create_db_engine(database_url_env) # Assigns to global engine
        if engine:
            Session = sessionmaker(autocommit=False, autoflush=False, bind=engine) # Assigns to global Session (sessionmaker)
            logger.info("Database engine and Session factory (sessionmaker) initialized.")
            print("Database engine and Session factory (sessionmaker) initialized.")
            try:
                Base.metadata.create_all(engine) 
                logger.info("Database tables (JobSearch) created or verified successfully.")
                print("Database tables (JobSearch) created or verified successfully.")
            except Exception as e_tables:
                logger.error(f"Error creating database tables: {str(e_tables)}", exc_info=True)
                print(f"Error creating database tables: {str(e_tables)}")
                engine = None # Nullify on critical error
                Session = None # Nullify on critical error
        else:
            logger.critical("Database engine could not be initialized. All database functionality will be disabled.")
            print("Database engine could not be initialized. All database functionality will be disabled.")
            engine = None 
            Session = None 
    else:
        logger.warning("DATABASE_URL not found. Database functionality will be disabled.")
        print("DATABASE_URL not found. Database functionality will be disabled.")
        engine = None
        Session = None

    def _create_session() -> Optional[SQLAlchemySession]:
        if Session is None: 
            logger.error("Session factory (sessionmaker) is not initialized. Cannot create database session.")
            print("Session factory (sessionmaker) is not initialized. Cannot create database session.")
            return None
        try:
            session = Session() 
            return session
        except Exception as e:
            logger.error(f"Error creating database session instance: {str(e)}", exc_info=True)
            print(f"Error creating database session instance: {str(e)}")
            return None

    def execute_with_retry(operation: Callable, *args: Any, max_retries: int = 2, **kwargs: Any) -> Any:
        if Session is None: 
            logger.warning(f"Skipping operation '{operation.__name__}': Database not available (Session factory is None).")
            print(f"Skipping operation '{operation.__name__}': Database not available (Session factory is None).")
            return None

        retry_count = 0
        last_error: Optional[Exception] = None
        
        while retry_count < max_retries:
            session: Optional[SQLAlchemySession] = None
            try:
                session = _create_session()
                if session is None:
                    logger.error(f"Could not get a session for operation '{operation.__name__}'.")
                    print(f"Could not get a session for operation '{operation.__name__}'.")
                    return None 

                result = operation(session, *args, **kwargs)
                session.commit()
                return result
            except (OperationalError, DisconnectionError) as e:
                last_error = e
                retry_count += 1
                logger.warning(f"Database operation '{operation.__name__}' failed (attempt {retry_count}/{max_retries}) due to connection issue: {str(e)}. Retrying in {1 * retry_count:.1f} seconds...")
                print(f"Database operation '{operation.__name__}' failed (attempt {retry_count}/{max_retries}) due to connection issue: {str(e)}. Retrying in {1 * retry_count:.1f} seconds...")
                if session:
                    try:
                        session.rollback()
                    except Exception as rb_exc:
                        logger.error(f"Error during rollback on retry: {rb_exc}", exc_info=True)
                        print(f"Error during rollback on retry: {rb_exc}")
                time.sleep(1 * retry_count)
            except SQLAlchemyError as e: 
                last_error = e
                logger.error(f"SQLAlchemyError during '{operation.__name__}': {str(e)}", exc_info=True)
                print(f"SQLAlchemyError during '{operation.__name__}': {str(e)}")
                if session:
                    try:
                        session.rollback()
                    except Exception as rb_exc:
                        logger.error(f"Error during rollback on SQLAlchemyError: {rb_exc}", exc_info=True)
                        print(f"Error during rollback on SQLAlchemyError: {rb_exc}")
                break 
            except Exception as e: 
                last_error = e
                logger.error(f"Unexpected error during '{operation.__name__}': {str(e)}", exc_info=True)
                print(f"Unexpected error during '{operation.__name__}': {str(e)}")
                if session:
                    try:
                        session.rollback()
                    except Exception as rb_exc:
                        logger.error(f"Error during rollback on general exception: {rb_exc}", exc_info=True)
                        print(f"Error during rollback on general exception: {rb_exc}")
                break 
            finally:
                if session:
                    try:
                        session.close()
                    except Exception as close_exc:
                        logger.error(f"Error closing session for '{operation.__name__}': {close_exc}", exc_info=True)
                        print(f"Error closing session for '{operation.__name__}': {close_exc}")
        
        logger.error(f"Database operation '{operation.__name__}' failed definitively after {retry_count} attempt(s). Last error: {str(last_error)}")
        print(f"Database operation '{operation.__name__}' failed definitively after {retry_count} attempt(s). Last error: {str(last_error)}")
        return None

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
                "timestamp": search.timestamp 
            })
        return results

    def save_job_search(job_title: str, risk_data: Dict[str, Any]) -> bool:
        if Session is None: 
            logger.warning(f"Skipping save_job_search for '{job_title}': Database not available.")
            print(f"Skipping save_job_search for '{job_title}': Database not available.")
            return False
        result = execute_with_retry(_save_job_search_operation, job_title, risk_data)
        return result if result is not None else False

    def get_popular_searches(limit: int = 5) -> List[Dict[str, Any]]:
        if Session is None: 
            logger.warning("Skipping get_popular_searches: Database not available.")
            print("Skipping get_popular_searches: Database not available.")
            return []
        return execute_with_retry(_get_popular_searches_operation, limit) or []

    def get_highest_risk_jobs(limit: int = 5) -> List[Dict[str, Any]]:
        if Session is None: 
            logger.warning("Skipping get_highest_risk_jobs: Database not available.")
            print("Skipping get_highest_risk_jobs: Database not available.")
            return []
        return execute_with_retry(_get_highest_risk_jobs_operation, limit) or []

    def get_lowest_risk_jobs(limit: int = 5) -> List[Dict[str, Any]]:
        if Session is None: 
            logger.warning("Skipping get_lowest_risk_jobs: Database not available.")
            print("Skipping get_lowest_risk_jobs: Database not available.")
            return []
        return execute_with_retry(_get_lowest_risk_jobs_operation, limit) or []

    def get_recent_searches(limit: int = 10) -> List[Dict[str, Any]]:
        if Session is None: 
            logger.warning("Skipping get_recent_searches: Database not available.")
            print("Skipping get_recent_searches: Database not available.")
            return []
        return execute_with_retry(_get_recent_searches_operation, limit) or []

    def check_database_health() -> Dict[str, Any]:
        if engine is None or Session is None:
            logger.warning("Database health check: Engine or Session factory not initialized.")
            print("Database health check: Engine or Session factory not initialized.")
            return {"status": "error", "message": "Database not configured"}

        session: Optional[SQLAlchemySession] = None
        try:
            session = _create_session()
            if session is None:
                return {"status": "error", "message": "Failed to create a database session"}
            
            result = session.execute(text("SELECT 1")).scalar_one()
            if result == 1:
                logger.info("Database health check: OK")
                print("Database health check: OK")
                return {"status": "ok", "message": "Database connection successful"}
            else:
                logger.warning("Database health check: Unexpected result from SELECT 1.")
                print("Database health check: Unexpected result from SELECT 1.")
                return {"status": "error", "message": "Unexpected result from health check query"}
        except Exception as e:
            logger.error(f"Database health check failed: {str(e)}", exc_info=True)
            print(f"Database health check failed: {str(e)}")
            return {"status": "error", "message": f"Database connection failed: {str(e)}"}
        finally:
            if session:
                session.close()

    def get_database_stats() -> Dict[str, Any]:
        if Session is None: 
            logger.warning("Skipping get_database_stats: Database not available.")
            print("Skipping get_database_stats: Database not available.")
            return {"total_searches": 0}

        session: Optional[SQLAlchemySession] = None
        try:
            session = _create_session()
            if session is None:
                return {"total_searches": 0}
            
            total_searches = session.query(JobSearch).count()
            return {"total_searches": total_searches}
        except Exception as e:
            logger.error(f"Error getting database stats: {str(e)}", exc_info=True)
            print(f"Error getting database stats: {str(e)}")
            return {"total_searches": 0}
        finally:
            if session:
                session.close()
    
    if __name__ == "__main__":
        print("Running database module directly for testing...")
        
        if engine and Session:
            print("Engine and Session factory appear to be initialized.")
            
            test_risk_data = {
                'year_1_risk': 10.0, 'year_5_risk': 20.0,
                'risk_category': 'Low', 'job_category': 'Test'
            }
            if save_job_search("Test Job", test_risk_data):
                print("Saved test job search successfully.")
            else:
                print("Failed to save test job search.")

            popular = get_popular_searches()
            print(f"Popular searches: {popular}")

            recent = get_recent_searches()
            print(f"Recent searches: {recent}")

            health = check_database_health()
            print(f"Database health: {health}")

            stats = get_database_stats()
            print(f"Database stats: {stats}")
        else:
            print("Database engine or Session factory not initialized. Cannot run tests.")

except Exception as e_module_load:
    logger.critical(f"CRITICAL ERROR during database.py module execution: {e_module_load}", exc_info=True)
    print(f"CRITICAL ERROR during database.py module execution: {e_module_load}")
    engine = None 
    Session = None 
