import os
import datetime
import logging
import time
import json
from typing import List, Dict, Any, Optional, Union
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Table, MetaData, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DisconnectionError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("database")

# Get database URL from environment
database_url = os.environ.get("DATABASE_URL")
if database_url is None:
    # Check if database URL is in secrets.toml
    try:
        import streamlit as st
        database_url = st.secrets.get("database", {}).get("DATABASE_URL")
        if database_url:
            logger.info("Using DATABASE_URL from secrets.toml")
    except:
        pass

if database_url is None:
    # Instead of raising an error, just use a dummy URL for SQLite
    database_url = "sqlite:///:memory:"
    logger.warning("No DATABASE_URL found, using in-memory SQLite database")

# Fix potential issue with URL format
if database_url:
    # First handle postgres:// format 
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://')
        logger.info("Converted postgres:// URL to postgresql://")
    
    # Handle invalid protocol in URL
    if database_url.startswith(('https://', 'http://')):
        # Extract the part after the protocol
        parts = database_url.split('://', 1)
        if len(parts) > 1:
            # Replace with postgresql://
            database_url = 'postgresql://' + parts[1]
            logger.info("Converted http(s):// URL to postgresql://")

# Maximum number of connection attempts
MAX_RETRIES = 5

# Create database engine with retry logic
def create_db_engine(url: str, max_retries: int = MAX_RETRIES):
    """
    Create database engine with retry logic and optimized connection pooling
    
    Args:
        url: Database connection URL
        max_retries: Maximum number of connection attempts
        
    Returns:
        SQLAlchemy engine or None if connection fails
    """
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            # Enhanced connection settings for Neon PostgreSQL
            connect_args = {}
            if 'postgresql' in url:
                connect_args = {
                    "connect_timeout": 15,             # 15 seconds timeout
                    "keepalives": 1,                   # Enable keepalives
                    "keepalives_idle": 30,             # Seconds before sending keepalive
                    "keepalives_interval": 10,         # Seconds between keepalives 
                    "keepalives_count": 5,             # Max number of keepalive retries
                    "sslmode": 'require',              # Force SSL connection
                    "application_name": "AI_Job_Analyzer"  # Identify app in database logs
                }
            
            # Create engine with optimized connection pooling for better reliability
            engine = create_engine(
                url, 
                connect_args=connect_args,
                poolclass=QueuePool,          # Explicit QueuePool for better control
                pool_size=5,                  # Connection pool size
                max_overflow=10,              # Max extra connections
                pool_timeout=30,              # Connection timeout
                pool_recycle=1800,            # Recycle connections after 30 min
                pool_pre_ping=True,           # Check connection validity before use
                echo_pool=False               # Don't log all pool events
            )
            
            # Test connection with a quick timeout
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("Successfully connected to database")
                
                # Record successful connection in a status file
                try:
                    with open("db_connection_status.json", "w") as f:
                        json.dump({
                            "status": "connected",
                            "last_connection": datetime.datetime.now().isoformat(),
                            "connection_attempts": retry_count + 1
                        }, f)
                except:
                    pass
                
                return engine
                
        except Exception as e:
            last_error = e
            retry_count += 1
            
            # Exponential backoff with jitter
            wait_time = min(2 ** retry_count + (retry_count * 0.1), 30)
            logger.warning(f"Database connection attempt {retry_count} failed: {str(e)}. Retrying in {wait_time:.1f} seconds...")
            
            try:
                # Record failed connection attempt
                with open("db_connection_status.json", "w") as f:
                    json.dump({
                        "status": "error",
                        "last_error": str(e),
                        "last_attempt": datetime.datetime.now().isoformat(),
                        "connection_attempts": retry_count
                    }, f)
            except:
                pass
                
            time.sleep(wait_time)
    
    # All retries failed
    logger.error(f"Failed to connect to database after {max_retries} attempts. Last error: {str(last_error)}")
    return None

try:
    # Create database engine with retry logic
    engine = create_db_engine(database_url)
    
    if engine is None:
        logger.error("Could not establish database connection - using fallback")
        # Import fallback functions
        from db_fallback import save_job_search, get_popular_searches, get_highest_risk_jobs, get_lowest_risk_jobs, get_recent_searches
        
        # Exit the module with fallback functions defined
        import sys
        sys.modules[__name__].__dict__.update(locals())
        exit()
        
except Exception as e:
    logger.error(f"Error setting up database connection: {str(e)}")
    # Import fallback functions
    from db_fallback import save_job_search, get_popular_searches, get_highest_risk_jobs, get_lowest_risk_jobs, get_recent_searches
    
    # Exit the module with fallback functions defined
    import sys
    sys.modules[__name__].__dict__.update(locals())
    exit()
    
# Database connection successful - create tables and set up ORM
Base = declarative_base()

# Define model for job searches
class JobSearch(Base):
    __tablename__ = 'job_searches'
    
    id = Column(Integer, primary_key=True)
    job_title = Column(String(255), nullable=False, index=True)  # Added index for faster queries
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, index=True)  # Added index for faster queries
    year_1_risk = Column(Float)
    year_5_risk = Column(Float)
    risk_category = Column(String(50))
    job_category = Column(String(50))
    
    def __repr__(self):
        return f"<JobSearch(job_title='{self.job_title}', risk_category='{self.risk_category}')>"

# Create tables if they don't exist
try:
    Base.metadata.create_all(engine)
    logger.info("Database tables created or verified")
except Exception as e:
    logger.error(f"Error creating database tables: {str(e)}")

# Create session factory with retry logic
def get_session():
    """
    Get a database session with retry logic
    
    Returns:
        SQLAlchemy session or None if session creation fails
    """
    if engine is None:
        logger.error("Cannot create session - database engine not available")
        return None
        
    try:
        Session = sessionmaker(bind=engine)
        return Session()
    except Exception as e:
        logger.error(f"Error creating database session: {str(e)}")
        return None

# Function to execute database operations with retry logic
def execute_with_retry(operation, *args, max_retries=3, **kwargs):
    """
    Execute a database operation with retry logic
    
    Args:
        operation: Function to execute
        *args: Arguments to pass to the function
        max_retries: Maximum number of retry attempts
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        Result of the operation or None if all retries fail
    """
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            session = get_session()
            if session is None:
                return None
                
            result = operation(session, *args, **kwargs)
            return result
        except (OperationalError, DisconnectionError) as e:
            # Database connection issues - retry
            last_error = e
            retry_count += 1
            
            # Close and dispose the session
            if session:
                try:
                    session.close()
                except:
                    pass
            
            # Exponential backoff
            wait_time = min(2 ** retry_count, 8)
            logger.warning(f"Database operation failed (attempt {retry_count}): {str(e)}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        except Exception as e:
            # Other errors - log and return None
            logger.error(f"Database operation error: {str(e)}")
            if session:
                try:
                    session.close()
                except:
                    pass
            return None
    
    logger.error(f"Database operation failed after {max_retries} attempts. Last error: {str(last_error)}")
    return None

# Database operations
def _save_job_search_operation(session, job_title, risk_data):
    """Internal function for save_job_search with session parameter"""
    try:
        job_search = JobSearch(
            job_title=job_title,
            year_1_risk=risk_data.get('year_1_risk'),
            year_5_risk=risk_data.get('year_5_risk'),
            risk_category=risk_data.get('risk_category'),
            job_category=risk_data.get('job_category')
        )
        session.add(job_search)
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        raise e

def save_job_search(job_title: str, risk_data: Dict[str, Any]) -> bool:
    """
    Save job search data to database with retry logic
    
    Args:
        job_title: Job title that was searched
        risk_data: Dictionary containing risk assessment data
        
    Returns:
        Boolean indicating success
    """
    result = execute_with_retry(_save_job_search_operation, job_title, risk_data)
    return result is True

def _get_popular_searches_operation(session, limit):
    """Internal function for get_popular_searches with session parameter"""
    # SQL to count job titles and order by count
    query = text("""
        SELECT job_title, COUNT(*) as count 
        FROM job_searches 
        GROUP BY job_title 
        ORDER BY count DESC 
        LIMIT :limit
    """)
    
    result = session.execute(query, {"limit": limit})
    
    # Convert result to list of dictionaries
    popular_searches = [{"job_title": row[0], "count": row[1]} for row in result]
    session.close()
    return popular_searches

def get_popular_searches(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Get most popular job searches
    
    Args:
        limit: Maximum number of results to return
        
    Returns:
        List of dictionaries with job titles and search counts
    """
    result = execute_with_retry(_get_popular_searches_operation, limit)
    return result or []

def _get_highest_risk_jobs_operation(session, limit):
    """Internal function for get_highest_risk_jobs with session parameter"""
    # SQL to get highest risk jobs
    query = text("""
        SELECT job_title, AVG(year_5_risk) as avg_risk 
        FROM job_searches 
        GROUP BY job_title 
        HAVING COUNT(*) > 1
        ORDER BY avg_risk DESC 
        LIMIT :limit
    """)
    
    result = session.execute(query, {"limit": limit})
    
    # Convert result to list of dictionaries
    high_risk_jobs = [{"job_title": row[0], "risk": float(row[1])} for row in result]
    session.close()
    return high_risk_jobs

def get_highest_risk_jobs(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Get jobs with highest average year 5 risk
    
    Args:
        limit: Maximum number of results to return
        
    Returns:
        List of dictionaries with job titles and risk scores
    """
    result = execute_with_retry(_get_highest_risk_jobs_operation, limit)
    return result or []

def _get_lowest_risk_jobs_operation(session, limit):
    """Internal function for get_lowest_risk_jobs with session parameter"""
    # SQL to get lowest risk jobs
    query = text("""
        SELECT job_title, AVG(year_5_risk) as avg_risk 
        FROM job_searches 
        GROUP BY job_title 
        HAVING COUNT(*) > 1
        ORDER BY avg_risk ASC 
        LIMIT :limit
    """)
    
    result = session.execute(query, {"limit": limit})
    
    # Convert result to list of dictionaries
    low_risk_jobs = [{"job_title": row[0], "risk": float(row[1])} for row in result]
    session.close()
    return low_risk_jobs

def get_lowest_risk_jobs(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Get jobs with lowest average year 5 risk
    
    Args:
        limit: Maximum number of results to return
        
    Returns:
        List of dictionaries with job titles and risk scores
    """
    result = execute_with_retry(_get_lowest_risk_jobs_operation, limit)
    return result or []

def _get_recent_searches_operation(session, limit):
    """Internal function for get_recent_searches with session parameter"""
    # Query recent searches
    recent_searches = session.query(JobSearch).order_by(
        JobSearch.timestamp.desc()
    ).limit(limit).all()
    
    # Convert to list of dictionaries
    results = []
    for search in recent_searches:
        results.append({
            "job_title": search.job_title,
            "year_1_risk": search.year_1_risk,
            "year_5_risk": search.year_5_risk,
            "risk_category": search.risk_category,
            "timestamp": search.timestamp
        })
    
    session.close()
    return results

def get_recent_searches(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get recent job searches
    
    Args:
        limit: Maximum number of results to return
        
    Returns:
        List of dictionaries with recent job search data
    """
    result = execute_with_retry(_get_recent_searches_operation, limit)
    return result or []

def check_database_health() -> Dict[str, Any]:
    """
    Check database health and connectivity
    
    Returns:
        Dictionary with health status information
    """
    try:
        start_time = time.time()
        session = get_session()
        
        if session is None:
            return {
                "status": "error",
                "message": "Could not create database session",
                "timestamp": datetime.datetime.now().isoformat()
            }
        
        # Try a simple query
        result = session.execute(text("SELECT 1")).fetchone()
        session.close()
        
        # Calculate query time
        query_time = time.time() - start_time
        
        if result and result[0] == 1:
            return {
                "status": "healthy",
                "message": "Database connection successful",
                "response_time_ms": round(query_time * 1000, 2),
                "timestamp": datetime.datetime.now().isoformat()
            }
        else:
            return {
                "status": "error",
                "message": "Database query returned unexpected result",
                "timestamp": datetime.datetime.now().isoformat()
            }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Database health check failed: {str(e)}",
            "timestamp": datetime.datetime.now().isoformat()
        }

def get_database_stats() -> Dict[str, Any]:
    """
    Get database statistics
    
    Returns:
        Dictionary with database statistics
    """
    try:
        session = get_session()
        if session is None:
            return {"error": "Could not create database session"}
            
        # Get total job searches
        total_searches = session.query(JobSearch).count()
        
        # Get unique job titles
        unique_jobs_query = text("SELECT COUNT(DISTINCT job_title) FROM job_searches")
        unique_jobs = session.execute(unique_jobs_query).scalar()
        
        # Get average risk by category
        avg_risk_query = text("""
            SELECT job_category, AVG(year_5_risk) as avg_risk
            FROM job_searches
            WHERE job_category IS NOT NULL
            GROUP BY job_category
            ORDER BY avg_risk DESC
        """)
        risk_by_category = [
            {"category": row[0], "avg_risk": float(row[1])}
            for row in session.execute(avg_risk_query)
        ]
        
        # Get recent activity
        recent_activity_query = text("""
            SELECT DATE(timestamp) as date, COUNT(*) as searches
            FROM job_searches
            WHERE timestamp > CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
        """)
        
        try:
            recent_activity = [
                {"date": row[0].isoformat(), "searches": row[1]}
                for row in session.execute(recent_activity_query)
            ]
        except:
            # Fallback for SQLite or other databases that don't support the interval syntax
            recent_activity = []
        
        session.close()
        
        return {
            "total_searches": total_searches,
            "unique_job_titles": unique_jobs,
            "risk_by_category": risk_by_category,
            "recent_activity": recent_activity,
            "timestamp": datetime.datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting database stats: {str(e)}")
        return {"error": f"Failed to retrieve database statistics: {str(e)}"}
