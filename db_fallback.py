"""
Fallback functionality when database is not available.
This module provides stub functions that can be used instead of actual database functions.
"""

def save_job_search(job_title, risk_data):
    """
    Stub function for saving job search data when DB is not available
    """
    print(f"Would save job search for '{job_title}' if database was available")
    return True

def get_popular_searches(limit=5):
    """
    Return sample popular searches when DB is not available
    """
    return [
        {"job_title": "Software Engineer", "count": 42},
        {"job_title": "Data Scientist", "count": 38},
        {"job_title": "Nurse", "count": 27},
        {"job_title": "Teacher", "count": 21},
        {"job_title": "Truck Driver", "count": 19}
    ][:limit]

def get_highest_risk_jobs(limit=5):
    """
    Return sample highest risk jobs when DB is not available
    """
    return [
        {"job_title": "Data Entry Clerk", "avg_risk": 85.2},
        {"job_title": "Customer Service Representative", "avg_risk": 79.8},
        {"job_title": "Cashier", "avg_risk": 78.6},
        {"job_title": "Bookkeeper", "avg_risk": 75.3},
        {"job_title": "Truck Driver", "avg_risk": 73.7}
    ][:limit]

def get_lowest_risk_jobs(limit=5):
    """
    Return sample lowest risk jobs when DB is not available
    """
    return [
        {"job_title": "Therapist", "avg_risk": 8.5},
        {"job_title": "Healthcare Manager", "avg_risk": 12.3},
        {"job_title": "Human Resources Director", "avg_risk": 14.8},
        {"job_title": "Teacher", "avg_risk": 18.9},
        {"job_title": "Social Worker", "avg_risk": 21.4}
    ][:limit]

def get_recent_searches(limit=10):
    """
    Return sample recent searches when DB is not available
    """
    import datetime
    
    now = datetime.datetime.now()
    
    return [
        {
            "job_title": "Software Engineer",
            "year_1_risk": 32.5,
            "year_5_risk": 48.7,
            "timestamp": now - datetime.timedelta(minutes=5),
            "risk_category": "Moderate"
        },
        {
            "job_title": "Data Scientist",
            "year_1_risk": 27.3,
            "year_5_risk": 42.1,
            "timestamp": now - datetime.timedelta(minutes=12),
            "risk_category": "Moderate"
        },
        {
            "job_title": "Teacher",
            "year_1_risk": 18.9,
            "year_5_risk": 35.6,
            "timestamp": now - datetime.timedelta(minutes=18),
            "risk_category": "Moderate"
        },
        {
            "job_title": "Nurse",
            "year_1_risk": 10.2,
            "year_5_risk": 24.8,
            "timestamp": now - datetime.timedelta(minutes=25),
            "risk_category": "Low"
        },
        {
            "job_title": "Truck Driver",
            "year_1_risk": 65.3,
            "year_5_risk": 82.7,
            "timestamp": now - datetime.timedelta(minutes=37),
            "risk_category": "High"
        }
    ][:limit]