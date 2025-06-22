"""
Job Comparison Module
This module provides the logic for comparing job data using real BLS data.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import job_api_integration_database_only as job_api_integration

def get_job_comparison_data(jobs_list):
    """
    Get comparison data for multiple jobs using ONLY database/BLS data.
    """
    try:
        return job_api_integration.get_jobs_comparison_data(jobs_list)
    except Exception as e:
        print(f"Error in job comparison: {e}")
        return {job: {"error": f"Data unavailable for {job}"} for job in jobs_list}

def create_comparison_chart(comparison_data):
    """
    Create a comparison chart using real BLS data only.
    """
    if not comparison_data:
        return None
    
    # Handle both dictionary and list formats
    if isinstance(comparison_data, list):
        # Convert list to dictionary format
        valid_jobs = {}
        for item in comparison_data:
            if isinstance(item, dict) and "job_title" in item:
                valid_jobs[item["job_title"]] = item
    else:
        # Filter out jobs with errors
        valid_jobs = {k: v for k, v in comparison_data.items() if "error" not in v}
    
    if not valid_jobs:
        return None
    
    # Prepare data for plotting
    jobs = list(valid_jobs.keys())
    year_1_risks = [valid_jobs[job].get('year_1_risk', 0) or 0 for job in jobs]
    year_5_risks = [valid_jobs[job].get('year_5_risk', 0) or 0 for job in jobs]
    
    # Ensure we have valid data
    if not jobs or all(risk == 0 for risk in year_1_risks + year_5_risks):
        return None
    
    # Create the comparison chart
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='1 Year Risk',
        x=jobs,
        y=year_1_risks,
        marker_color='lightblue'
    ))
    
    fig.add_trace(go.Bar(
        name='5 Year Risk', 
        x=jobs,
        y=year_5_risks,
        marker_color='darkblue'
    ))
    
    fig.update_layout(
        title='AI Displacement Risk Comparison',
        xaxis_title='Jobs',
        yaxis_title='Risk Percentage (%)',
        barmode='group',
        height=400
    )
    
    return fig

def create_employment_comparison(comparison_data):
    """
    Create employment data comparison using real BLS data.
    """
    if not comparison_data:
        return None
    
    # Handle both dictionary and list formats
    if isinstance(comparison_data, list):
        # Convert list to dictionary format
        valid_jobs = {}
        for item in comparison_data:
            if isinstance(item, dict) and "job_title" in item:
                valid_jobs[item["job_title"]] = item
    else:
        valid_jobs = {k: v for k, v in comparison_data.items() if "error" not in v}
    
    if not valid_jobs:
        return None
    
    jobs = list(valid_jobs.keys())
    employment = [valid_jobs[job].get('current_employment') or 0 for job in jobs]
    growth = [valid_jobs[job].get('projected_growth') or valid_jobs[job].get('percent_change') or 0 for job in jobs]
    
    # Create employment comparison chart
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Current Employment',
        x=jobs,
        y=employment,
        yaxis='y',
        marker_color='green'
    ))
    
    fig.add_trace(go.Scatter(
        name='Projected Growth (%)',
        x=jobs,
        y=growth,
        yaxis='y2',
        mode='markers+lines',
        marker_color='red',
        marker_size=10
    ))
    
    fig.update_layout(
        title='Employment Data Comparison',
        xaxis_title='Jobs',
        yaxis=dict(title='Current Employment', side='left'),
        yaxis2=dict(title='Growth Rate (%)', side='right', overlaying='y'),
        height=400
    )
    
    return fig

def get_job_data(job_title):
    """Get job data using database-only approach."""
    try:
        return job_api_integration.get_job_data(job_title)
    except Exception as e:
        print(f"Error getting job data: {e}")
        return {"error": f"Data unavailable for {job_title}"}

def create_comparison_table(comparison_data):
    """Create a comparison table from job data."""
    if not comparison_data:
        return None
    
    # Handle both dictionary and list formats
    if isinstance(comparison_data, list):
        # Convert list to dictionary format
        valid_jobs = {}
        for item in comparison_data:
            if isinstance(item, dict) and "job_title" in item:
                valid_jobs[item["job_title"]] = item
    else:
        valid_jobs = {k: v for k, v in comparison_data.items() if "error" not in v}
    if not valid_jobs:
        return None
    
    df_data = []
    for job, data in valid_jobs.items():
        # Handle None values safely
        current_emp = data.get('current_employment') or 0
        growth_rate = data.get('projected_growth') or data.get('percent_change') or 0
        wage = data.get('median_wage') or 0
        
        df_data.append({
            "Job Title": job,
            "1-Year Risk (%)": data.get("year_1_risk", 0),
            "5-Year Risk (%)": data.get("year_5_risk", 0),
            "Current Employment": f"{current_emp:,}" if current_emp else "Data unavailable",
            "Growth Rate (%)": f"{growth_rate:.1f}%" if growth_rate else "Data unavailable",
            "Median Wage": f"${wage:,}" if wage else "Data unavailable"
        })
    
    return pd.DataFrame(df_data)

def create_risk_heatmap(comparison_data):
    """Create a risk heatmap from comparison data."""
    if not comparison_data:
        return None
    
    # Handle both dictionary and list formats
    if isinstance(comparison_data, list):
        # Convert list to dictionary format
        valid_jobs = {}
        for item in comparison_data:
            if isinstance(item, dict) and "job_title" in item:
                valid_jobs[item["job_title"]] = item
    else:
        valid_jobs = {k: v for k, v in comparison_data.items() if "error" not in v}
    if not valid_jobs:
        return None
    
    jobs = list(valid_jobs.keys())
    year_1_risks = [valid_jobs[job].get("year_1_risk", 0) or 0 for job in jobs]
    year_5_risks = [valid_jobs[job].get("year_5_risk", 0) or 0 for job in jobs]
    
    # Ensure we have valid data
    if not jobs or all(risk == 0 for risk in year_1_risks + year_5_risks):
        return None
    
    heatmap_data = [year_1_risks, year_5_risks]
    
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data,
        x=jobs,
        y=["1-Year Risk", "5-Year Risk"],
        colorscale="RdYlBu_r"
    ))
    
    fig.update_layout(
        title="Risk Heatmap Comparison",
        height=300
    )
    
    return fig

def create_radar_chart(comparison_data):
    """Create a radar chart for job comparison."""
    if not comparison_data:
        return None
    
    # Handle both dictionary and list formats
    if isinstance(comparison_data, list):
        # Convert list to dictionary format
        valid_jobs = {}
        for item in comparison_data:
            if isinstance(item, dict) and "job_title" in item:
                valid_jobs[item["job_title"]] = item
    else:
        valid_jobs = {k: v for k, v in comparison_data.items() if "error" not in v}
    if not valid_jobs:
        return None
    
    fig = go.Figure()
    
    for job, data in valid_jobs.items():
        categories = ["AI Risk (1Y)", "AI Risk (5Y)", "Job Growth", "Wage Level"]
        growth_val = data.get("projected_growth") or data.get("percent_change") or 0
        wage_val = data.get("median_wage") or 0
        
        # Ensure all values are numeric
        year_1_risk = data.get("year_1_risk", 0) or 0
        year_5_risk = data.get("year_5_risk", 0) or 0
        
        values = [
            year_1_risk,
            year_5_risk,
            min(max(growth_val * 10, 0), 100),
            min(max(wage_val / 1000, 0), 100)
        ]
        
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill="toself",
            name=job
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )),
        showlegend=True,
        title="Job Comparison Radar Chart"
    )
    
    return fig
