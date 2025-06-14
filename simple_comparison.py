"""
Job Comparison Module (simple_comparison.py)

Generates visualizations and comparison tables using real BLS data
obtained via job_api_integration_database_only.py.
"""
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import logging

# Assuming job_api_integration_database_only is in the same path or installed
try:
    import job_api_integration_database_only as job_api_integration
except ImportError:
    logging.critical("Failed to import job_api_integration_database_only. This module is essential for simple_comparison.")
    # Define a stub if missing, so the app can show a critical error.
    class job_api_integration_stub:
        @staticmethod
        def get_jobs_comparison_data(jobs_list):
            return {job: {"error": "CRITICAL: job_api_integration_database_only module not found.", "job_title": job} for job in jobs_list}
    job_api_integration = job_api_integration_stub()


logger = logging.getLogger(__name__)

def get_job_comparison_data(jobs_list: list[str]) -> dict:
    """
    Get comparison data for multiple jobs using ONLY database/BLS data.
    """
    logger.info(f"Fetching comparison data for jobs: {jobs_list}")
    try:
        data = job_api_integration.get_jobs_comparison_data(jobs_list)
        logger.info(f"Successfully fetched comparison data for {len(jobs_list)} jobs.")
        return data
    except Exception as e:
        logger.error(f"Error in get_job_comparison_data: {e}", exc_info=True)
        return {job: {"error": f"Data unavailable for {job} due to system error: {e}", "job_title": job} for job in jobs_list}

def create_comparison_chart(comparison_data: dict) -> go.Figure | None:
    """
    Create a comparison bar chart for 1-Year and 5-Year AI Displacement Risk.
    """
    if not comparison_data:
        logger.warning("create_comparison_chart: No comparison data provided.")
        return None
    
    valid_jobs_data = {k: v for k, v in comparison_data.items() if v and "error" not in v}
    
    if not valid_jobs_data:
        logger.warning("create_comparison_chart: No valid job data found after filtering errors.")
        return None
    
    job_titles = list(valid_jobs_data.keys())
    year_1_risks = [valid_jobs_data[job].get('year_1_risk', 0) or 0 for job in job_titles]
    year_5_risks = [valid_jobs_data[job].get('year_5_risk', 0) or 0 for job in job_titles]
    
    if not job_titles or (all(r == 0 for r in year_1_risks) and all(r == 0 for r in year_5_risks)):
        logger.warning("create_comparison_chart: Job titles list is empty or all risk values are zero.")
        return None
        
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name='1-Year Risk',
        x=job_titles,
        y=year_1_risks,
        marker_color='#63A4FF', # Light blue
        text=[f"{y:.1f}%" for y in year_1_risks],
        textposition='auto'
    ))
    fig.add_trace(go.Bar(
        name='5-Year Risk', 
        x=job_titles,
        y=year_5_risks,
        marker_color='#0052B8', # Dark blue
        text=[f"{y:.1f}%" for y in year_5_risks],
        textposition='auto'
    ))
    
    fig.update_layout(
        title_text='AI Displacement Risk Comparison',
        xaxis_title_text='Job Titles',
        yaxis_title_text='Risk Percentage (%)',
        barmode='group',
        legend_title_text='Risk Horizon',
        height=450,
        margin=dict(l=50, r=50, t=80, b=120), # Adjust bottom margin for long job titles
        xaxis_tickangle=-45 # Angle job titles if they are long
    )
    logger.info("Successfully created comparison chart.")
    return fig

def create_comparison_table(comparison_data: dict) -> pd.DataFrame | None:
    """
    Create a pandas DataFrame for detailed job comparison.
    """
    if not comparison_data:
        logger.warning("create_comparison_table: No comparison data provided.")
        return None

    valid_jobs_data = {k: v for k, v in comparison_data.items() if v and "error" not in v}

    if not valid_jobs_data:
        logger.warning("create_comparison_table: No valid job data found after filtering errors.")
        return None

    table_rows = []
    for job_title_key, data in valid_jobs_data.items():
        # job_title_key is the key from the input dict (original search term)
        # data.get('job_title') is the standardized title from BLS
        display_title = data.get('job_title', job_title_key) 
        
        current_emp = data.get('current_employment')
        proj_growth = data.get('projected_growth') # This is percent_change from API
        med_wage = data.get('median_wage')

        table_rows.append({
            "Job Title": display_title,
            "SOC Code": data.get('occupation_code', 'N/A'),
            "Risk Category": data.get('risk_category', 'N/A'),
            "1-Year Risk (%)": f"{data.get('year_1_risk', 0):.1f}",
            "5-Year Risk (%)": f"{data.get('year_5_risk', 0):.1f}",
            "Current Employment": f"{int(current_emp):,}" if current_emp is not None else "N/A",
            "Projected Growth (%)": f"{proj_growth:.1f}" if proj_growth is not None else "N/A",
            "Median Annual Wage": f"${int(med_wage):,}" if med_wage is not None else "N/A"
        })
    
    if not table_rows:
        logger.warning("create_comparison_table: No rows generated for the table.")
        return None

    df = pd.DataFrame(table_rows)
    logger.info("Successfully created comparison table DataFrame.")
    return df

def create_risk_heatmap(comparison_data: dict) -> go.Figure | None:
    """
    Create a heatmap visualizing 1-Year and 5-Year risks for jobs.
    """
    if not comparison_data:
        logger.warning("create_risk_heatmap: No comparison data provided.")
        return None

    valid_jobs_data = {k: v for k, v in comparison_data.items() if v and "error" not in v}

    if not valid_jobs_data:
        logger.warning("create_risk_heatmap: No valid job data found after filtering errors.")
        return None

    job_titles = [valid_jobs_data[job].get('job_title', job) for job in valid_jobs_data.keys()]
    year_1_risks = [valid_jobs_data[job].get('year_1_risk', 0) or 0 for job in valid_jobs_data.keys()]
    year_5_risks = [valid_jobs_data[job].get('year_5_risk', 0) or 0 for job in valid_jobs_data.keys()]

    if not job_titles or (all(r == 0 for r in year_1_risks) and all(r == 0 for r in year_5_risks)):
        logger.warning("create_risk_heatmap: Job titles list is empty or all risk values are zero.")
        return None
        
    # Data for heatmap: rows are risk horizons, columns are jobs
    heatmap_z_data = [year_1_risks, year_5_risks]
    y_labels = ["1-Year Risk", "5-Year Risk"]

    fig = go.Figure(data=go.Heatmap(
        z=heatmap_z_data,
        x=job_titles,
        y=y_labels,
        colorscale="RdYlGn_r", # Red (high risk) to Green (low risk)
        zmin=0,
        zmax=100,
        text=[[f"{val:.1f}%" for val in row] for row in heatmap_z_data], # Display percentages on cells
        texttemplate="%{text}",
        showscale=True,
        colorbar={"title": "Risk (%)"}
    ))
    
    fig.update_layout(
        title_text="AI Displacement Risk Progression Heatmap",
        xaxis_title_text="Job Titles",
        yaxis_title_text="Risk Horizon",
        height=350 + len(job_titles) * 10, # Adjust height based on number of jobs
        margin=dict(l=100, r=50, t=80, b=120),
        xaxis_tickangle=-45
    )
    logger.info("Successfully created risk heatmap.")
    return fig

def create_radar_chart(comparison_data: dict) -> go.Figure | None:
    """
    Create a radar chart comparing jobs across multiple dimensions.
    Dimensions: AI Risk (1Y), AI Risk (5Y), Job Growth (scaled), Median Wage (scaled).
    """
    if not comparison_data:
        logger.warning("create_radar_chart: No comparison data provided.")
        return None

    valid_jobs_data = {k: v for k, v in comparison_data.items() if v and "error" not in v}
    
    if not valid_jobs_data:
        logger.warning("create_radar_chart: No valid job data found after filtering errors.")
        return None

    fig = go.Figure()
    categories = ["AI Risk (1Y)", "AI Risk (5Y)", "Job Growth Outlook", "Median Wage Level"]

    for job_key, data in valid_jobs_data.items():
        display_title = data.get('job_title', job_key)
        
        year_1_risk = data.get('year_1_risk', 0) or 0
        year_5_risk = data.get('year_5_risk', 0) or 0
        
        # projected_growth is percent_change
        growth_val = data.get('projected_growth', 0) or 0
        # Scale growth: 0% growth -> 50. +10% growth -> 100. -10% growth -> 0.
        # This makes "higher is better" for growth outlook on the radar.
        # (original app_production: min(max(growth_val * 10, 0), 100) - only shows positive)
        # New scaling: (value + 10) * 5. So -10% -> 0, 0% -> 50, +10% -> 100.
        scaled_growth = min(max(0, (growth_val + 10) * 5), 100)

        median_wage = data.get('median_wage', 0) or 0
        # Scale wage: $100k -> 100. $50k -> 50.
        # (original app_production: min(max(median_wage / 1000, 0), 100))
        scaled_wage = min(max(0, (median_wage / 1000)), 100) # Assuming wage is in absolute dollars

        values = [
            year_1_risk,    # Lower is better, but radar shows magnitude.
            year_5_risk,    # Lower is better.
            scaled_growth,  # Higher is better.
            scaled_wage     # Higher is better.
        ]
        
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            name=display_title,
            hovertemplate='<b>%{theta}</b>: %{r:.1f}<extra></extra>' # Custom hover text
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100], # All scaled values are 0-100
                tickvals=[0, 25, 50, 75, 100],
                ticktext=['0/Low', '25', '50/Avg', '75', '100/High']
            )
        ),
        showlegend=True,
        title_text="Multi-Factor Job Comparison Radar",
        legend_title_text="Job Titles",
        height=500,
        margin=dict(l=80, r=80, t=100, b=80)
    )
    logger.info("Successfully created radar chart.")
    return fig

if __name__ == '__main__':
    # Example usage for testing this module directly
    # This requires job_api_integration_database_only.py to be functional
    # and environment variables (DATABASE_URL, BLS_API_KEY) to be set.
    
    logging.basicConfig(level=logging.INFO)
    logger.info("Running simple_comparison.py direct tests...")

    test_job_list = ["Software Developer", "Registered Nurse", "NonExistentJob123"]
    
    logger.info(f"\n--- Test 1: get_job_comparison_data ---")
    comp_data = get_job_comparison_data(test_job_list)
    if comp_data:
        logger.info(f"Comparison data fetched for {len(comp_data)} jobs (includes potential errors).")
        for job, details in comp_data.items():
            if "error" in details:
                logger.warning(f"  {job}: Error - {details['error']}")
            else:
                logger.info(f"  {job}: Success - Year 5 Risk: {details.get('year_5_risk')}")
    else:
        logger.error("Failed to fetch any comparison data.")

    # Filter out errors for chart/table generation tests
    valid_comp_data = {k: v for k, v in comp_data.items() if v and "error" not in v}

    if valid_comp_data:
        logger.info(f"\n--- Test 2: create_comparison_chart ---")
        chart_fig = create_comparison_chart(valid_comp_data)
        if chart_fig:
            logger.info("Comparison chart figure created successfully.")
            # chart_fig.show() # Uncomment to display if running in an environment that supports it
        else:
            logger.error("Failed to create comparison chart figure.")

        logger.info(f"\n--- Test 3: create_comparison_table ---")
        table_df = create_comparison_table(valid_comp_data)
        if table_df is not None and not table_df.empty:
            logger.info("Comparison table DataFrame created successfully:")
            logger.info(f"\n{table_df.to_string()}")
        else:
            logger.error("Failed to create comparison table DataFrame or it was empty.")

        logger.info(f"\n--- Test 4: create_risk_heatmap ---")
        heatmap_fig = create_risk_heatmap(valid_comp_data)
        if heatmap_fig:
            logger.info("Risk heatmap figure created successfully.")
            # heatmap_fig.show()
        else:
            logger.error("Failed to create risk heatmap figure.")

        logger.info(f"\n--- Test 5: create_radar_chart ---")
        radar_fig = create_radar_chart(valid_comp_data)
        if radar_fig:
            logger.info("Radar chart figure created successfully.")
            # radar_fig.show()
        else:
            logger.error("Failed to create radar chart figure.")
    else:
        logger.warning("Skipping chart/table generation tests as no valid job data was fetched.")
        
    logger.info("\nsimple_comparison.py direct tests complete.")
