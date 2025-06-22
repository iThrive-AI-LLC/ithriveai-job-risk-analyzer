"""
Job Title Autocomplete Module (v2)

This module provides autocomplete functionality for job title searches,
sourcing job titles exclusively from the BLS database.
It includes features for search ranking and suggestion filtering.
"""

import os
import streamlit as st
from sqlalchemy import text
from typing import List, Dict, Any
import logging
import database  # central database module exposing shared `engine`

# Configure logging
logger = logging.getLogger(__name__)

# Cache for storing job titles to minimize database queries
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_job_titles_from_db() -> List[Dict[str, Any]]:
    """
    Load all distinct job titles and standardized titles from the database.
    Results are cached. Prioritizes standardized_title if available.
    
    Returns:
        List of dictionaries, each with "display_title" and "soc_code".
        Returns an empty list if database connection fails or no titles are found.
    """
    # Use the shared SQLAlchemy engine initialised in `database.py`
    engine = database.engine

    if engine is None:
        logger.error("Shared database engine is not initialised. Cannot load job titles for autocomplete.")
        return []
    
    job_titles_list: List[Dict[str, Any]] = []
    
    try:
        with engine.connect() as conn:
            # Fetch distinct job_title, standardized_title, and occupation_code
            # Prioritize standardized_title for display if it exists and is different from job_title,
            # otherwise use job_title.
            query = text("""
                SELECT DISTINCT 
                    job_title, 
                    standardized_title, 
                    occupation_code 
                FROM bls_job_data
                ORDER BY standardized_title, job_title
            """)
            result = conn.execute(query)
            
            seen_display_titles = set()
            for row_tuple in result:
                row = row_tuple._mapping # Convert NamedTuple to dict-like
                jt = row.get("job_title")
                st_title = row.get("standardized_title")
                soc = row.get("occupation_code")

                display_title = st_title if st_title and st_title.strip() else jt
                
                if display_title and display_title.strip() and display_title not in seen_display_titles:
                    job_titles_list.append({
                        "display_title": display_title.strip(), 
                        "soc_code": soc,
                        "search_terms": [jt.lower() if jt else "", st_title.lower() if st_title else ""] # For searching
                    })
                    seen_display_titles.add(display_title)

            if not job_titles_list:
                logger.info("No job titles found in the bls_job_data table.")
            else:
                logger.info(f"Successfully loaded {len(job_titles_list)} distinct job titles from database.")
            return job_titles_list
            
    except Exception as e:
        logger.error(f"Error loading job titles from database: {str(e)}", exc_info=True)
        return [] # Return empty list on error, no hardcoded fallbacks

def search_job_titles(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search for job titles matching a query string, with ranking.
    
    Args:
        query: Search string.
        limit: Maximum number of results to return.
        
    Returns:
        List of matching job titles (dictionaries with "display_title", "soc_code").
    """
    all_job_titles = load_job_titles_from_db()

    if not all_job_titles:
        return [] # Database error or no titles loaded

    query_lower = query.lower().strip()
    
    if not query_lower:
        # If query is empty, return a sample of all titles (e.g., first N alphabetically)
        return all_job_titles[:limit]
    
    exact_matches: List[Dict[str, Any]] = []
    starts_with_matches: List[Dict[str, Any]] = []
    contains_matches: List[Dict[str, Any]] = []
    
    # To avoid duplicates in results if a title matches multiple criteria
    added_titles = set()

    for job in all_job_titles:
        display_title_lower = job["display_title"].lower()
        
        if display_title_lower in added_titles:
            continue

        # Exact match on display title
        if display_title_lower == query_lower:
            exact_matches.append(job)
            added_titles.add(display_title_lower)
            continue # Move to next job if exact match found

        # Starts with match on display title
        if display_title_lower.startswith(query_lower):
            starts_with_matches.append(job)
            added_titles.add(display_title_lower)
            continue
        
        # Contains match on display title or original search terms
        if query_lower in display_title_lower or \
           any(query_lower in term for term in job["search_terms"] if term):
            contains_matches.append(job)
            added_titles.add(display_title_lower)

    # Combine results, prioritizing exact matches, then starts-with, then contains
    results = exact_matches + starts_with_matches + contains_matches
    
    # Ensure results are unique by display_title one more time, just in case (though `added_titles` should handle it)
    final_results: List[Dict[str, Any]] = []
    seen_final_titles = set()
    for job in results:
        if job["display_title"] not in seen_final_titles:
            final_results.append(job)
            seen_final_titles.add(job["display_title"])
            
    return final_results[:limit]

def job_title_autocomplete(label: str, key: str = "", placeholder: str = "Search for a job title...", help: str = "") -> str:
    """
    Create a job title autocomplete input field using Streamlit components.
    
    Args:
        label: Label for the input field.
        key: Unique key for Streamlit session state.
        placeholder: Placeholder text.
        help: Help text for the input field.
        
    Returns:
        Selected job title as a string. If no selection from dropdown, returns the raw query.
    """
    # Create unique keys if not provided to prevent Streamlit widget collision
    if not key:
        # A simple way to generate a somewhat unique key based on label
        key_base = "".join(filter(str.isalnum, label.lower()))
        input_key = f"job_search_input_{key_base}"
        select_key = f"job_search_select_{key_base}"
    else:
        input_key = f"{key}_input"
        select_key = f"{key}_select"

    # Text input for search query
    query = st.text_input(
        label=label,
        placeholder=placeholder,
        help=help,
        key=input_key
    )
    
    selected_value = query # Default to the raw query

    if query:
        matches = search_job_titles(query)
        
        if matches:
            # Display matches in a selectbox
            # Options are the display_titles from the matches
            options = [job["display_title"] for job in matches]
            
            # Add a "clear selection/use raw query" option or handle it implicitly
            # For now, if user types and selectbox appears, they are guided to pick from it.
            # If they don't, the raw query is used.
            
            # The selectbox will take precedence if a selection is made.
            # We need to manage its state carefully.
            # If the query changes, the selectbox should update or reset.
            # Streamlit's default selectbox behavior might be sufficient here.
            
            # Ensure the current query is an option if it's a valid job title itself,
            # or provide a way for the user to confirm using their typed query.
            # For simplicity, the selectbox will only show DB matches.
            
            # If the current query exactly matches one of the options, pre-select it.
            current_query_as_option_index = None
            if query in options:
                current_query_as_option_index = options.index(query)

            # Use a placeholder for the selectbox if nothing is selected yet or query doesn't match
            selectbox_label = "Select a matching job title (or refine your search):"
            
            selected_from_dropdown = st.selectbox(
                label=selectbox_label,
                options=options,
                index=current_query_as_option_index, # Pre-select if query is a direct match
                key=select_key,
                help="Select a job from the database or continue typing to refine."
            )
            
            # If a selection is made from the dropdown, it becomes the return value
            if selected_from_dropdown:
                 selected_value = selected_from_dropdown
        else:
            # No matches from the database for the current query
            st.info("No matching job titles found in the database. You can still analyze the entered title if it's valid.")
            # selected_value remains the raw query
    
    return selected_value

if __name__ == "__main__":
    # Example usage within a Streamlit app context (for testing)
    st.title("Job Title Autocomplete Test")

    # Ensure DATABASE_URL is set in environment or secrets for this test to connect to DB
    if "DATABASE_URL" not in os.environ and not (hasattr(st, 'secrets') and st.secrets.get("database", {}).get("DATABASE_URL")):
        st.error("DATABASE_URL not set. Please configure it in your environment or Streamlit secrets.")
    else:
        st.write("Attempting to load job titles from DB...")
        titles = load_job_titles_from_db()
        if titles:
            st.write(f"Loaded {len(titles)} job titles. First 5: {titles[:5]}")
        else:
            st.warning("Could not load job titles from the database. Autocomplete may not function correctly.")

        selected_job_1 = job_title_autocomplete(
            label="Search for your job title (Instance 1):",
            key="job_search_1",
            placeholder="e.g., Software Developer, Nurse...",
            help="Type at least 2 characters to see suggestions."
        )
        st.write(f"You selected (Instance 1): **{selected_job_1}**")

        st.markdown("---")

        selected_job_2 = job_title_autocomplete(
            label="Search for another job title (Instance 2):",
            key="job_search_2",
            placeholder="e.g., Project Manager, Data Analyst...",
            help="Suggestions are sourced from the BLS database."
        )
        st.write(f"You selected (Instance 2): **{selected_job_2}**")

        st.markdown("---")
        st.subheader("Test Search Functionality Directly")
        test_query = st.text_input("Enter a test query for search_job_titles:")
        if test_query:
            results = search_job_titles(test_query)
            st.write("Search Results:")
            if results:
                for r in results:
                    st.write(f"- {r['display_title']} (SOC: {r['soc_code']})")
            else:
                st.write("No results found.")
