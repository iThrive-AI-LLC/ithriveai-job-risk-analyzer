import streamlit as st
import os

st.set_page_config(page_title="Secrets Test Page", layout="wide")

st.title("⚙️ Streamlit Secrets Configuration Test")
st.markdown("""
This page helps verify if your `BLS_API_KEY` and `DATABASE_URL` are correctly configured
and accessible within your Streamlit Cloud application.
""")

st.header("1. BLS API Key Check")

bls_api_key_st_secrets = None
bls_api_key_env = os.environ.get("BLS_API_KEY")
source_bls = "Not found"

try:
    if hasattr(st, 'secrets') and "api_keys" in st.secrets and "BLS_API_KEY" in st.secrets["api_keys"]:
        bls_api_key_st_secrets = st.secrets["api_keys"]["BLS_API_KEY"]
        source_bls = "Streamlit secrets (st.secrets)"
    elif bls_api_key_env:
        source_bls = "Environment variable (os.environ)"
    else:
        source_bls = "Not found in Streamlit secrets or environment variables"
except Exception as e:
    st.error(f"Error accessing st.secrets for BLS_API_KEY: {e}")
    source_bls = f"Error accessing st.secrets: {e}"

st.subheader("BLS_API_KEY Status:")
if bls_api_key_st_secrets:
    st.success(f"✅ BLS_API_KEY found via {source_bls}!")
    masked_key = bls_api_key_st_secrets[:4] + "****" + bls_api_key_st_secrets[-4:] if len(bls_api_key_st_secrets) > 8 else "****"
    st.write(f"   Value (masked): `{masked_key}`")
elif bls_api_key_env:
    st.success(f"✅ BLS_API_KEY found via {source_bls}!")
    masked_key = bls_api_key_env[:4] + "****" + bls_api_key_env[-4:] if len(bls_api_key_env) > 8 else "****"
    st.write(f"   Value (masked): `{masked_key}`")
else:
    st.error(f"❌ BLS_API_KEY {source_bls}.")
    st.markdown("""
        **Troubleshooting Tips:**
        - Ensure your `secrets.toml` file in Streamlit Cloud settings has the following structure:
          ```toml
          [api_keys]
          BLS_API_KEY = "your_actual_bls_api_key"
          ```
        - Double-check for typos in `api_keys` or `BLS_API_KEY`.
        - Make sure you've saved the secrets and rebooted the app after changes.
    """)

st.markdown("---")

st.header("2. Database URL Check")

database_url_st_secrets = None
database_url_env = os.environ.get("DATABASE_URL")
source_db = "Not found"

try:
    if hasattr(st, 'secrets') and "database" in st.secrets and "DATABASE_URL" in st.secrets["database"]:
        database_url_st_secrets = st.secrets["database"]["DATABASE_URL"]
        source_db = "Streamlit secrets (st.secrets)"
    elif database_url_env:
        source_db = "Environment variable (os.environ)"
    else:
        source_db = "Not found in Streamlit secrets or environment variables"
except Exception as e:
    st.error(f"Error accessing st.secrets for DATABASE_URL: {e}")
    source_db = f"Error accessing st.secrets: {e}"

st.subheader("DATABASE_URL Status:")
if database_url_st_secrets:
    st.success(f"✅ DATABASE_URL found via {source_db}!")
    # Mask parts of the URL for display
    try:
        from urllib.parse import urlparse
        parsed_url = urlparse(database_url_st_secrets)
        masked_url = f"{parsed_url.scheme}://{parsed_url.username}:****@{parsed_url.hostname}{parsed_url.path}"
        st.write(f"   Value (partially masked): `{masked_url}`")
    except Exception:
        st.write(f"   Value (could not parse for masking): `{database_url_st_secrets[:20]}...`")
elif database_url_env:
    st.success(f"✅ DATABASE_URL found via {source_db}!")
    try:
        from urllib.parse import urlparse
        parsed_url = urlparse(database_url_env)
        masked_url = f"{parsed_url.scheme}://{parsed_url.username}:****@{parsed_url.hostname}{parsed_url.path}"
        st.write(f"   Value (partially masked): `{masked_url}`")
    except Exception:
        st.write(f"   Value (could not parse for masking): `{database_url_env[:20]}...`")
else:
    st.error(f"❌ DATABASE_URL {source_db}.")
    st.markdown("""
        **Troubleshooting Tips:**
        - Ensure your `secrets.toml` file in Streamlit Cloud settings has the following structure:
          ```toml
          [database]
          DATABASE_URL = "your_full_database_connection_string"
          ```
        - Double-check for typos in `database` or `DATABASE_URL`.
        - Ensure the connection string is complete and correct (e.g., `postgresql://user:password@host:port/dbname?sslmode=require`).
        - Make sure you've saved the secrets and rebooted the app.
    """)

st.markdown("---")
st.header("3. How to Use This Page")
st.markdown("""
1.  **Upload this file (`test_secrets.py`) to your GitHub repository.**
    *   You can put it in the root directory or in a `pages/` subdirectory.
    *   If in `pages/`, it will appear in the sidebar navigation.
    *   If in the root, you can access it via `your-app-url/test_secrets`.
2.  **Ensure your secrets are configured in Streamlit Cloud.**
    *   Go to "Manage app" -> "Secrets".
    *   The content should look like this (replace with your actual values):
        ```toml
        [api_keys]
        BLS_API_KEY = "your_bls_api_key"

        [database]
        DATABASE_URL = "postgresql://user:password@host:port/dbname?sslmode=require"
        ```
3.  **Reboot your Streamlit app.**
4.  **Navigate to this test page.**
5.  **Review the status messages above.** They will tell you if Streamlit can find and read your configured secrets.
""")

st.info("This page only checks if the secrets are *accessible* to the Streamlit app. It does not validate if the API key is correct or if the database URL allows a connection.")
