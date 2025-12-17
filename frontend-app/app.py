import streamlit as st
from google.cloud import bigquery
import os

# --- Configuration ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("BQ_DATASET")
TABLE_ID = os.environ.get("BQ_TABLE")

st.set_page_config(page_title="AI Marketing Gallery", layout="wide")

st.title("🛍️ AI Marketing Content Gallery")
st.markdown(f"Displaying results from BigQuery table: `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`")

if not PROJECT_ID or not DATASET_ID or not TABLE_ID:
    st.error("Missing environment variables. Please check your deployment.")
else:
    try:
        client = bigquery.Client()
        
        # Query the latest 20 results
        query = f"""
            SELECT product_name, keywords, generated_content, generated_image_url
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE generated_image_url IS NOT NULL 
              AND generated_image_url NOT LIKE 'Error%'
            ORDER BY processed_at DESC
            LIMIT 20
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        cols = st.columns(3) # Create a grid layout
        
        for i, row in enumerate(results):
            with cols[i % 3]:
                st.subheader(row.product_name)
                if row.generated_image_url:
                    st.image(row.generated_image_url, use_container_width=True)
                
                st.markdown("**Keywords:** " + row.keywords)
                with st.expander("Read Marketing Copy"):
                    st.write(row.generated_content)
                st.divider()
                
    except Exception as e:
        st.error(f"Error connecting to BigQuery: {e}")
