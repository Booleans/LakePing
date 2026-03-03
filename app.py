import os
import streamlit as st
from databricks.connect import DatabricksSession
from ping_logic import run_ping_sweep

# DATABRICKS_HOST is auto-injected by the Databricks Apps runtime
CLUSTER_ID = os.getenv("CLUSTER_ID")  # Set as an app environment variable

@st.cache_resource
def get_spark():
    return DatabricksSession.builder.remote(
        host=os.getenv("DATABRICKS_HOST"),
        cluster_id=CLUSTER_ID
    ).getOrCreate()

st.title("📡 Phone Network Health Monitor")

if st.button("Run Ping Sweep"):
    spark = get_spark()
    with st.spinner("Pinging all phones..."):
        results_df = run_ping_sweep(spark)
    
    up = results_df[results_df["status"] == "UP"]
    down = results_df[results_df["status"] == "DOWN"]
    
    col1, col2 = st.columns(2)
    col1.metric("✅ Phones UP", len(up))
    col2.metric("❌ Phones DOWN", len(down))
    
    st.dataframe(results_df, use_container_width=True)