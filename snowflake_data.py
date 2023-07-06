import pandas as pd
import streamlit as st
import snowflake.connector
import splunk_search as ss
import matplotlib.pyplot as plt

# Establish Snowflake connection
@st.cache_resource
def snowflake_conn():
    my_con = snowflake.connector.connect(**st.secrets["snowflake"])
    return my_con.cursor()

# Get a count for the rows inserted into a table in the past x hour
@st.cache_data(ttl=1200)
def get_table_count(table: str, time: int) -> int:
    return my_cur.execute(f"SELECT count(*) As TOTAL_RECORDS FROM PROD_GPM_DW.VLT.{table} where dv_load_timestamp >= DATEADD(hour, -{time}, current_timestamp())").fetchone()[0]

# Get a count for the rows inserted into a table between two hours ago and one hour ago
@st.cache_data(ttl=1200)
def get_table_count2(table: str, time:int) -> int:
    return my_cur.execute(f"SELECT count(*) FROM VLT.{table} where dv_load_timestamp <= DATEADD(hour, -{time}, current_timestamp()) AND dv_load_timestamp >= DATEADD(hour, -{time+time}, current_timestamp())").fetchone()[0]

# Get topic names for a specific table
@st.cache_data
def get_topic_names(table: str) -> pd:
    return my_cur.execute(f"SELECT distinct(TOPIC_NAME) FROM PROD_GPM_DW.METADATA.KAFKA_SINK_SOURCE_TARGET_V where source_system = 'RXMGT' AND TYPE = 2 AND TARGET_TABLE = 'VLT.{table}'").fetch_pandas_all()

# Get count of records inserted per hour in the past 24 hours
@st.cache_data(ttl=1200)
def records_timeseries_df(table: str) -> pd:
    return my_cur.execute(f"SELECT DATE_TRUNC('HOUR', dv_load_timestamp) AS hour_of_day, count(*) as vlt_records FROM VLT.{table} where DV_LOAD_TIMESTAMP >= DATEADD(hour, -24, current_timestamp()) group by DATE_TRUNC('HOUR', dv_load_timestamp) order by hour_of_day DESC").fetch_pandas_all()

st.cache_data(ttl=1200)
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


# Snowflake connection info
my_cur = snowflake_conn()
