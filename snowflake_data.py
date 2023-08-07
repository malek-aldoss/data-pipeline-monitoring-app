import pandas as pd
import streamlit as st
import snowflake.connector


# Establish Snowflake connection
@st.cache_resource
def snowflake_conn():
    my_con = snowflake.connector.connect(**st.secrets["snowflake"])
    return my_con


# Extract target table names
@st.cache_data(ttl=1200)
def extract_target_tables(source: str) -> pd.DataFrame:
    conn = snowflake_conn()
    query = f"SELECT DISTINCT(REPLACE(TARGET_TABLE, 'VLT.')) AS TARGET_TABLE FROM PROD_GPM_DW.METADATA.KAFKA_SINK_SOURCE_TARGET_V where source_system = '{source}' AND TYPE = 2"
    df = pd.read_sql(query, con=conn)
    return df

# Extract topic names
@st.cache_data(ttl=1200)
def extract_topic_names(source: str) -> pd.DataFrame:
    conn = snowflake_conn()
    query = f"select distinct(topic_name) from PROD_GPM_DW.METADATA.KAFKA_SINK_SOURCE_TARGET_V where source_system = '{source}' AND TYPE = 2"
    df = pd.read_sql(query, con=conn)
    return df


# Get a count for the rows inserted into a table in the past x hour
@st.cache_data(ttl=1200)
def get_table_count(table: str, time: int) -> int:
    conn = snowflake_conn()
    table_name = table.replace('S_DVM_', '')
    try:
        query = f"SELECT count(distinct(H_{table_name}_KEY)) As TOTAL_RECORDS FROM PROD_GPM_DW.VLT.{table} where dv_load_timestamp >= DATEADD(hour, -{time}, current_timestamp())"
        df = pd.read_sql(query, con=conn)
    except:
        query = f"SELECT count(*) As TOTAL_RECORDS FROM PROD_GPM_DW.VLT.{table} where dv_load_timestamp >= DATEADD(hour, -{time}, current_timestamp())"
        df = pd.read_sql(query, con=conn)
    
    return df['TOTAL_RECORDS'].iloc[0]

# Get a count for table that is being fed by the topic
@st.cache_data(ttl=1200)
def get_topic_count(topic: str, time: int) -> pd.DataFrame:
    tables_df = get_target_tables(topic)
    tables_df['TARGET_COUNT'] = [get_table_count(tables_df['TARGET_TABLE'][i], time) for i in tables_df.index]
    
    return tables_df


# Get topic names that feed a specific table
@st.cache_data(ttl=1200)
def get_topic_names(table: str) -> pd.DataFrame:
    conn = snowflake_conn()
    query = f"SELECT distinct(TOPIC_NAME) FROM PROD_GPM_DW.METADATA.KAFKA_SINK_SOURCE_TARGET_V where TARGET_TABLE = 'VLT.{table}'"
    df = pd.read_sql(query, con=conn)
    return df

# Get target table names that are feed by a specific topic
@st.cache_data(ttl=1200)
def get_target_tables(topic: str) -> pd.DataFrame:
    conn = snowflake_conn()
    query = f"SELECT REPLACE(TARGET_TABLE, 'VLT.') AS TARGET_TABLE FROM METADATA.KAFKA_SINK_SOURCE_TARGET_V WHERE TOPIC_NAME = '{topic}';"
    df = pd.read_sql(query, con=conn)
    return df


# Get count of records inserted per hour in the past 24 hours
@st.cache_data(ttl=1200)
def records_timeseries_sf(table: str) -> pd.DataFrame:
    conn = snowflake_conn()
    table_name = table.replace('S_DVM_', '')
    try:
        query = f"SELECT DATE_TRUNC('HOUR', LASTUPD_TS) AS hour_of_day, count(distinct(H_{table_name}_KEY)) as vlt_records FROM VLT.{table} where LASTUPD_TS >= DATEADD(hour, -24, current_timestamp()) group by DATE_TRUNC('HOUR', LASTUPD_TS) order by hour_of_day DESC"
        df = pd.read_sql(query, con=conn)
    except:
        query = f"SELECT DATE_TRUNC('HOUR', created_date) AS hour_of_day, count(distinct(H_{table_name}_KEY)) as vlt_records FROM VLT.{table} where created_date >= DATEADD(hour, -24, current_timestamp()) group by DATE_TRUNC('HOUR', created_date) order by hour_of_day DESC"
        df = pd.read_sql(query, con=conn)
        
    return df    

# Get count of records inserted per hour in the past 24 hours
@st.cache_data(ttl=1200)
def sf_records_24h(table: str) -> pd.DataFrame:
    conn = snowflake_conn()
    
    query = f"SELECT DATE_TRUNC('HOUR', DV_LOAD_TIMESTAMP) AS hour_of_day, count(*) as TARGET_COUNT FROM VLT.{table} where DV_LOAD_TIMESTAMP >= DATEADD(hour, -24, current_timestamp()) group by DATE_TRUNC('HOUR', DV_LOAD_TIMESTAMP) order by hour_of_day DESC"
    df = pd.read_sql(query, con=conn)
        
    return df    

