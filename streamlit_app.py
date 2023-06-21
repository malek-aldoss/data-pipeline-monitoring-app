import pandas as pd
import os
import streamlit as st
import plost
import snowflake.connector

# Establish Snowflake connection
def snowflake_conn():
    my_con = snowflake.connector.connect(**st.secrets["snowflake"])
    return my_con.cursor()

# Get a count for the rows inserted into a table in the past x hours
def get_table_count(table: str, time: int) -> int:
    return my_cur.execute(f"SELECT count(*) As TOTAL_RECORDS FROM PROD_GPM_DW.VLT.{table} where dv_load_timestamp >= DATEADD(hour, -{time}, current_timestamp())").fetchone()[0]

# Get a count for the rows inserted into a table between two hours ago and one hour ago
def get_table_count2(table: str, time:int) -> int:
    return my_cur.execute(f"SELECT count(*) FROM VLT.{table} where dv_load_timestamp <= DATEADD(hour, -{time}, current_timestamp()) AND dv_load_timestamp >= DATEADD(hour, -{time+time}, current_timestamp())").fetchone()[0]

# Get topic names for a specific table
def get_topic_names(table: str) -> pd:
    return my_cur.execute(f"SELECT distinct(TOPIC_NAME) FROM PROD_GPM_DW.METADATA.KAFKA_SINK_SOURCE_TARGET_V where source_system = 'RXMGT' AND TYPE = 2 AND TARGET_TABLE = 'VLT.{table}'").fetch_pandas_all()

# Get count of records inserted per hour in the past 24 hours
def records_timeseries_df(table: str) -> pd:
    return my_cur.execute(f"SELECT DATE_TRUNC('HOUR', dv_load_timestamp) AS hour_of_day, count(*) as total_records FROM VLT.{table} where DV_LOAD_TIMESTAMP >= DATEADD(day, -1, current_timestamp()) group by DATE_TRUNC('HOUR', dv_load_timestamp) order by hour_of_day DESC").fetch_pandas_all()



# Page setup
st.set_page_config(page_title="Covetrus Data Monitoring App", layout='wide', initial_sidebar_state='expanded', page_icon="🔔")
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Snowflake connection info
my_cur = snowflake_conn()
my_cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION()")
my_data_row = my_cur.fetchone()

# Extract taget table record counts
df = my_cur.execute("SELECT DISTINCT(REPLACE(TARGET_TABLE, 'VLT.')) AS TARGET_TABLE FROM PROD_GPM_DW.METADATA.KAFKA_SINK_SOURCE_TARGET_V where source_system = 'RXMGT' AND TYPE = 2").fetch_pandas_all()

st.title("🔔 `Data Pipeline Monitoring App`")
st.divider()

# Side bar setup
st.sidebar.header('⚙️ Dashboard Filter')
st.sidebar.subheader('Target Table')
select_target_table = st.sidebar.selectbox('Select target table', df) 
st.sidebar.subheader('Time Frame')
insert_time_frame = st.sidebar.slider('Records insereted in the last __ hour:', min_value=1, max_value=24)

st.sidebar.markdown(f'''
---
#### Snowflake Connection established with:
##### By: {my_data_row[0]}
##### Region: {my_data_row[2]}
Created with ❤️ by Malek.
''')


##### Row A
st.markdown(f"### Records Inserted in the past {insert_time_frame} hour")
col1, col2 = st.columns([1,3])
col1.metric("Into VLT schema", 
            get_table_count(select_target_table, insert_time_frame), 
            get_table_count(select_target_table, insert_time_frame) - 
            get_table_count2(select_target_table, insert_time_frame)
            )
col1.metric("Into Splunk", 
            get_table_count(select_target_table, insert_time_frame), 
            get_table_count(select_target_table, insert_time_frame) - 
            get_table_count2(select_target_table, insert_time_frame)
            )

col2.text(f"kafka topics that feed into {select_target_table}")
col2.write(get_topic_names(select_target_table))


##### Row B
records_per_hour = records_timeseries_df(select_target_table)
st.divider()
st.markdown('### Records Inserted in the Past 24 Hours')
st.line_chart(records_per_hour, x='HOUR_OF_DAY', y = 'TOTAL_RECORDS', use_container_width=True)

