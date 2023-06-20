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
    return my_cur.execute(f"SELECT count(*) FROM VLT.{table} where dv_load_timestamp <= DATEADD(hour, -{time}, current_timestamp()) AND dv_load_timestamp >= DATEADD(hour, -{time+1}, current_timestamp())").fetchone()[0]

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



# Side bar setup
st.sidebar.header('🔔 `Data Monitoring Dashboard`')
st.sidebar.subheader('Target Table')
select_target_table = st.sidebar.selectbox('Select target table', df) 

st.sidebar.subheader('Time Frame')
time_records = st.sidebar.slider('Records insereted in the last:', min_value=1, max_value=24)


# st.sidebar.subheader('Heat map parameter')
# time_hist_color = st.sidebar.selectbox('Color by', ('temp_min', 'temp_max')) 
# st.sidebar.subheader('Donut chart parameter')
# donut_theta = st.sidebar.selectbox('Select data', ('q2', 'q3'))

# st.sidebar.subheader('Line chart parameters')
# plot_data = st.sidebar.multiselect('Select data', ['temp_min', 'temp_max'], ['temp_min', 'temp_max'])
# plot_height = st.sidebar.slider('Specify plot height', 200, 500, 250)

st.sidebar.markdown(f'''
---
#### Snowflake Connection established with:
##### {my_data_row}
Created with ❤️ by Malek.
''')


##### Row A
st.markdown('### Metrics')
col1, col2 = st.columns([1,3])
col1.metric("Records Inserted", 
            get_table_count(select_target_table, time_records), 
            get_table_count(select_target_table, time_records) - 
            get_table_count2(select_target_table, time_records)
            )
col2.write(get_topic_names(select_target_table))


##### Row B
records_per_hour = records_timeseries_df(select_target_table)
st.divider()
st.markdown('### Records Inserted in the Past 24 Hours')
st.line_chart(records_per_hour, x='HOUR_OF_DAY', y = 'TOTAL_RECORDS')


##### Row C
# seattle_weather = pd.read_csv('https://raw.githubusercontent.com/tvst/plost/master/data/seattle-weather.csv', parse_dates=['date'])
# stocks = pd.read_csv('https://raw.githubusercontent.com/dataprofessor/data/master/stocks_toy.csv')

# c1, c2 = st.columns((7,3))
# with c1:
#     st.markdown('### Heatmap')
#     plost.time_hist(
#     data=seattle_weather,
#     date='date',
#     x_unit='week',
#     y_unit='day',
#     color=time_hist_color,
#     aggregate='median',
#     legend=None,
#     height=345,
#     use_container_width=True)
# with c2:
#     st.markdown('### Donut chart')
#     plost.donut_chart(
#         data=stocks,
#         theta=donut_theta,
#         color='company',
#         legend='bottom', 
#         use_container_width=True)
