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
    return my_cur.execute(f"select DATE_TRUNC('HOUR', hour_of_day) AS hour_of_day, sum(vlt_records) as vlt_records from (select *, case when DATE_PART('HOUR', hour_of_day) = 0 AND DATE_PART('MINUTE', hour_of_day) between 0 AND 20 THEN 23 when DATE_PART('MINUTE', hour_of_day) between 0 AND 20 THEN DATE_PART('HOUR', hour_of_day) - 1 else DATE_PART('HOUR', hour_of_day) end as correct_hour from (SELECT dv_load_timestamp AS hour_of_day, count(*) as vlt_records FROM VLT.{table} where DV_LOAD_TIMESTAMP >= DATEADD(day, -1, current_timestamp()) group by dv_load_timestamp order by hour_of_day DESC)) group by DATE_TRUNC('HOUR', hour_of_day) order by hour_of_day DESC").fetch_pandas_all()

st.cache_data(ttl=1200)
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


def combine_data():
    vlt_records_per_hour = records_timeseries_df(select_target_table)
    splunk_records_per_hour = ss.get_events_timeline(ss.get_search_id(select_target_table, "-24h@h"))
    
    vlt_records_per_hour['HOUR_OF_DAY'] = pd.to_datetime(vlt_records_per_hour['HOUR_OF_DAY'])
    splunk_records_per_hour['earliest_strftime'] = pd.to_datetime(splunk_records_per_hour['earliest_strftime'])

    merged_df = pd.merge(splunk_records_per_hour, vlt_records_per_hour, left_on='earliest_strftime', right_on='HOUR_OF_DAY', how='outer')
    
    merged_df.drop('HOUR_OF_DAY', axis=1, inplace=True)
    
    merged_df.rename(columns={'VLT_RECORDS': 'vlt_records'}, inplace=True)
    merged_df.rename(columns={'total_count': 'splunk_records'}, inplace=True)
    merged_df.rename(columns={'earliest_strftime': 'hour_of_day'}, inplace=True)
    
    # vlt_records_per_hour[''] = pd.to_datetime(vlt_records_per_hour['HOUR_OF_DAY'])
    
    merged_df['vlt_records'].fillna(0, inplace=True)
    merged_df['vlt_records'] = merged_df['vlt_records'].astype(int)
       
    return merged_df



# Page setup
st.set_page_config(page_title="Covetrus Data Monitoring App", layout='wide', initial_sidebar_state='expanded', page_icon="🔔")
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


# Snowflake connection info
my_cur = snowflake_conn()
my_cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION()")
my_data_row = my_cur.fetchone()


# Extract taget table names
target_tables = my_cur.execute("SELECT DISTINCT(REPLACE(TARGET_TABLE, 'VLT.')) AS TARGET_TABLE FROM PROD_GPM_DW.METADATA.KAFKA_SINK_SOURCE_TARGET_V where source_system = 'RXMGT' AND TYPE = 2").fetch_pandas_all()

topic_names = my_cur.execute("select distinct(topic_name) from PROD_GPM_DW.METADATA.KAFKA_SINK_SOURCE_TARGET_V where source_system = 'RXMGT' AND TYPE = 2").fetch_pandas_all()










# Side bar setup
with st.sidebar:
    st.header('⚙️ Dashboard Filter')
    st.subheader('Target Table')
    filter_by = st.radio('Filter by:', ["Target Table", "Topic Name"], disabled=True)
    
    if filter_by == "Target Table":
        select_target_table = st.selectbox('Select target table', target_tables) 
    else:
        select_topic_name = st.selectbox('Select topic name', topic_names)
    
    st.subheader('Time Frame')
    insert_time_frame = st.slider('Records insereted in the last __ hour:', min_value=1, max_value=24)
    st.markdown(f'''
    ---
    #### Snowflake Connection established with:
    ###### &ensp;&ensp;&ensp; By: {my_data_row[0]}
    ###### &ensp;&ensp;&ensp; Region: {my_data_row[2]}
    Created by Malek.
    ''')


##### Page Title
st.title("🔔 `Data Pipeline Monitoring App`")
st.divider()


##### Row A
st.markdown(f"### Records Inserted in the past {insert_time_frame} hour")
col1, col2 = st.columns([1,3])

with col1:
    st.metric("Into VLT schema", 
                get_table_count(select_target_table, insert_time_frame), 
                get_table_count(select_target_table, insert_time_frame) - 
                get_table_count2(select_target_table, insert_time_frame)
                )
    st.metric("Into Splunk", ss.get_events_summary(ss.get_search_id(select_target_table, f"-{insert_time_frame}h@h"))) #Only works on S_RXMGT_ORDER

with col2:
    st.text(f"kafka topics that feed into {select_target_table}")
    st.write(get_topic_names(select_target_table))


##### Row B
combined_records = combine_data()

st.divider()
st.markdown('### Records Inserted in the Past 24 Hours')
fig, ax = plt.subplots()
combined_records.plot.bar(x = 'hour_of_day', y=['splunk_records','vlt_records'],ax=ax)
st.pyplot(fig)


##### Row C
st.divider()
st.markdown("### Records Missing from Snowflake")
# missing_records = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
missing_records = None

if missing_records is None:
    st.success("Snowflake's data vault is up to date!")
else:
    st.warning("Some records are missing in Snowflake")
    st.table(missing_records)
    csv = convert_df(missing_records)
    
    st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name='missing_records.csv',
    mime='text/csv',)
    