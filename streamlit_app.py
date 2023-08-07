import mysql.connector
import pandas as pd
import streamlit as st
# import matplotlib.pyplot as plt
import plotly.express as px
from snowflake_data import *
from dvm_data import *
from splunk_data import *

st.cache_data()
def join_sf_splunk(table):
    sf_df = sf_records_24h(table)
    splunk_df = get_events_timeline(get_search_id(table, "-24h@h"))
    
    sf_df['HOUR_OF_DAY'] = pd.to_datetime(sf_df['HOUR_OF_DAY'])
    splunk_df['HOUR_OF_DAY'] = pd.to_datetime(splunk_df['HOUR_OF_DAY'])

    merged_df = pd.merge(splunk_df, sf_df, on='HOUR_OF_DAY', how='outer')

    merged_df['SOURCE_COUNT'].fillna(0, inplace=True)
    merged_df['SOURCE_COUNT'] = merged_df['SOURCE_COUNT'].astype(int)
       
    return merged_df

st.cache_data(ttl=1200)
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

# Joins data from snowflake and dvm MySQL databases for a 24 hr timeline of records inserted in each
st.cache_data(ttl=1200)
def join_sf_dvm(table):
    sf_df = records_timeseries_sf(table)
    dvm_df = records_timeseries_dvm(table)
    
    sf_df['HOUR_OF_DAY'] = pd.to_datetime(sf_df['HOUR_OF_DAY'])
    dvm_df['HOUR_OF_DAY'] = pd.to_datetime(dvm_df['HOUR_OF_DAY'])
    main_df = pd.merge(sf_df, dvm_df, on='HOUR_OF_DAY')
    
    return main_df


########################################################################################## Page setup
st.set_page_config(page_title="Covetrus Data Monitoring App", layout='wide', initial_sidebar_state='expanded', page_icon="🔔")
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Snowflake connection
sf_cur = snowflake_conn().cursor()
sf_cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION()")
my_data_row = sf_cur.fetchone()

# MySQL dvm connection
dvm_con = mysql.connector.connect(**st.secrets["MySQL"])
dvm_cur = dvm_con.cursor()


######################################################################################### Side bar setup
with st.sidebar:
    st.header('⚙️ Dashboard Filter')
    # st.subheader('Target Table')
    
    # Search filters 
    filter_by_type = st.radio('Filter by:', ["Target Table", "Topic Name"], disabled=False)
    filter_by_source = st.selectbox('Source:', ["RXMGT", "DVM"], disabled=False)
    
    # Extract taget table names and Topic Names
    target_tables = extract_target_tables(filter_by_source).sort_values(by=['TARGET_TABLE'])
    topic_names = extract_topic_names(filter_by_source).sort_values(by=['TOPIC_NAME'])
    
    
    if filter_by_type == "Target Table":
        select_target_table = st.selectbox('Select target table', target_tables) 
    else:
        select_topic_name = st.selectbox('Select topic name', topic_names)
    
    st.subheader('Time Frame')
    insert_time_frame = st.slider('Records insereted in the last __ hour:', min_value=1, max_value=24)
    st.markdown(f'''
    ---
    #### Snowflake Connection established:
    ###### &ensp;&ensp;&ensp; By: {my_data_row[0]}
    ###### &ensp;&ensp;&ensp; Region: {my_data_row[2]}
    ''')



######################################################################################### Main
# Page Title
st.title("🔔 `Data Pipeline Monitoring App`")
st.divider()

# Main Metrics, Charts, and Info
st.markdown(f"### Records Inserted in the past {insert_time_frame} hour")

# A target table was selected
if filter_by_type == "Target Table":
    # Row A 
    col1, col2 = st.columns([1,3])
    with col1:
        st.metric("Target Count", get_table_count(select_target_table, insert_time_frame))
        
        if filter_by_source == "DVM": # DVM source selected
            try:
                st.metric("Source Count", get_dvm_count(select_target_table, insert_time_frame))
            except:
                st.metric("Source Count", "Unavailable")

        else: # RXMGT source selected
            st.metric("Source Count", get_events_summary(get_search_id(select_target_table, f"-{insert_time_frame}h@h"))) 
            
            # try:
            #     st.metric("Source Count", get_events_summary(get_search_id(select_target_table, f"-{insert_time_frame}h@h"))) 
            # except:
            #     st.metric("Source Count", "Unavailable")
        
    with col2:
        st.text(f"kafka topics that feed into {select_target_table}")
        st.table(get_topic_names(select_target_table))

    # Row B
    if filter_by_source == "DVM": # DVM source selected
        st.divider()
        st.markdown('### Records Inserted in the Past 24 Hours')
        
        try:
            sf_dvm_data = join_sf_dvm(select_target_table)
            fig = px.bar(data_frame=sf_dvm_data, y=['VLT_RECORDS', 'DVM_RECORDS'], x='HOUR_OF_DAY', barmode='group')
            fig.update_layout(
            xaxis_tickfont_size=14,
            yaxis=dict(
                title='Records Count',
                titlefont_size=16,
                tickfont_size=14,
            ),
            legend=dict(
                x=0,
                y=1.2,
                bgcolor='rgba(255, 255, 255, 0)',
                bordercolor='rgba(255, 255, 255, 0)'
            ),
            barmode='group',
            bargap=0.20, # gap between bars of adjacent location coordinates.
            bargroupgap=0.15 # gap between bars of the same location coordinate.
            )
            st.write(fig)
        except:
            pass
                  
    else: # RXMGT source selected
        st.divider()
        st.markdown('### Records Inserted in the Past 24 Hours')
        

        sf_splunk_data = join_sf_splunk(select_target_table)
        # fig, ax = plt.subplots()
        # sf_splunk_data.plot.bar(x = 'hour_of_day', y=['splunk_records','vlt_records'],ax=ax)
        # st.pyplot(fig)
        
        fig = px.bar(data_frame=sf_splunk_data, y=['SOURCE_COUNT', 'TARGET_COUNT'], x='HOUR_OF_DAY', barmode='group')
        fig.update_layout(
        xaxis_tickfont_size=14,
        yaxis=dict(
            title='Records Count',
            titlefont_size=16,
            tickfont_size=14,
        ),
        legend=dict(
            x=0,
            y=1.2,
            bgcolor='rgba(255, 255, 255, 0)',
            bordercolor='rgba(255, 255, 255, 0)'
        ),
        barmode='group',
        bargap=0.20, # gap between bars of adjacent location coordinates.
        bargroupgap=0.15 # gap between bars of the same location coordinate.
        )
        st.write(fig)



#### A Topic was selected 
else:
    # Row A
    ttc = get_topic_count(select_topic_name, insert_time_frame)
    st.dataframe(ttc)

    # with col2:
    #     st.text(f"Target tables getting feed by {select_topic_name}")
    #     st.write(get_target_tables(sf_cur, select_topic_name))