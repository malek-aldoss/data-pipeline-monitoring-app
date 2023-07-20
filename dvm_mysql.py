import mysql.connector
import streamlit as st
import pandas as pd
from mysql.connector import errorcode



# Establish MySQL connection
@st.cache_resource()
def dvm_conn():
    cnx = mysql.connector.connect(**st.secrets["MySQL"])
    return cnx


# Gets the count of a dvm table
@st.cache_data(ttl=1200)
def get_dvm_count(table: str, time:int):
    table_name = table.replace('S_DVM_', '').lower()
    conn= dvm_conn()
    
    try:
        query = f"SELECT count(id) as dvm_count from dvm.{table_name} where lastupd_ts >= date_sub(now(),interval {time} hour)"
        df = pd.read_sql(query, con=conn)
    except:
        query = f"SELECT count(id) as dvm_count from dvm.{table_name} where created_date >= date_sub(now(),interval {time} hour)"
        df = pd.read_sql(query, con=conn)
    
    return df['dvm_count'].iloc[0]


# Gets the count of records inserted per hour in the past 24 hours
@st.cache_data(ttl=1200)
def records_timeseries_dvm(table: str):
    table_name = table.replace('S_DVM_', '').lower()
    conn= dvm_conn()
    
    try:
        query = f"SELECT DATE_FORMAT(lastupd_ts, '%Y-%m-%d %H:00:00') AS HOUR_OF_DAY, count(id) as DVM_RECORDS FROM dvm.{table_name} where lastupd_ts >= date_sub(now(), interval 24 hour) group by EXTRACT(HOUR FROM lastupd_ts) order by lastupd_ts DESC;"
        df = pd.read_sql(query, con=conn)
    except:
        query = f"SELECT DATE_FORMAT(created_date, '%Y-%m-%d %H:00:00') AS HOUR_OF_DAY, count(id) as DVM_RECORDS FROM dvm.{table_name} where created_date >= date_sub(now(), interval 24 hour) group by EXTRACT(HOUR FROM created_date) order by created_date DESC;"
        df = pd.read_sql(query, con=conn)
    
    return df








