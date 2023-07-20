from datetime import datetime
import streamlit as st
import pandas as pd
import json
import time
import requests


auth_token = st.secrets["splunk"]["token"]
base_url = "https://directvetmarketing.splunkcloud.com:8089/services"


def get_search_id(table_name: str, earliest_time: str, latest_time:str = "now", status_buckets:int = 300) -> str:
    endpoint = base_url + "/search/jobs"
    
    headers = {"Authorization": f"Bearer {auth_token}",
               "Content-Type": "application/x-www-form-urlencoded",
               "Connection": "keep-alive"}
    
    payload = {
        "id": f"{table_name}_001",
        "search": f"| savedsearch {table_name}",
        "earliest_time": earliest_time,
        "latest_time": latest_time,
        "status_buckets": status_buckets,
        "output_mode": "json"
        }
    
    response = requests.post(url=endpoint, headers=headers, data=payload)
    return response.json()['sid']


def get_search_dispatchState(search_id: str) -> str:
    endpoint = base_url + f"/search/jobs/{search_id}"
    headers = {"Authorization": f"Bearer {auth_token}"}
    
    payload = {"output_mode": "json"}
    
    response = requests.get(url=endpoint, headers=headers, params=payload)
    return response.json()['entry'][0]['content']['dispatchState']


def get_events_summary(search_id: str) -> int:
    while(get_search_dispatchState(search_id) != "DONE"):
        time.sleep(1)
    
    endpoint = base_url + f"/search/v2/jobs/{search_id}/summary"
    headers = {"Authorization": f"Bearer {auth_token}"}
    
    payload = {"output_mode": "json"}
    
    response = requests.get(url=endpoint, headers=headers, params=payload)
    return response.json()['event_count']


def get_events(search_id: str) -> json:
    while(get_search_dispatchState(search_id) != "DONE"):
        time.sleep(1)
    
    endpoint = base_url + f"/search/v2/jobs/{search_id}/results"
    headers = {"Authorization": f"Bearer {auth_token}",
               "Content-Type": "application/json"}
    
    payload = {"output_mode": "json"}
    
    response = requests.get(url=endpoint, headers=headers, params=payload)
    return response


def get_events_timeline(search_id: str) -> pd.DataFrame:
    while(get_search_dispatchState(search_id) != "DONE"):
        time.sleep(1)
    
    endpoint = base_url + f"/search/v2/jobs/{search_id}/timeline"
    headers = {"Authorization": f"Bearer {auth_token}",
               "Content-Type": "application/json"}
    
    payload = {"output_mode": "json"}
    
    response = requests.get(url=endpoint, headers=headers, params=payload)
    
    buckets = response.json()['buckets']
    bucket_data = [
    {
        "total_count": bucket["total_count"],
        "earliest_strftime": datetime.strptime(bucket["earliest_strftime"], "%Y-%m-%dT%H:%M:%S.%f+00:00").strftime("%Y-%m-%d %H:%M:%S")
    }
    for bucket in buckets
    ]
    
    # Create pandas DataFrame from the extracted data
    df = pd.DataFrame(bucket_data)
    
    return df