# %% imports
import sys
import os
from datetime import datetime
from influxdb_client import InfluxDBClient
import pandas as pd
from dotenv import load_dotenv
sys.path.append("../src")

from emoncms_client import EmonCMSClient

# %% Get data from influxdb
# === CONFIG ===
load_dotenv()
url = "http://192.168.178.121:38086"  # or your InfluxDB URL
token = os.getenv("INFLUX_TOKEN")
org = os.getenv("INFLUX_ORG")
bucket = "Home_Assistant"

# === TIME RANGE ===
start_time = datetime(2024, 2, 1, 0, 0, 0)
stop_time = datetime(2025, 4, 15, 0, 0, 0)

# Format as RFC3339 strings
start_str = start_time.isoformat() + "Z"
stop_str = stop_time.isoformat() + "Z"

# === QUERY ===
# query = f'''
# from(bucket: "Home_Assistant")
#   |> range(start: {start_str}, stop: {stop_str})
#   |> filter(fn: (r) => r["entity_id"] == "ecowitt_solarradiation")
#   |> filter(fn: (r) => r["_field"] == "value")
#   |> filter(fn: (r) => r["_measurement"] == "W/m²")
#   |> filter(fn: (r) => r["domain"] == "sensor")
#   |> filter(fn: (r) => r["friendly_name"] == "WS2900 solarradiation" or r["friendly_name"] == "WS2900 ecowitt_solarradiation")
#   |> filter(fn: (r) => r["source"] == "HA")
#   |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
#   |> yield(name: "mean")
# '''
query = f'''
from(bucket: "Home_Assistant")
  |> range(start: {start_str}, stop: {stop_str})
  |> filter(fn: (r) => r["entity_id"] == "ecowitt_windspeed")
  |> filter(fn: (r) => r["_field"] == "value")
  |> filter(fn: (r) => r["source"] == "HA")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
  |> yield(name: "mean")
'''

# === RUN QUERY ===
client_influx = InfluxDBClient(url=url, token=token, org=org)
query_api = client_influx.query_api()
df = query_api.query_data_frame(query)
df_clean = (
    df[["_time", "_value"]]
    .rename(columns={"_time": "time", "_value": "value"})
    .sort_values("time")
    .drop_duplicates("time")  # keep first by default
    .reset_index(drop=True)
)

# Ensure timezone-aware datetime (UTC), then convert to UNIX seconds
df_clean["time"] = pd.to_datetime(df_clean["time"], utc=True)
df_clean["time"] = df_clean["time"].astype(int) // 10**9  # convert to seconds

# %% create new feed
EMONCMS_URL = "http://emonpi"  # Replace with your EmonCMS server URL
API_KEY_RW = os.getenv("API_KEY_RW") # R/W
client_emon = EmonCMSClient(server_url=EMONCMS_URL, api_key=API_KEY_RW, read_write = True)
NEW_FEED_NAME = "wind_speed"
NEW_FEED_TAG = "ecowitt"
feed_id = client_emon.create_feed(name = NEW_FEED_NAME,
                   tag = NEW_FEED_TAG,
                   engine = 5, # PHPFINA
                   options = {"interval":60})

# %% fill feed with previously read data
client_emon.insert_multiple_data_points(feed_id=feed_id, data=df_clean)

# %%
