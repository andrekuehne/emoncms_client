# %%
import os
import sys
import pandas as pd
from dotenv import load_dotenv
sys.path.append("../src")

from emoncms_client import EmonCMSClient

# --- Configuration ---
# IMPORTANT: Set these variables to match your EmonCMS setup and desired query
EMONCMS_URL = "http://emonpi"  # Replace with your EmonCMS server URL
# get api keys from .env file
load_dotenv()
API_KEY_RO = os.getenv("API_KEY_RO") # Read-only
API_KEY_RW = os.getenv("API_KEY_RW") # R/W

# Specify the feed criteria:
FEED_NAME = "electric_Power" # Replace with the exact NAME of the feed you want to query
# Set FEED_TAG to a string to filter by tag as well.
# Set to None or "" to filter only by name.
FEED_TAG = "arotherm_plus_105"  # Replace with the exact TAG or set to None/""

START_DATE = "2025-02-06 00:00:00" # Replace with your desired start date (YYYY-MM-DD or other PHP compatible string)
END_DATE = "2025-02-07 00:00:00"   # Replace with your desired end date (YYYY-MM-DD or other PHP compatible string, e.g., "now")
INTERVAL = 3600 # Interval in seconds for aggregation (86400 for daily, 3600 for hourly, etc.)
# --- End Configuration ---

# --- Test Script to read data---
client = EmonCMSClient(server_url=EMONCMS_URL, api_key=API_KEY_RO)
client.get_feed_data_by_name(
    feed_name=FEED_NAME,
    tag=FEED_TAG,
    start_time=START_DATE,
    end_time=END_DATE,
    interval=INTERVAL,
    average=True,
)
# %% create new feed
client = EmonCMSClient(server_url=EMONCMS_URL, api_key=API_KEY_RW, read_write = True)
NEW_FEED_NAME = "TEST_3"
NEW_FEED_TAG = "TESTTAG"
client.create_feed(name = NEW_FEED_NAME,
                   tag = NEW_FEED_TAG,
                   engine = 5,
                   options = {"interval":30})
# %%
