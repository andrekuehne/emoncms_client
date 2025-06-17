# %% Import
import cdsapi
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import os
import sys
import numpy as np
from dotenv import load_dotenv
sys.path.append("../src")

from emoncms_client import EmonCMSClient
# %% User input
lat_center = 52.601
lon_center = 13.456
delta = 0.002  # Half-width of bounding box in degrees
start_date = '2025-04-01'
end_date = '2025-05-01'
dataset = 'reanalysis-era5-land' #'reanalysis-era5-single-levels'
variable = 'surface_solar_radiation_downwards'
# %% Download solar radiation for a date range across months/years
# Bounding box
area = [
    lat_center + delta,
    lon_center - delta,
    lat_center - delta,
    lon_center + delta,
]

# Generate all days and group by year and month
dates = pd.date_range(start=start_date, end=end_date, freq='D')
df_dates = dates.to_frame(index=False, name='date')
df_dates['year'] = df_dates['date'].dt.year
df_dates['month'] = df_dates['date'].dt.month
df_dates['day'] = df_dates['date'].dt.day

c = cdsapi.Client()

for (year, month), group in df_dates.groupby(['year', 'month']):
    days = sorted(group['day'].unique())
    print(f"Fetching {year}-{month:02d}...")


    filename = f'../data/{dataset}_{variable}_{year}{month:02d}.nc'
    request = {
        'variable': [variable],
        'year': str(year),
        'month': f'{month:02d}',
        'day': [f'{d:02d}' for d in days],
        'time': [f'{h:02d}:00' for h in range(24)],
        'data_format': 'netcdf',
        'download_format': 'unarchived',
        'area': area,
        }
    
    c.retrieve(dataset, request, filename)

# %% Combine all files into one
output_file = f"../data/{dataset}_{variable}_{start_date}_{end_date}.nc"
ds = xr.open_mfdataset(f"../data/{dataset}_{variable}*.nc", combine="by_coords")
ds.to_netcdf(output_file)

# %% Load file and  convert to W/m²

ds = xr.open_dataset(output_file)

# Convert from J/m² to W/m² (1 hour = 3600 seconds)
ssrd_wm2 = ds['ssrd'] / 3600

# Average over the tiny grid box
ssrd_point = ssrd_wm2.mean(dim=["latitude", "longitude"])

# Convert to pandas DataFrame
df_hourly = ssrd_point.to_dataframe().rename(columns={'ssrd': 'irradiance_wm2'})
df_hourly.index.name = 'timestamp'
# Convert UTC datetime index to UNIX timestamps (seconds since epoch)
df_hourly['unix_timestamp'] = df_hourly.index.astype('int64') // 10**9

# %% prepare to write to emoncms
df_emoncms = df_hourly[['unix_timestamp', 'irradiance_wm2']].rename(columns={
    'unix_timestamp': 'time',
    'irradiance_wm2': 'value'
})

# %% create new feed in emoncms
EMONCMS_URL = "http://emonpi"  # Replace with your EmonCMS server URL
API_KEY_RW = os.getenv("API_KEY_RW") # R/W
client_emon = EmonCMSClient(server_url=EMONCMS_URL, api_key=API_KEY_RW, read_write = True)
NEW_FEED_NAME = dataset + "_" + variable
NEW_FEED_TAG = "cds"
try:
    feed_id = client_emon.create_feed(name = NEW_FEED_NAME,
                   tag = NEW_FEED_TAG,
                   engine = 5, # PHPFINA
                   options = {"interval":3600})
except:
    # If the feed already exists, get its ID
    feeds = client_emon.list_feeds()
    feed_id = feeds.loc[(feeds['name'] == NEW_FEED_NAME) & (feeds['tag'] == NEW_FEED_TAG), 'id'].values[0]
    print(f"Feed '{NEW_FEED_NAME}' already exists with ID: {feed_id}")


# %% fill feed with previously read data
client_emon.insert_multiple_data_points(feed_id=feed_id, data=df_emoncms)
# %%

# # %% compare era5 vs era5-land
# ds_era5_land = xr.open_dataset("../data/reanalysis-era5-land_surface_solar_radiation_downwards_202402.nc")
# ds_era5 = xr.open_dataset("../data/solar_202402.nc")

# # %%
# np_era5_land = np.roll(np.diff(ds_era5_land['ssrd'].values.squeeze()),1)
# np_era5_land[np_era5_land < 0] = 0
# np_era_5 = ds_era5['ssrd'].values.squeeze()[0:-1]

# plt.plot(np_era5_land, label='era5-land')
# plt.plot(np_era_5, label='era5')
# plt.plot(np_era_5 - np_era5_land, label='era5 - era5-land')
# plt.xlim(0,100)
# plt.legend()
