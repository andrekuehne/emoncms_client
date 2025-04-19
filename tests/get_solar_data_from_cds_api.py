# %% Import
import cdsapi
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import os
import sys
from dotenv import load_dotenv
sys.path.append("../src")

from emoncms_client import EmonCMSClient
# %% User input
lat_center = 52.601
lon_center = 13.456
delta = 0.002  # Half-width of bounding box in degrees
start_date = '2024-02-01'
end_date = '2025-04-30'
output_file = f"../data/solar_{start_date}_{end_date}.nc"

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

    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': 'surface_solar_radiation_downwards',
            'year': str(year),
            'month': f'{month:02d}',
            'day': [f'{d:02d}' for d in days],
            'time': [f'{h:02d}:00' for h in range(24)],
            'area': area,
        },
        f'../data/solar_{year}{month:02d}.nc'
    )

# %% Combine all files into one
ds = xr.open_mfdataset("../data/solar_20*.nc", combine="by_coords")
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
    'irradiance_w2m': 'value'
})

# %% create new feed in emoncms
EMONCMS_URL = "http://emonpi"  # Replace with your EmonCMS server URL
API_KEY_RW = os.getenv("API_KEY_RW") # R/W
client_emon = EmonCMSClient(server_url=EMONCMS_URL, api_key=API_KEY_RW, read_write = True)
NEW_FEED_NAME = "wind_speed"
NEW_FEED_TAG = "ecowitt"
feed_id = client_emon.create_feed(name = NEW_FEED_NAME,
                   tag = NEW_FEED_TAG,
                   engine = 5, # PHPFINA
                   options = {"interval":3600})

# %% fill feed with previously read data
client_emon.insert_multiple_data_points(feed_id=feed_id, data=df_clean)
