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

# %%
import xarray as xr
import numpy as np
import statsmodels.api as sm


def convert_accumulated_to_hourly_power(ds: xr.Dataset, var_name: str) -> xr.Dataset:
    """
    Converts accumulated energy data in J/m² to hourly average power in W/m²,
    accounting for daily resets at 00:00.

    Parameters:
        ds (xr.Dataset): Dataset with 'valid_time' coordinate and accumulated energy variable.
        var_name (str): Name of the accumulated variable (e.g., 'str').

    Returns:
        xr.Dataset: Dataset with hourly average power in W/m².
    """
    energy = ds[var_name]
    time = ds['valid_time']

    # Compute the time difference in seconds between timestamps
    delta_t = (time.diff('valid_time') / np.timedelta64(1, 's')).astype('float32')

    # Compute the difference in energy (J/m²) to get hourly energy
    delta_energy = energy.diff('valid_time')

    # Trim time to match the diff'd arrays
    time_mid = time.isel(valid_time=slice(1, None))

    # Identify where hour of *previous* time is not 0 (skip 00:00 values)
    mask = time.shift(valid_time=1).dt.hour.isel(valid_time=slice(1, None)) != 0

    # Apply the mask
    hourly_power = (delta_energy / delta_t).where(mask)
    hourly_power = hourly_power.assign_coords(valid_time=time_mid)
    hourly_power.name = f"{var_name}_power"

    return hourly_power.to_dataset()

def integrate_daily_energy(ds: xr.Dataset, var_name: str) -> xr.Dataset:
    """
    Integrates hourly power data (W/m²) to daily energy (kWh/m²).
    
    Parameters:
        ds (xr.Dataset): Dataset with hourly power values and 'valid_time' coordinate.
        var_name (str): Name of the power variable (e.g., 'str_power').

    Returns:
        xr.Dataset: Dataset with daily energy totals in kWh/m². Timestamps are at 00:00 of each day.
    """
    power = ds[var_name]
    time = ds['valid_time']

    # Convert power to energy per hour (Wh/m²)
    energy_per_hour = power * (1 / 1000)  # W to kW → kWh for 1 hour

    # Group by day
    daily_energy = energy_per_hour.resample(valid_time='1D').sum()

    # Rename the variable
    daily_energy.name = f"{var_name}_daily_energy"

    return daily_energy.to_dataset()


def add_daily_energy_to_excel(excel_path, ds: xr.Dataset, energy_var: str, date_column: str = 'date'):
    """
    Reads an Excel file and adds daily energy values from a 1D time-series xarray dataset.

    Parameters:
        excel_path (str): Path to the Excel file.
        ds (xr.Dataset): Dataset with daily energy values.
        energy_var (str): Variable name in the dataset to extract.
        date_column (str): Name of the date column in the Excel file.

    Returns:
        pd.DataFrame: Updated DataFrame with added energy column.
    """
    import pandas as pd

    # Read Excel
    df = pd.read_excel(excel_path)
    df[date_column] = pd.to_datetime(df[date_column])

    # Ensure we only get a 1D series over time (reduce any spatial dimensions)
    energy_series = ds[energy_var]

    # If there are extra dimensions (e.g. lat/lon), take the first point
    for dim in energy_series.dims:
        if dim != 'valid_time':
            energy_series = energy_series.isel({dim: 0})

    # Convert to DataFrame
    energy_df = energy_series.to_dataframe().reset_index()

    # Merge on date
    merged = pd.merge(df, energy_df[['valid_time', energy_var]],
                      how='left', left_on=date_column, right_on='valid_time')

    return merged.drop(columns=['valid_time'])

def multilinear_regression(df, y_col, x_cols):
    X = df[x_cols]
    X = sm.add_constant(X)  # adds offset/intercept term
    y = df[y_col]
    
    model = sm.OLS(y, X).fit()
    return model

def add_lagged_columns(df, timestamp_col='timestamp', cols_to_lag=None):
    if cols_to_lag is None:
        cols_to_lag = []

    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    df.set_index(timestamp_col, inplace=True)
    df.sort_index(inplace=True)

    # Determine rows where previous date is exactly 1 day before
    date_diff = df.index.to_series().diff() == pd.Timedelta(days=1)

    # Shift and add lagged columns
    for col in cols_to_lag:
        df[f'{col}_lag'] = df[col].shift(1)

    df = df[date_diff]
    return df.reset_index()


# %%
from emoncms_client import EmonCMSClient
# %% User input
lat_center = 52.601
lon_center = 13.456
delta = 0.002  # Half-width of bounding box in degrees
start_date = '2025-02-01'
end_date = '2025-04-30'
dataset = 'reanalysis-era5-land' #'reanalysis-era5-single-levels'
variable = 'surface_solar_radiation_downwards'
variable = 'surface_net_thermal_radiation'
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
var_name = 'ssrd' # Surface solar radiation downwards
var_name = 'str' # Surface net thermal radiation
ds = xr.open_dataset(output_file)
ds_power = convert_accumulated_to_hourly_power(ds, 'str')
ds_energy_daily = integrate_daily_energy(ds_power, 'str_power')
# %%
df_full = add_daily_energy_to_excel('../heat_data.xlsx', ds_energy_daily, 'str_power_daily_energy',date_column='Date')
df_full['dT'] = df_full['T_in'] - df_full['T_out']

# %% regression with normal dT
df = df_full.dropna()
df = add_lagged_columns(df, timestamp_col='Date', cols_to_lag=['dT', 'Solar_kWh', 'Heat_kWh'])
#df = add_lagged_dT(df, timestamp_col='Date', dT_col='Solar_kWh')
y_col='Heat_kWh'
x_cols=['dT', 'Solar_kWh']
X = df[x_cols]
X = sm.add_constant(X)  # adds offset/intercept term
y = df[y_col]/24
model = sm.OLS(y, X).fit()
print(model.summary())

# %% 
y_col='Heat_kWh'
x_cols=['dT', 'Solar_kWh', 'dT_lag']
X = df[x_cols]
X = sm.add_constant(X)  # adds offset/intercept term
y = df[y_col]/24
model_lag_dt = sm.OLS(y, X).fit()
print(model_lag_dt.summary())

# %% combined dT
df['dT_combined'] = (df['dT']*model_lag_dt.params['dT'] + df['dT_lag']*model_lag_dt.params['dT_lag'])/(model_lag_dt.params['dT'] + model_lag_dt.params['dT_lag'])
y_col='Heat_kWh'
x_cols=['dT_combined', 'Solar_kWh']
X = df[x_cols]
X = sm.add_constant(X)  # adds offset/intercept term
y = df[y_col]/24
model_lag_dt = sm.OLS(y, X).fit()
print(model_lag_dt.summary())
# %% 
y_col='Heat_kWh'
x_cols=['dT', 'Solar_kWh', 'dT_lag', 'Solar_kWh_lag']
X = df[x_cols]
X = sm.add_constant(X)  # adds offset/intercept term
y = df[y_col]/24
model_lag_dt_solar = sm.OLS(y, X).fit()
print(model_lag_dt_solar.summary())
# %% autoregressive
y_col='Heat_kWh'
x_cols=['dT', 'Solar_kWh', 'Heat_kWh_lag']
X = df[x_cols]
X = sm.add_constant(X)  # adds offset/intercept term
y = df[y_col]/24
model_lag_heat = sm.OLS(y, X).fit()
print(model_lag_heat.summary())

# %%
y_col='Heat_kWh'
x_cols=['dT', 'Solar_kWh', 'Heat_kWh_lag']
X = df[x_cols]
X_no_const = X.copy()
X = sm.add_constant(X)  # adds offset/intercept term
y = df[y_col]/24
model_lag_all = sm.OLS(y, X).fit()
print(model_lag_all.summary())

from statsmodels.stats.outliers_influence import variance_inflation_factor

vif_df = pd.DataFrame()
vif_df["feature"] = X_no_const.columns
vif_df["VIF"] = [variance_inflation_factor(X_no_const.values, i) for i in range(X_no_const.shape[1])]
print(vif_df)