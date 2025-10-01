import pandas as pd
import joblib
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv
import openmeteo_requests
import requests_cache
from retry_requests import retry
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


def get_current_data_date():
    todays_date = datetime.utcnow().date() - timedelta(days=3)
    return todays_date

def get_eia_time_range(todays_date):
    start_day = todays_date.strftime("%Y-%m-%dT00")
    end_day = (todays_date + timedelta(days=1)).strftime("%Y-%m-%dT00")
    return start_day, end_day

def get_weather_date_range(todays_date):
    date_str = todays_date.strftime("%Y-%m-%d")
    return date_str, date_str

# Load .env file
load_dotenv()
API_KEY = os.getenv("EIA_API_KEY")
if not API_KEY:
    raise ValueError("API key not found")

todays_date = get_current_data_date()
formatted_eia_time_start, formatted_eia_time_end, = get_eia_time_range(todays_date)
start_date, end_date = get_weather_date_range(todays_date)

# Pull Energy Data
eia_url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
eia_params = {
    "api_key": API_KEY,
    "frequency": "hourly",
    "data[0]": "value",
    "facets[respondent][0]": "NYIS",
    "start": formatted_eia_time_start,
    "end": formatted_eia_time_end,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": 0,
    "length": 5000
}
eia_response = requests.get(eia_url, params=eia_params)

# Convert response to JSON
eia_json = eia_response.json()

# Convert to DataFrame
df_eia = pd.DataFrame(eia_json["response"]["data"])
df_eia = df_eia[["period", "fueltype", "value"]]
df_eia = df_eia.pivot(index="period", columns="fueltype", values="value").reset_index()

#  Pull Weather Data
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

cities = {
    "NYC": (40.7128, -74.0060),
    "Albany": (42.6526, -73.7562),
    "Buffalo": (42.8864, -78.8784),
    "Syracuse": (43.0481, -76.1474),
    "Rochester": (43.1566, -77.6088)
}
hourly_vars = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "cloud_cover", "precipitation"]
weather_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"

weather_frames = []
for city_name, (lat, lon) in cities.items():
    print(f"Fetching weather for {city_name}...")
    weather_params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": hourly_vars,
        "timezone": "America/New_York"
    }
    responses = openmeteo.weather_api(weather_url, params=weather_params)
    response = responses[0]
    hourly = response.Hourly()
    date_range = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
    city_df = pd.DataFrame({
        "period": date_range,
        f"temp_{city_name}": hourly.Variables(0).ValuesAsNumpy(),
        f"humidity_{city_name}": hourly.Variables(1).ValuesAsNumpy(),
        f"wind_{city_name}": hourly.Variables(2).ValuesAsNumpy(),
        f"cloud_{city_name}": hourly.Variables(3).ValuesAsNumpy(),
        f"precip_{city_name}": hourly.Variables(4).ValuesAsNumpy()
    })
    weather_frames.append(city_df)

#  Combine all City Data
df_weather = weather_frames[0]
for frame in weather_frames[1:]:
    df_weather = pd.merge(df_weather, frame, on="period", how="inner")
del weather_frames

# Merge on Peroid 
df_eia["period"] = pd.to_datetime(df_eia["period"], utc=True)
df_weather["period"] = pd.to_datetime(df_weather["period"], utc=True)
df_merged = pd.merge(df_eia, df_weather, on="period", how="inner")

# Prediction, drop period to columns get an array of features
features = df_merged.drop(columns=["period"])
model = joblib.load("notebook/model/anomaly_model.pkl")
predictions = model.predict(features)
del df_weather


# Prep for plotting 
df_merged["prediction"] = predictions
df_eia = pd.merge(df_eia, df_merged[["period", "prediction"]], on="period", how="left")
df_eia = df_eia.dropna(subset=["prediction"])
df_eia = df_eia.reset_index(drop=True)

df_eia["hour"] = df_eia["period"].dt.hour
cols_to_plot = ["NG", "NUC", "COL", "SUN", "WAT", "WND"]
df_eia[cols_to_plot] = df_eia[cols_to_plot].apply(pd.to_numeric, errors='coerce')

heatmap_df = df_eia.pivot_table(
    index="hour",           
    values=cols_to_plot,    
    aggfunc="mean",  
).T  

plt.figure(figsize=(16, 8))
sns.heatmap(heatmap_df, annot=True, fmt=".0f", cmap="YlGnBu")

plt.title("Generation by Hour")
plt.xlabel("Hour (UTC)")
plt.ylabel("Generation Type")
plt.tight_layout()
plt.show()
