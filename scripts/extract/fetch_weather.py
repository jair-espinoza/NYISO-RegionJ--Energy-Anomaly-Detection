import openmeteo_requests
import os
import pandas as pd
import requests_cache
from retry_requests import retry
from dotenv import load_dotenv
from sqlalchemy import create_engine


# --- Load Env Vars. ---
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")

# --- Setup the Open-Meteo API client with cache and retry on error ---
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
start_date = "2020-01-01"
end_date = "2025-07-19"
hourly_vars = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "cloud_cover", "precipitation"]

# Define cities with coordinates
cities = {
    "NYC": (40.7128, -74.0060),
}

# --- Fetch for all Cities ----
def fetech_weather_date():
    weather_frames = []
    for city_name, (lat, lon) in cities.items():
        print(f"Fetching weather for {city_name}...")

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": hourly_vars,
            "timezone": "America/New_York"
        }

        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        hourly = response.Hourly()

        date_range = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )

        # Extract variables
        city_df = pd.DataFrame({
            "period": date_range,
            "temp": hourly.Variables(0).ValuesAsNumpy(),
            "humidity": hourly.Variables(1).ValuesAsNumpy(),
            "wind": hourly.Variables(2).ValuesAsNumpy(),
            "cloud": hourly.Variables(3).ValuesAsNumpy(),
            "precip": hourly.Variables(4).ValuesAsNumpy()
        })

        city_df["city"] = city_name
        weather_frames.append(city_df)
    weather_df = pd.concat(weather_frames, ignore_index=True)
    return weather_df

# --- Insert to PostgresSQL ---
def insert_to_db(weather_frames):
    engine = create_engine(DATABASE_URL)
    weather_frames.to_sql("weather_data", engine, if_exists="append", index=False)
    print("Inserted data into 'weather_data")

# ---- Main ----
def main():
    df = fetech_weather_date()
    print(df.head())
    insert_to_db(df)

if __name__ == "__main__":
    main()