# Pull Energy Data
import requests
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# Get Postgress URL
def fetch_eia_data(api_key, start_date, end_date, length=5000):
    url = "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/"
    offset = 0 # starting point for pagination
    all_data = []

    # Param for data request
    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[subba][0]": "ZONJ",
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000
    }
    while True:
        params["offset"] = offset
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")

        json_data = response.json()

        # Extract data batch
        batch = json_data.get("response", {}).get("data", [])

        if not batch:
            print("No more data to fetch.")
            break

        all_data.extend(batch)
        print(f"Fetched {len(batch)} records at offset {offset}.")

        if len(batch) < length:
            # Last page reached
            break
        offset += length  # increment offset for next batch
    return pd.DataFrame(all_data)

def main():
    # Load .env file
    load_dotenv()

    # Access Api Key From .env file
    API_KEY = os.getenv("EIA_API_KEY")
    if not API_KEY:
        raise ValueError("EIA_API_KEY not found in .env")
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in .env")

    # Set desired date 
    start_date = "2020-01-01T00"
    end_date = "2025-07-19T00"

    print("begining EIA...")
    df = fetch_eia_data(API_KEY, start_date, end_date)

    if df.empty:
        print("unable to extract information")
        return
    
    df = df[["period", "subba", "value"]]
    df["period"] = pd.to_datetime(df["period"])
                                  
    # Connect to DB and insert
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        df.to_sql("eia_data", conn, if_exists="append", index=False)
        print("Data inserted into table 'eia_data'")

if __name__ == "__main__":
    main()