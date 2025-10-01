import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import seaborn as sns

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in .env")
engine = create_engine(DATABASE_URL)

def load_data():
    eia_data = "SELECT * FROM eia_data WHERE subba = 'ZONJ' "
    weather_data = "SELECT * FROM weather_data WHERE city = 'NYC' "

    eia_df = pd.read_sql(eia_data,engine)
    weather_df = pd.read_sql(weather_data,engine)

    return weather_df, eia_df

def merge_data(eia_df, weather_df):
    df = pd.merge(eia_df, weather_df, on="period", how="inner" )
    df = df.dropna()
    return df

def eda(df):
    print(df.describe())

    # correlation
    plt.figure(figsize=(10,6))
    sns.heatmap(df[['value', 'temp', 'humidity', 'wind', 'cloud', 'precip']].corr(), annot=True, cmap="coolwarm")
    plt.title("Correlation Matrix In NYC: Energy Demand vs Weather")
    plt.show()

    # scatter plot for each weather feature
    features = ['temp', 'humidity', 'wind', 'cloud', 'precip']
    for feature in features:
        plt.figure()
        sns.scatterplot(data=df, x=feature, y='value')
        plt.title(f"Energy Demand vs {feature.capitalize()}")
        plt.xlabel(feature.capitalize())
        plt.ylabel("Energy Demand (MWh)")
        plt.show()

def main():
    eia_df, weather_df = load_data()
    df = merge_data(eia_df, weather_df)
    eda(df)

if __name__ == "__main__":
    main()