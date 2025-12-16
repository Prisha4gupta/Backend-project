import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def run_public_etl():
    data = {
        "City_Name": ["New York", "London", "paris", "TOKYO", "New York"],
        "TEMP_c": [25, 15, 18, 22, 25],
        "date": ["2023-01-01"] * 5
    }

    df = pd.DataFrame(data)
    print("Extract: Loaded raw public data")

    df.columns = [c.lower() for c in df.columns]
    df['city_name'] = df['city_name'].str.title()
    df = df.drop_duplicates()

    print("Transform: Cleaned data")

    engine = create_engine(os.getenv("DATABASE_URL"))
    df.to_sql("weather_data", engine, if_exists="replace", index=False)

    print("Load: Saved to table weather_data")

if __name__ == "__main__":
    run_public_etl()
