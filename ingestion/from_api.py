
"""Example API ingestion script that fetches dummy sales data and loads it into MySQL.

Replace API_ENDPOINT with a real endpoint when available.
"""
import argparse, requests, pandas as pd
from sqlalchemy import create_engine

API_ENDPOINT = "https://dummyjson.com/products"  # placeholder public API

def fetch_api() -> pd.DataFrame:
    resp = requests.get(API_ENDPOINT, timeout=30)
    resp.raise_for_status()
    data = resp.json()["products"]
    return pd.json_normalize(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_user", default="root")
    parser.add_argument("--db_password", default="password")
    parser.add_argument("--db_name", default="retail")
    parser.add_argument("--db_host", default="127.0.0.1")
    args = parser.parse_args()

    df = fetch_api()
    engine = create_engine(f"mysql+pymysql://{args.db_user}:{args.db_password}@{args.db_host}/{args.db_name}")
    df.to_sql("stg_api_products", con=engine, if_exists="replace", index=False)
    print("API data loaded to stg_api_products")
