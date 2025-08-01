
"""Batch ingestion: Load all CSVs from data/raw into MySQL staging tables.

Usage:
    python ingestion/from_file.py --db_user root --db_password password --db_name retail
"""

import argparse
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables from .env file
load_dotenv()

def load_csvs_to_mysql(raw_dir: Path, engine):
    for csv_path in raw_dir.glob("*.csv"):
        table = f"stg_{csv_path.stem.lower()}"
        print(f"Loading {csv_path.name} -> {table}")
        df = pd.read_csv(csv_path)
        df.to_sql(table, con=engine, if_exists="replace", index=False)
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
    print("CSV load complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_user", default=os.getenv("DB_USER"))
    parser.add_argument("--db_password", default=os.getenv("DB_PASSWORD"))
    parser.add_argument("--db_name", default=os.getenv("DB_NAME"))
    parser.add_argument("--db_host", default=os.getenv("DB_HOST"))
    args = parser.parse_args()

    # URL-encode the password to handle special characters like @
    encoded_password = quote_plus(args.db_password)
    engine = create_engine(f"mysql+pymysql://{args.db_user}:{encoded_password}@{args.db_host}/{args.db_name}")
    load_csvs_to_mysql(Path(__file__).parents[1] / "data" / "raw", engine)
