
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
        
        # Read new data
        print(f"Reading {csv_path.name}...")
        df = pd.read_csv(csv_path)
        df['ingestion_ts'] = pd.Timestamp.now()
        
        # Check if table exists
        with engine.begin() as conn:
            table_exists = conn.execute(text(f"SHOW TABLES LIKE '{table}'")).fetchone()
        
        if table_exists:
            # TRUE INCREMENTAL: Load only new data
            print(f"Table {table} exists - performing true incremental load...")
            initial_count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", engine).iloc[0]['count']
            
            # Get existing records to compare
            existing_df = pd.read_sql(f"SELECT * FROM {table}", engine)
            
            # Identify new records (records in CSV that don't exist in DB)
            # Use first column as primary key for comparison
            pk_column = df.columns[0]
            
            if not existing_df.empty:
                # Get existing primary keys
                existing_keys = set(existing_df[pk_column].astype(str))
                # Filter CSV data to only new records
                new_records = df[~df[pk_column].astype(str).isin(existing_keys)]
                print(f"Found {len(new_records)} new records out of {len(df)} total records")
                
                if len(new_records) > 0:
                    # Load only new records
                    new_records.to_sql(table, con=engine, if_exists="append", index=False)
                    final_count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", engine).iloc[0]['count']
                    print(f"True incremental load complete: {initial_count} → {final_count} records (+{len(new_records)} new)")
                else:
                    print("No new records found - skipping load")
                    final_count = initial_count
            else:
                # Table exists but is empty, load all data
                df.to_sql(table, con=engine, if_exists="append", index=False)
                final_count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", engine).iloc[0]['count']
                print(f"Empty table - loaded all {len(df)} records")
            
        else:
            # FULL RELOAD: Create table from scratch
            print(f"Table {table} doesn't exist - performing full reload...")
            df.to_sql(table, con=engine, if_exists="replace", index=False)
            
            # Add ingestion timestamp column if not already present
            with engine.begin() as conn:
                try:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
                except:
                    pass  # Column might already exist
            
            final_count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", engine).iloc[0]['count']
            print(f"Full reload complete: {final_count} records loaded")
    
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
