
"""Batch ingestion: Load all CSVs from data/raw into MySQL staging tables with incremental loading.

This script implements incremental loading by:
1. Using composite primary keys to detect duplicates
2. INSERT IGNORE strategy to skip existing records
3. Efficient temporary table approach for batch processing
4. Automatic schema detection and table creation

Usage:
    python ingestion/from_file.py --db_user root --db_password password --db_name retail_analytics
"""

import argparse
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus
import time

# Load environment variables from .env file
load_dotenv()

def safe_count_records(engine, table):
    """Safely count records with timeout handling"""
    try:
        # Set a timeout for the query
        with engine.begin() as conn:
            conn.execute(text("SET SESSION MAX_EXECUTION_TIME=10000"))  # 10 second timeout
            result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
            return result.fetchone()[0]
    except Exception as e:
        print(f"Warning: Could not count records in {table}: {e}")
        return 0

def safe_check_table_exists(engine, table):
    """Safely check if table exists"""
    try:
        with engine.begin() as conn:
            result = conn.execute(text(f"SHOW TABLES LIKE '{table}'"))
            return result.fetchone() is not None
    except Exception as e:
        print(f"Warning: Could not check if table {table} exists: {e}")
        return False

def load_csvs_to_mysql(raw_dir: Path, engine):
    """Load CSV files incrementally into MySQL staging tables.
    
    Args:
        raw_dir: Path to directory containing CSV files
        engine: SQLAlchemy engine connected to MySQL database
        
    The incremental loading strategy:
    1. Define business keys (natural primary keys) and updatable columns for each table
    2. Use INSERT ... ON DUPLICATE KEY UPDATE for true upsert functionality
    3. Insert new records when business key doesn't exist
    4. Update existing records when business key matches but other columns changed
    5. Process data in chunks for memory efficiency
    6. Track changes with ingestion_ts for data lineage
    """
    # Define business keys (natural primary keys) and updatable columns
    table_config = {
        "stg_sales": {
            "primary_keys": ["Store", "Dept", "Date"],
            "update_columns": ["Weekly_Sales", "IsHoliday"]
        },
        "stg_stores": {
            "primary_keys": ["Store"],
            "update_columns": ["Type", "Size"]
        },
        "stg_features": {
            "primary_keys": ["Store", "Date"],
            "update_columns": ["Temperature", "Fuel_Price", "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5", "CPI", "Unemployment", "IsHoliday"]
        },
    }

    for csv_path in raw_dir.glob("*.csv"):
        table = f"stg_{csv_path.stem.lower()}"
        config = table_config.get(table)

        if not config:
            print(f"Skipping {csv_path.name}: No configuration defined for table {table}.")
            continue
            
        pk = config["primary_keys"]
        update_cols = config["update_columns"]

        print(f"Processing {csv_path.name} for table {table}...")

        # Check if table exists safely
        table_exists = safe_check_table_exists(engine, table)
        
        initial_count = 0
        has_primary_key = False
        
        if table_exists:
            # Try to get initial count with timeout
            initial_count = safe_count_records(engine, table)
            
            # Check if primary key exists
            try:
                with engine.begin() as conn:
                    pk_check = conn.execute(text(f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'")).fetchone()
                    has_primary_key = pk_check is not None
            except Exception as e:
                print(f"Warning: Could not check primary key for {table}: {e}")
                has_primary_key = False
            
            print(f"Table '{table}' exists with {initial_count} records. Primary key: {'Yes' if has_primary_key else 'No'}")
            
            # If table has too many records or no primary key, drop and recreate it
            if initial_count > 1000000 or (not has_primary_key and initial_count > 10000):
                print(f"Table {table} appears to be corrupted or too large. Dropping and recreating...")
                with engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                table_exists = False
                initial_count = 0
                has_primary_key = False
            elif not has_primary_key and initial_count < 50000:  # Safety limit
                print(f"Adding missing primary key to {table}...")
                with engine.begin() as conn:
                    # For TEXT columns like Date, we need to specify a key length
                    pk_columns = []
                    for col in pk:
                        if col.lower() == 'date':
                            pk_columns.append(f"`{col}`(20)")
                        else:
                            pk_columns.append(f"`{col}`")
                    
                    pk_sql = f"ALTER TABLE {table} ADD PRIMARY KEY ({','.join(pk_columns)});"
                    try:
                        conn.execute(text(pk_sql))
                        print(f"Primary key added to '{table}'.")
                        has_primary_key = True
                    except Exception as e:
                        print(f"Warning: Could not add primary key to {table}: {e}")
        else:
            print(f"Table '{table}' does not exist. It will be created.")

        chunk_size = 50000  # Increased chunk size for fewer, larger operations
        total_new_records = 0
        
        for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
            # Always add the timestamp
            chunk['ingestion_ts'] = pd.to_datetime(pd.Timestamp.now())

            # If the table doesn't exist, create it with the first chunk and set the primary key
            if not table_exists and i == 0:
                print("Table does not exist. Performing initial load with first chunk...")
                chunk.to_sql(table, con=engine, if_exists='replace', index=False, method='multi')
                
                with engine.begin() as conn:
                    # For TEXT columns like Date, we need to specify a key length
                    pk_columns = []
                    for col in pk:
                        if col.lower() == 'date':
                            pk_columns.append(f"`{col}`(20)")  # Limit Date to first 20 chars
                        else:
                            pk_columns.append(f"`{col}`")
                    
                    pk_sql = f"ALTER TABLE {table} ADD PRIMARY KEY ({','.join(pk_columns)});"
                    try:
                        conn.execute(text(pk_sql))
                        print(f"Primary key created for table '{table}'.")
                        has_primary_key = True
                    except Exception as e:
                        print(f"Warning: Could not create primary key for {table}: {e}")
                        print("Proceeding without primary key - duplicates may occur.")
                
                total_new_records += len(chunk)
                table_exists = True  # Mark as existing for subsequent chunks
                continue

            # Check if we have a primary key for duplicate detection
            if not has_primary_key:
                print(f"Skipping chunk {i+1}: No primary key exists for duplicate detection. Please fix table structure first.")
                continue
                
            # --- Direct INSERT IGNORE for better performance ---
            print(f"Chunk {i+1}: Processing {len(chunk)} records...")
            
            # Build the column list for the INSERT statement
            db_columns = "`, `".join(chunk.columns)
            
            # Create a temporary table to batch insert data
            temp_table_name = f"temp_{table}_{os.urandom(8).hex()}"
            
            # Count initial records for tracking (with timeout)
            initial_table_count = safe_count_records(engine, table)
            
            # Load chunk to temporary table and insert with duplicate handling
            chunk.to_sql(temp_table_name, con=engine, if_exists='replace', index=False, method='multi')
            
            with engine.begin() as conn:
                # Use INSERT ... ON DUPLICATE KEY UPDATE for true upsert functionality
                # This will insert new records or update existing ones when business key matches
                
                # Build the UPDATE clause for non-key columns
                update_clauses = []
                for col in update_cols:
                    if col in chunk.columns:  # Only update if column exists in data
                        update_clauses.append(f"`{col}` = VALUES(`{col}`)") 
                
                # Always update ingestion_ts to track when record was last modified
                update_clauses.append("`ingestion_ts` = VALUES(`ingestion_ts`)")
                
                update_clause = ", ".join(update_clauses)
                
                insert_sql = f"""
                INSERT INTO {table} (`{db_columns}`)
                SELECT `{db_columns}` FROM {temp_table_name}
                ON DUPLICATE KEY UPDATE {update_clause}
                """
                
                # Execute the upsert
                conn.execute(text(insert_sql))
                
                # Drop the temporary table
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            
            # Count final records to determine changes (with timeout)
            final_table_count = safe_count_records(engine, table)
            net_change = final_table_count - initial_table_count
            
            total_new_records += net_change
            if net_change > 0:
                print(f"Chunk {i+1}: Added {net_change} new records.")
            elif net_change == 0:
                print(f"Chunk {i+1}: Updated existing records (no net change in count).")
            else:
                print(f"Chunk {i+1}: Unexpected count change: {net_change}")

        final_count = safe_count_records(engine, table)
        print(f"Load for {csv_path.name} complete. Initial: {initial_count}, Final: {final_count}, New: {total_new_records}")

    print("All CSV loading tasks are complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_user", default=os.getenv("DB_USER"))
    parser.add_argument("--db_password", default=os.getenv("DB_PASSWORD"))
    parser.add_argument("--db_name", default=os.getenv("DB_NAME", "retail_analytics"))
    parser.add_argument("--db_host", default=os.getenv("DB_HOST"))
    args = parser.parse_args()

    # URL-encode the password to handle special characters like @
    encoded_password = quote_plus(args.db_password)
    engine = create_engine(f"mysql+pymysql://{args.db_user}:{encoded_password}@{args.db_host}/{args.db_name}")
    load_csvs_to_mysql(Path(__file__).parents[1] / "data" / "raw", engine)
