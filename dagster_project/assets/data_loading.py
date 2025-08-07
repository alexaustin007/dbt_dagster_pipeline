from dagster import asset, get_dagster_logger, Field, Int, String
from sqlalchemy import text
import pandas as pd
import os
from pathlib import Path
from typing import Dict, List

data_loading_config = {
    "chunk_size": Field(Int, default_value=50000, is_required=False),
    "raw_data_path": Field(String, default_value="data/raw", is_required=False),
}

@asset(
    config_schema=data_loading_config,
    description="Load sales.csv data into stg_sales table with incremental loading",
    tags={"data_source": "sales", "table": "stg_sales"},
    required_resource_keys={"mysql_resource"}
)
def raw_sales_data(context):
    """Load sales.csv into stg_sales table with incremental loading."""
    logger = get_dagster_logger()
    engine = context.resources.mysql_resource
    
    csv_path = Path(context.op_config['raw_data_path']) / "sales.csv"
    table = "stg_sales"
    
    # Table configuration
    primary_keys = ["Store", "Dept", "Date"]
    update_columns = ["Weekly_Sales", "IsHoliday"]
    
    return _load_csv_to_mysql(
        context, engine, csv_path, table, primary_keys, update_columns
    )

@asset(
    config_schema=data_loading_config,
    description="Load stores.csv data into stg_stores table with incremental loading",
    tags={"data_source": "stores", "table": "stg_stores"},
    required_resource_keys={"mysql_resource"}
)
def raw_stores_data(context):
    """Load stores.csv into stg_stores table with incremental loading."""
    logger = get_dagster_logger()
    engine = context.resources.mysql_resource
    
    csv_path = Path(context.op_config['raw_data_path']) / "stores.csv"
    table = "stg_stores"
    
    # Table configuration
    primary_keys = ["Store"]
    update_columns = ["Type", "Size"]
    
    return _load_csv_to_mysql(
        context, engine, csv_path, table, primary_keys, update_columns
    )

@asset(
    config_schema=data_loading_config,
    description="Load features.csv data into stg_features table with incremental loading",
    tags={"data_source": "features", "table": "stg_features"},
    required_resource_keys={"mysql_resource"}
)
def raw_features_data(context):
    """Load features.csv into stg_features table with incremental loading."""
    logger = get_dagster_logger()
    engine = context.resources.mysql_resource
    
    csv_path = Path(context.op_config['raw_data_path']) / "features.csv"
    table = "stg_features"
    
    # Table configuration
    primary_keys = ["Store", "Date"]
    update_columns = ["Temperature", "Fuel_Price", "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5", "CPI", "Unemployment", "IsHoliday"]
    
    return _load_csv_to_mysql(
        context, engine, csv_path, table, primary_keys, update_columns
    )

@asset(
    description="Composite asset indicating all raw data has been loaded",
    deps=[raw_sales_data, raw_stores_data, raw_features_data],
    tags={"composite": "true"}
)
def all_raw_data_loaded():
    """Composite asset that depends on all raw data loading assets."""
    return "All raw data successfully loaded to staging tables"

def _safe_count_records(engine, table: str) -> int:
    """Safely count records with timeout handling."""
    try:
        with engine.begin() as conn:
            conn.execute(text("SET SESSION MAX_EXECUTION_TIME=10000"))  # 10 second timeout
            result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
            return result.fetchone()[0]
    except Exception as e:
        get_dagster_logger().warning(f"Could not count records in {table}: {e}")
        return 0

def _safe_check_table_exists(engine, table: str) -> bool:
    """Safely check if table exists."""
    try:
        with engine.begin() as conn:
            result = conn.execute(text(f"SHOW TABLES LIKE '{table}'"))
            return result.fetchone() is not None
    except Exception as e:
        get_dagster_logger().warning(f"Could not check if table {table} exists: {e}")
        return False

def _load_csv_to_mysql(context, engine, csv_path: Path, table: str, primary_keys: List[str], update_columns: List[str]):
    """Load CSV file incrementally into MySQL staging table."""
    logger = get_dagster_logger()
    chunk_size = context.op_config['chunk_size']
    
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    logger.info(f"Processing {csv_path.name} for table {table}...")
    
    # Check if table exists safely
    table_exists = _safe_check_table_exists(engine, table)
    
    initial_count = 0
    has_primary_key = False
    
    if table_exists:
        # Try to get initial count with timeout
        initial_count = _safe_count_records(engine, table)
        
        # Check if primary key exists
        try:
            with engine.begin() as conn:
                pk_check = conn.execute(text(f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'")).fetchone()
                has_primary_key = pk_check is not None
        except Exception as e:
            logger.warning(f"Could not check primary key for {table}: {e}")
            has_primary_key = False
        
        logger.info(f"Table '{table}' exists with {initial_count} records. Primary key: {'Yes' if has_primary_key else 'No'}")
        
        # If table has too many records or no primary key, drop and recreate it
        if initial_count > 1000000 or (not has_primary_key and initial_count > 10000):
            logger.info(f"Table {table} appears to be corrupted or too large. Dropping and recreating...")
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            table_exists = False
            initial_count = 0
            has_primary_key = False
        elif not has_primary_key and initial_count < 50000:  # Safety limit
            logger.info(f"Adding missing primary key to {table}...")
            with engine.begin() as conn:
                # For TEXT columns like Date, we need to specify a key length
                pk_columns = []
                for col in primary_keys:
                    if col.lower() == 'date':
                        pk_columns.append(f"`{col}`(20)")
                    else:
                        pk_columns.append(f"`{col}`")
                
                pk_sql = f"ALTER TABLE {table} ADD PRIMARY KEY ({','.join(pk_columns)});"
                try:
                    conn.execute(text(pk_sql))
                    logger.info(f"Primary key added to '{table}'.")
                    has_primary_key = True
                except Exception as e:
                    logger.warning(f"Could not add primary key to {table}: {e}")
    else:
        logger.info(f"Table '{table}' does not exist. It will be created.")

    total_new_records = 0
    
    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
        # Always add the timestamp
        chunk['ingestion_ts'] = pd.to_datetime(pd.Timestamp.now())

        # If the table doesn't exist, create it with the first chunk and set the primary key
        if not table_exists and i == 0:
            logger.info("Table does not exist. Performing initial load with first chunk...")
            chunk.to_sql(table, con=engine, if_exists='replace', index=False, method='multi')
            
            with engine.begin() as conn:
                # For TEXT columns like Date, we need to specify a key length
                pk_columns = []
                for col in primary_keys:
                    if col.lower() == 'date':
                        pk_columns.append(f"`{col}`(20)")  # Limit Date to first 20 chars
                    else:
                        pk_columns.append(f"`{col}`")
                
                pk_sql = f"ALTER TABLE {table} ADD PRIMARY KEY ({','.join(pk_columns)});"
                try:
                    conn.execute(text(pk_sql))
                    logger.info(f"Primary key created for table '{table}'.")
                    has_primary_key = True
                except Exception as e:
                    logger.warning(f"Could not create primary key for {table}: {e}")
                    logger.warning("Proceeding without primary key - duplicates may occur.")
            
            total_new_records += len(chunk)
            table_exists = True  # Mark as existing for subsequent chunks
            continue

        # Check if we have a primary key for duplicate detection
        if not has_primary_key:
            logger.warning(f"Skipping chunk {i+1}: No primary key exists for duplicate detection. Please fix table structure first.")
            continue
            
        # Direct INSERT IGNORE for better performance
        logger.info(f"Chunk {i+1}: Processing {len(chunk)} records...")
        
        # Build the column list for the INSERT statement
        db_columns = "`, `".join(chunk.columns)
        
        # Create a temporary table to batch insert data
        temp_table_name = f"temp_{table}_{os.urandom(8).hex()}"
        
        # Count initial records for tracking (with timeout)
        initial_table_count = _safe_count_records(engine, table)
        
        # Load chunk to temporary table and insert with duplicate handling
        chunk.to_sql(temp_table_name, con=engine, if_exists='replace', index=False, method='multi')
        
        with engine.begin() as conn:
            # Use INSERT ... ON DUPLICATE KEY UPDATE for true upsert functionality
            # This will insert new records or update existing ones when business key matches
            
            # Build the UPDATE clause for non-key columns
            update_clauses = []
            for col in update_columns:
                if col in chunk.columns:  # Only update if column exists in data
                    update_clauses.append(f"`{col}` = VALUES(`{col}`)") 
            
            # Always update ingestion_ts to track when record was last modified
            update_clauses.append("`ingestion_ts` = VALUES(`ingestion_ts`)")
            
            update_clause = ", ".join(update_clauses)
            
            insert_sql = f'''
            INSERT INTO {table} (`{db_columns}`)
            SELECT `{db_columns}` FROM {temp_table_name}
            ON DUPLICATE KEY UPDATE {update_clause}
            '''
            
            # Execute the upsert
            conn.execute(text(insert_sql))
            
            # Drop the temporary table
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
        
        # Count final records to determine changes (with timeout)
        final_table_count = _safe_count_records(engine, table)
        net_change = final_table_count - initial_table_count
        
        total_new_records += net_change
        if net_change > 0:
            logger.info(f"Chunk {i+1}: Added {net_change} new records.")
        elif net_change == 0:
            logger.info(f"Chunk {i+1}: Updated existing records (no net change in count).")
        else:
            logger.info(f"Chunk {i+1}: Unexpected count change: {net_change}")

    final_count = _safe_count_records(engine, table)
    logger.info(f"Load for {csv_path.name} complete. Initial: {initial_count}, Final: {final_count}, New: {total_new_records}")
    
    return {
        "table": table,
        "initial_count": initial_count,
        "final_count": final_count,
        "new_records": total_new_records,
        "file_processed": csv_path.name
    }