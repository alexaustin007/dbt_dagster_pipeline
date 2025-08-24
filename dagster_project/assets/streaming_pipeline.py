"""
Dagster assets for complete streaming pipeline orchestration
Manages the entire flow: Kafka → Spark → MySQL → dbt transformations
"""
import subprocess
import time
import pandas as pd
from dagster import asset, AssetMaterialization, MetadataValue, DagsterEventType
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path
import os
import signal
import psutil

# dbt project path
DBT_PROJECT_PATH = Path("/Users/alexaustinchettiar/Downloads/retail_data_pipeline_full/dbt_project")

# Simple asset to run dbt streaming models without creating duplicate assets
@asset(group_name="streaming_transformations")
def run_streaming_dbt(context) -> dict:
    """
    Run dbt streaming models as a single asset to avoid duplicates
    """
    import subprocess
    import os
    
    try:
        # Set environment variables for dbt
        env = os.environ.copy()
        env.update({
            "DB_USER": "root",
            "DB_PASSWORD": "Alex@12345", 
            "DB_HOST": "127.0.0.1",
            "DB_NAME": "retail_analytics"
        })
        
        # Run dbt streaming models
        result = subprocess.run([
            "dbt", "run", "--select", "streaming", 
            "--project-dir", str(DBT_PROJECT_PATH),
            "--profiles-dir", str(DBT_PROJECT_PATH)
        ], 
        capture_output=True, 
        text=True, 
        env=env,
        cwd=str(DBT_PROJECT_PATH)
        )
        
        if result.returncode == 0:
            context.log.info("dbt streaming models ran successfully")
            return {
                "status": "success",
                "stdout": result.stdout,
                "models_built": 3  # rt_sales_summary, rt_inventory_alerts, rt_customer_analytics
            }
        else:
            context.log.error(f"dbt failed: {result.stderr}")
            return {
                "status": "failed",
                "stderr": result.stderr
            }
            
    except Exception as e:
        context.log.error(f"Error running dbt: {e}")
        return {"status": "error", "message": str(e)}


@asset(group_name="streaming_infrastructure")
def kafka_producer_status(context) -> dict:
    """
    Check if Kafka producer is running and generating events
    """
    try:
        # Check if producer process is running
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'store_aware_producer.py' in cmdline:
                    context.log.info(f"Kafka producer running with PID: {proc.info['pid']}")
                    return {
                        "status": "running",
                        "pid": proc.info['pid'],
                        "process_name": proc.info['name']
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        context.log.warning("Kafka producer not found running")
        return {"status": "not_running", "pid": None}
        
    except Exception as e:
        context.log.error(f"Error checking producer status: {e}")
        return {"status": "error", "message": str(e)}


@asset(group_name="streaming_infrastructure")
def spark_streaming_status(context) -> dict:
    """
    Check if Spark streaming job is running and processing data
    """
    try:
        # Check if spark streaming process is running
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'enhanced_spark_stream.py' in cmdline:
                    context.log.info(f"Spark streaming running with PID: {proc.info['pid']}")
                    return {
                        "status": "running",
                        "pid": proc.info['pid'],
                        "process_name": proc.info['name']
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        context.log.warning("Spark streaming job not found running")
        return {"status": "not_running", "pid": None}
        
    except Exception as e:
        context.log.error(f"Error checking spark status: {e}")
        return {"status": "error", "message": str(e)}


@asset(group_name="streaming_data", deps=[kafka_producer_status, spark_streaming_status])
def streaming_data_validation(context) -> dict:
    """
    Validate that streaming data is flowing into MySQL
    """
    try:
        import mysql.connector
        from mysql.connector import Error
        
        # Connect to MySQL
        connection = mysql.connector.connect(
            host='localhost',
            database='retail_analytics',
            user='root',
            password='Alex@12345'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Check recent data (last 5 minutes)
            query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN event_time >= NOW() - INTERVAL 5 MINUTE THEN 1 END) as recent_records,
                MAX(event_time) as latest_record,
                MIN(event_time) as earliest_record,
                COUNT(DISTINCT store_id) as unique_stores,
                COUNT(DISTINCT dept_id) as unique_departments
            FROM stream_sales_events
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                total_records, recent_records, latest_record, earliest_record, unique_stores, unique_departments = result
                
                validation_result = {
                    "total_records": total_records,
                    "recent_records": recent_records,
                    "latest_record": str(latest_record) if latest_record else None,
                    "earliest_record": str(earliest_record) if earliest_record else None,
                    "unique_stores": unique_stores,
                    "unique_departments": unique_departments,
                    "data_freshness": "fresh" if recent_records > 0 else "stale",
                    "validation_status": "healthy" if recent_records > 0 else "warning"
                }
                
                context.log.info(f"Streaming data validation: {validation_result}")
                return validation_result
            
    except Error as e:
        context.log.error(f"MySQL connection error: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
    
    return {"status": "error", "message": "Failed to validate streaming data"}


@asset(group_name="streaming_analytics", deps=[streaming_data_validation, run_streaming_dbt])
def streaming_analytics_summary(context) -> dict:
    """
    Generate business metrics from streaming data
    """
    try:
        import mysql.connector
        from mysql.connector import Error
        
        connection = mysql.connector.connect(
            host='localhost',
            database='retail_analytics',
            user='root',
            password='Alex@12345'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Get hourly sales summary
            hourly_query = """
            SELECT 
                HOUR(event_time) as hour_of_day,
                COUNT(*) as transactions,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_transaction_value
            FROM stream_sales_events 
            WHERE DATE(event_time) = CURDATE()
            GROUP BY HOUR(event_time)
            ORDER BY hour_of_day DESC
            LIMIT 5
            """
            
            cursor.execute(hourly_query)
            hourly_results = cursor.fetchall()
            
            # Get top performing stores
            store_query = """
            SELECT 
                store_id,
                COUNT(*) as transactions,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_transaction_value
            FROM stream_sales_events 
            WHERE event_time >= NOW() - INTERVAL 1 HOUR
            GROUP BY store_id
            ORDER BY total_revenue DESC
            LIMIT 5
            """
            
            cursor.execute(store_query)
            store_results = cursor.fetchall()
            
            analytics = {
                "hourly_performance": [
                    {
                        "hour": row[0],
                        "transactions": row[1],
                        "revenue": float(row[2]),
                        "avg_value": float(row[3])
                    } for row in hourly_results
                ],
                "top_stores_last_hour": [
                    {
                        "store_id": row[0],
                        "transactions": row[1],
                        "revenue": float(row[2]),
                        "avg_value": float(row[3])
                    } for row in store_results
                ],
                "analysis_timestamp": pd.Timestamp.now().isoformat()
            }
            
            context.log.info(f"Generated streaming analytics: {len(hourly_results)} hourly records, {len(store_results)} top stores")
            return analytics
            
    except Error as e:
        context.log.error(f"Error generating analytics: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
    
    return {"status": "error", "message": "Failed to generate analytics"}


# Job to run the complete streaming pipeline
from dagster import job

@job
def streaming_pipeline_job():
    """
    Complete streaming pipeline orchestration
    """
    # Check infrastructure
    producer_status = kafka_producer_status()
    spark_status = spark_streaming_status()
    
    # Validate data flow
    data_validation = streaming_data_validation()
    
    # Run dbt transformations
    dbt_run = run_streaming_dbt()
    
    # Generate analytics
    analytics = streaming_analytics_summary()