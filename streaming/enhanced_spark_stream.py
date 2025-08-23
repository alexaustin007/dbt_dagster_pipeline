"""
Enhanced Spark Structured Streaming Job for realistic retail events
Supports the new enhanced event schema with proper data types and validation
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)

# Initialize Spark session (packages handled via spark-submit)
spark = SparkSession.builder \
    .appName("enhanced_retail_streaming") \
    .getOrCreate()

# Enhanced schema matching the new producer
enhanced_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("dept_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("promotion_applied", BooleanType(), False)
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and apply schema
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), enhanced_schema).alias("data")
).select("data.*")

# Data transformations and validation
transformed_df = parsed_df \
    .withColumn("event_time", col("event_time").cast(TimestampType())) \
    .withColumn("processed_at", current_timestamp()) \
    .filter(col("quantity") > 0) \
    .filter(col("unit_price") > 0) \
    .filter(col("total_amount") > 0) \
    .filter(col("store_id").isNotNull()) \
    .filter(col("dept_id").isNotNull())

# Select only columns that exist in MySQL table
final_df = transformed_df.select(
    "event_id", "event_time", "store_id", "dept_id", "product_id", 
    "customer_id", "quantity", "unit_price", "total_amount", 
    "transaction_type", "payment_method", "promotion_applied", "processed_at"
)

# Database connection properties
db_properties = {
    "url": "jdbc:mysql://127.0.0.1:3306/retail_analytics",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": "Alex@12345"  # Using actual password from .env
}

# Write stream to MySQL
def write_to_mysql(df, epoch_id):
    """Write batch to MySQL with error handling"""
    try:
        df.write \
            .format("jdbc") \
            .options(**db_properties) \
            .option("dbtable", "stream_sales_events") \
            .mode("append") \
            .save()
        print(f"Batch {epoch_id}: Successfully wrote {df.count()} records")
    except Exception as e:
        print(f"Batch {epoch_id}: Error writing to MySQL: {e}")

# Start streaming query
streaming_query = final_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", "/tmp/spark_checkpoint/enhanced_sales") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Enhanced streaming job started...")
print("Listening for sales events with enhanced schema...")

# Keep the application running
streaming_query.awaitTermination()