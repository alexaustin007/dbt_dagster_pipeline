
"""Spark Structured Streaming Job: Read events from Kafka and write to MySQL"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder.appName("stream_sales").getOrCreate()

schema = StructType([
    StructField("event_id", IntegerType()),
    StructField("event_time", StringType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType())
])

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sales_events")
    .load()
)

parsed = (
    kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("event_time").cast(TimestampType()))
)

(
    parsed.writeStream
    .format("jdbc")
    .option("url", "jdbc:mysql://127.0.0.1:3306/retail")
    .option("dbtable", "stream_sales_events")
    .option("user", "root")
    .option("password", "password")
    .option("checkpointLocation", "/tmp/spark_checkpoint/stream_sales")
    .start()
    .awaitTermination()
)
