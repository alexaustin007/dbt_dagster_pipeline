
"""Spark job to read sales.csv, drop duplicates, and load to MySQL"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("clean_sales").getOrCreate()
df = spark.read.csv("data/raw/sales.csv", header=True, inferSchema=True)
deduped = df.dropDuplicates()
(
    deduped.write
    .format("jdbc")
    .option("url", "jdbc:mysql://127.0.0.1:3306/retail")
    .option("dbtable", "stg_sales_clean")
    .option("user", "root")
    .option("password", "password")
    .mode("overwrite")
    .save()
)
spark.stop()
