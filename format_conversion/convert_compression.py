from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Hive to Parquet") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the hive table
ship_mode_df = spark.sql("SELECT * FROM ship_mode")

# Write to a new table with ZLIB compression
ship_mode_df.write \
    .option("compression", "snappy") \
    .parquet("ship_mode_parquet_snappy")
