from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Hive to Parquet") \
    .enableHiveSupport() \
    .getOrCreate()


spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")


def write_table_to_parquet(table_name, compression_type):
    # Read the hive table
    table_df = spark.sql(f"SELECT * FROM {table_name}")

    # Write to a new table with given compression codec
    table_df.write \
        .option("compression", compression_type) \
        .mode("overwrite") \
        .parquet(f"hdfs://172.22.156.187:9000/hadoop/parquet/{table_name}_parquet_{compression_type}.parquet")


def write_table_to_orc(table_name, compression_type):
    # Read the hive table
    table_df = spark.sql(f"SELECT * FROM {table_name}")

    # Write to a new table with given compression
    table_df.write \
        .option("compression", compression_type) \
        .mode("overwrite") \
        .orc(f"hdfs://172.22.156.187:9000/hadoop/orc/{table_name}_orc_{compression_type}.orc")


table_names = [
"call_center",
"catalog_page",
"catalog_returns",
"catalog_sales",
"customer",
"customer_address",
"customer_demographics",
"date_dim",
"household_demographics",
"income_band",
"inventory",
"item",
"promotion",
"reason",
"ship_mode",
"store",
"store_returns",
"store_sales",
"time_dim",
"warehouse",
"web_page",
"web_returns",
"web_sales",
"web_site"]


for table_name in table_names:
    print(f"Writing data for {table_name}")
    write_table_to_parquet(table_name, "zlib")
