import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import time
import re
from sql_metadata import Parser
import csv

# Setup command line argument parsing
parser = argparse.ArgumentParser(description="Replace table names and execute queries.")
parser.add_argument(
    "--suffix",
    type=str,
    default="",
    help="Suffix to append to table names e.g. orc_none",
)
args = parser.parse_args()

# Initialize Spark session with Hive support
spark = (
    SparkSession.builder.appName("Iceberg Integration")
    .enableHiveSupport()
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.iceberg.spark.SparkSessionCatalog",
    )
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://user/hive/warehouse")
    .getOrCreate()
)

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")


# Function to remove SQL comments and clean for PysparkSQL
def remove_sql_comments(query):
    # Remove inline comments
    query = re.sub(r"--.*", "", query)
    # Remove block comments
    query = re.sub(r"/\*.*?\*/", "", query, flags=re.DOTALL)
    # Replace newlines with spaces
    query = query.replace("\n", " ")
    # Replace non-ASCII characters with a single space
    query = re.sub(r"[^\x00-\x7F]+", " ", query)
    # Replace straight double quotes with backticks for SQL compatibility
    query = query.replace('"', "`")

    # # Pattern to find and adjust date addition and subtraction patterns
    # pattern = r"\(cast\s*\(\s*'(\d{4}-\d{2}-\d{2})'\s+as\s+date\)\s*([-+])\s*(\d+)\s+days\)"

    # Enhanced regex pattern to find and adjust date addition and subtraction patterns
    # Handles variations in date formatting and spaces
    pattern = r"\(cast\s*\(\s*'(\d{4}-\d{1,2}-\d{1,2})'\s+as\s+date\)\s*([-+])\s*(\d+)\s+days\)"
    # Replacement logic using a lambda function to decide between DATE_ADD and DATE_SUB
    replacement = lambda m: (
        f"DATE_ADD(CAST('{m.group(1)}' AS DATE), {m.group(3)})"
        if m.group(2) == "+"
        else f"DATE_SUB(CAST('{m.group(1)}' AS DATE), {m.group(3)})"
    )
    # Apply the regex to the query
    query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)

    return query.strip()


# Function to replace table names in the query using sql_metadata
def replace_table_names(query, suffix):
    parser = Parser(query)
    for table in parser.tables:
        if suffix.strip():
            new_table_name = f"{table}_{suffix}"
        else:
            new_table_name = table
        query = re.sub(r"\b" + re.escape(table) + r"\b", new_table_name, query)
    return query


# Function to execute a query and return its execution time and the query itself
def execute_query(query):
    cleaned_query = remove_sql_comments(query)
    updated_query = replace_table_names(cleaned_query, suffix=args.suffix)
    start_time = time.time()
    try:
        spark.sql(updated_query).show()
    except Exception as e:
        print(f"Error executing query: {updated_query}")
        print(e)
        return (updated_query, None)
    end_time = time.time()
    return (updated_query, end_time - start_time)


# Read and process queries from the file
with open("/mnt/data/divided_sql_query.sql", "r") as file:
    queries = [query.strip() for query in file.read().split(";") if query.strip()]

# Execute queries and collect the results
results = [execute_query(query) for query in queries if query]

if args.suffix.strip():
    csv_path = f"/mnt/data/query_execution_stats_{args.suffix}.csv"
else:
    csv_path = "/mnt/data/query_execution_stats_parquet_snappy.csv"
# Write results to a CSV file
with open(csv_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["query", "execution_time"])  # Corrected header
    writer.writerows(results)  # Write data rows, ensuring correct format
