import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import time
import re
from sql_metadata import Parser
from pyhive import hive
import csv

# Setup command line argument parsing
parser = argparse.ArgumentParser(description='Replace table names and execute queries.')
parser.add_argument('--suffix', type=str, default='orc_none', help='Suffix to append to table names')
parser.add_argument('--query_type', type=str, help='Type of query to execute, as specified in the comments (e.g., Set_operations)')
args = parser.parse_args()

# Initialize Spark session with Hive support
'''spark = SparkSession.builder \
    .appName("SQLQueryExecution") \
    .enableHiveSupport() \
    .getOrCreate()
'''
conn = hive.Connection(host='localhost', port=10000, username='root')
cursor = conn.cursor()


# Function to remove SQL comments
def remove_sql_comments(query):
    query = re.sub(r'--.*', '', query)
    query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
    query = query.replace('\n', ' ')
    return query.strip()

# Function to replace table names in the query using sql_metadata
def replace_table_names(query, suffix):
    parser = Parser(query)
    for table in parser.tables:
        new_table_name = f"{table}_{suffix}"
        query = re.sub(r'\b' + re.escape(table) + r'\b', new_table_name, query)
    return query

# Function to execute a query and return its execution time and the query itself
def execute_query(query):
    query = remove_sql_comments(query)
    query = replace_table_names(query, suffix=args.suffix)
    start_time = time.time()
    print("Starting execution ... ")
    try:
        cursor.execute(query)
    except Exception as e:
        print(f"Error executing query: {query}")
        print(e)
        print("Error exit ...")
        return [query, None]
    end_time = time.time()
    print("exiting ... {}".format(end_time - start_time))
    return [query, (end_time - start_time)]

def filter_queries_by_type(queries, query_type):
    # Filter queries to only include those with the specified type in their leading comment
    filtered_queries = []
    for query in queries:
        match = re.search(r'-- QUERY_\d+ - (\w+)', query)
        if match and match.group(1).lower() == query_type.lower():
            filtered_queries.append(query)
    return filtered_queries

# Read and process queries from the file
with open("/mnt/data/divided_sql_query.sql", 'r') as file:
    queries = file.read().split(';')
    print(len(queries))

# Filter queries based on the command-line argument for query type
if args.query_type:
    queries = filter_queries_by_type(queries, args.query_type)

# Execute queries and collect the results
results = [execute_query(query) for query in queries if query]

# Check if results are empty before creating DataFrame
if results:
    csv_path = f"/mnt/data/hive_query_execution_stats_{args.suffix}_{args.query_type if args.query_type else 'all'}.csv"
    csv_file = open(csv_path, "w", newline="")
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(results)
else:
    print("No results to process.")
    # Optionally create an empty DataFrame with a predefined schema if needed
    schema = StructType([
        StructField("query", StringType(), True),
        StructField("execution_time", FloatType(), True)
    ])
    df = spark.createDataFrame([], schema)
    # You can still write this empty DataFrame to a CSV if necessary
    df.write.mode("overwrite").csv("/mnt/data/hive_empty_query_execution_stats.csv", header=True)
