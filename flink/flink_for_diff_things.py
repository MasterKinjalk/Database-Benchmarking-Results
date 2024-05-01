import argparse
import time
import re
import csv

from pyflink.table import *
from pyflink.table.catalog import HiveCatalog
from sql_metadata import Parser


settings = EnvironmentSettings.new_instance().in_batch_mode().build()
t_env = TableEnvironment.create(environment_settings=settings)

catalog_name = "myhive"
default_database = "default"
hive_conf_dir = "/opt/apache-hive-3.1.2-bin/conf"  # a local path

hive_catalog = HiveCatalog(catalog_name, default_database, hive_conf_dir)
t_env.register_catalog("myhive", hive_catalog)

# set the HiveCatalog as the current catalog of the session
t_env.use_catalog("myhive")
# t_env.execute_sql("SET 'execution.runtime-mode' = 'batch';")
# Setup command line argument parsing
parser = argparse.ArgumentParser(description='Replace table names and execute queries.')
parser.add_argument('--suffix', type=str, default='', help='Suffix to append to table names e.g. orc_none')
args = parser.parse_args()

# Function to remove SQL comments and clean for PysparkSQL
def remove_sql_comments(query):
    # Remove inline comments
    query = re.sub(r'--.*', '', query)
    # Remove block comments
    query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
    # Replace newlines with spaces
    query = query.replace('\n', ' ')
    # Replace non-ASCII characters with a single space
    query = re.sub(r'[^\x00-\x7F]+', ' ', query)
    # Replace straight double quotes with backticks for SQL compatibility
    query = query.replace('"', "`")
    
    # # Pattern to find and adjust date addition and subtraction patterns
    # pattern = r"\(cast\s*\(\s*'(\d{4}-\d{2}-\d{2})'\s+as\s+date\)\s*([-+])\s*(\d+)\s+days\)"
    
    # Enhanced regex pattern to find and adjust date addition and subtraction patterns
    # Handles variations in date formatting and spaces
    pattern = r"\(cast\s*\(\s*'(\d{4}-\d{1,2}-\d{1,2})'\s+as\s+date\)\s*([-+])\s*(\d+)\s+days\)"
    # Replacement logic using a lambda function to decide between DATE_ADD and DATE_SUB
    replacement = lambda m: f"DATE_ADD(CAST('{m.group(1)}' AS DATE), {m.group(3)})" \
        if m.group(2) == '+' else f"DATE_SUB(CAST('{m.group(1)}' AS DATE), {m.group(3)})"
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
        query = re.sub(r'\b' + re.escape(table) + r'\b', new_table_name, query)
    return query

# Function to execute a query and return its execution time and the query itself
def execute_query(query):
    cleaned_query = remove_sql_comments(query)
    updated_query = replace_table_names(cleaned_query, suffix=args.suffix)
    start_time = time.time()
    try:
        table_result = t_env.execute_sql(query)
        # Without this jobs run asynchronously
        with table_result.collect() as results:
            for result in results:
                continue
            results.close()
    except Exception as e:
        print(f"Error executing query: {updated_query}")
        print(e)
        return (updated_query, None)
    end_time = time.time()
    print("****************************")
    print(end_time-start_time)
    print("****************************")
    return (updated_query, end_time - start_time)

# Read and process queries from the file
with open("/mnt/data/flink_divided_sql_query.sql", 'r') as file:
    queries = [query.strip() for query in file.read().split(';') if query.strip()]

# Execute queries and collect the results
results = []
for query in queries:
    if query:
        result = execute_query(query)
        if result[1] == None:
            break
        results.append(result)

# results = [execute_query(query) for query in queries if query]

if args.suffix.strip():
    csv_path = f"/mnt/data/flink_query_execution_stats_{args.suffix}.csv"
else:
    csv_path = "/mnt/data/flink_query_execution_stats_parquet_snappy.csv"
# Write results to a CSV file
with open(csv_path, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['query', 'execution_time'])  # Corrected header
    writer.writerows(results)  # Write data rows, ensuring correct format











































# import argparse
# import time
# import re
# import csv

# from pyflink.table import *
# from pyflink.table.catalog import HiveCatalog
# from sql_metadata import Parser


# settings = EnvironmentSettings.new_instance().in_batch_mode().build()
# t_env = TableEnvironment.create(environment_settings=settings)

# catalog_name = "myhive"
# default_database = "default"
# hive_conf_dir = "/opt/apache-hive-3.1.2-bin/conf"  # a local path

# hive_catalog = HiveCatalog(catalog_name, default_database, hive_conf_dir)
# t_env.register_catalog("myhive", hive_catalog)

# # set the HiveCatalog as the current catalog of the session
# t_env.use_catalog("myhive")

# # Setup command line argument parsing
# parser = argparse.ArgumentParser(description='Replace table names and execute queries.')
# parser.add_argument('--suffix', type=str, default='orc_none', help='Suffix to append to table names')
# parser.add_argument('--query_type', type=str, help='Type of query to execute, as specified in the comments (e.g., Set_operations)')
# args = parser.parse_args()

# # Initialize Spark session with Hive support
# '''spark = SparkSession.builder \
#     .appName("SQLQueryExecution") \
#     .enableHiveSupport() \
#     .getOrCreate()
# '''


# # Function to remove SQL comments
# def remove_sql_comments(query):
#     query = re.sub(r'--.*', '', query)
#     query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
#     query = query.replace('\n', ' ')
#     return query.strip()

# # Function to replace table names in the query using sql_metadata
# def replace_table_names(query, suffix):
#     parser = Parser(query)
#     for table in parser.tables:
#         new_table_name = f"{table}_{suffix}"
#         query = re.sub(r'\b' + re.escape(table) + r'\b', new_table_name, query)
#     return query

# # Function to execute a query and return its execution time and the query itself
# def execute_query(query):
#     query = remove_sql_comments(query)
#     query = replace_table_names(query, suffix=args.suffix)
#     start_time = time.time()
#     print("Starting execution ... ")
#     try:
#         t_env.execute_sql(query)
#     except Exception as e:
#         print(f"Error executing query: {query}")
#         print(e)
#         print("Error exit ...")
#         return [query, None]
#     end_time = time.time()
#     print("exiting ... {}".format(end_time - start_time))
#     return [query, (end_time - start_time)]

# def filter_queries_by_type(queries, query_type):
#     # Filter queries to only include those with the specified type in their leading comment
#     filtered_queries = []
#     for query in queries:
#         match = re.search(r'-- QUERY_\d+ - (\w+)', query)
#         if match and match.group(1).lower() == query_type.lower():
#             filtered_queries.append(query)
#     return filtered_queries

# # Read and process queries from the file
# with open("/mnt/data/divided_sql_query.sql", 'r') as file:
#     queries = file.read().split(';')
#     print(len(queries))

# # Filter queries based on the command-line argument for query type
# if args.query_type:
#     queries = filter_queries_by_type(queries, args.query_type)

# # Execute queries and collect the results
# results = [execute_query(query) for query in queries if query]

# # Check if results are empty before creating DataFrame
# if results:
#     csv_path = f"/mnt/data/flink_query_execution_stats_{args.suffix}_{args.query_type if args.query_type else 'all'}.csv"
#     csv_file = open(csv_path, "w", newline="")
#     csv_writer = csv.writer(csv_file)
#     csv_writer.writerow(['query', 'time'])
#     for row in results:
#         csv_writer.writerow(row)
# else:
#     print("No results to process.")
#     # Optionally create an empty DataFrame with a predefined schema if needed
#     schema = StructType([
#         StructField("query", StringType(), True),
#         StructField("execution_time", FloatType(), True)
#     ])
#     df = spark.createDataFrame([], schema)
#     # You can still write this empty DataFrame to a CSV if necessary
#     df.write.mode("overwrite").csv("/mnt/data/flink_empty_query_execution_stats.csv", header=True)
