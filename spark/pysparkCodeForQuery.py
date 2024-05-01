from pyspark.sql import SparkSession, Row
import time
import re

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("SQLQueryExecution") \
    .enableHiveSupport() \
    .getOrCreate()

# Function to remove SQL comments
def remove_sql_comments(query):
    # Remove single-line comments
    query = re.sub(r'--.*', '', query)
    # Remove multi-line comments
    query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
    query = query.replace('\n', ' ')
    return query.strip()

# Function to execute a query and return its execution time and the query itself
def execute_query(query):
    query = remove_sql_comments(query)  # Preprocess the query to remove comments
    start_time = time.time()
    try:
        spark.sql(query).show()  # Execute the SQL query
    except Exception as e:
        print(f"Error executing query: {query}")
        print(e)
        return Row(query=query, execution_time=None)
    end_time = time.time()
    execution_time = end_time - start_time
    return Row(query=query, execution_time=execution_time)

# Read the SQL queries from the file
with open("/mnt/data/divided_sql_query.sql", 'r') as file:
    queries = file.read().split(';')  # Assuming each query ends with a ';'

# Remove any empty queries that may result from splitting and preprocessing
queries = [remove_sql_comments(query).strip() for query in queries if remove_sql_comments(query).strip()]

# List to hold Row objects containing query and its execution time
results = []

# Execute queries and collect the statistics
for query in queries:
    result = execute_query(query)
    if result.execution_time is not None:
        results.append(result)

# Create a DataFrame from the results
schema = ["query", "execution_time"]
df = spark.createDataFrame(results, schema=schema)

# Write the DataFrame to a CSV file
df.write.csv("/mnt/data/query_execution_stats.csv", header=True)

# Close the Spark session
spark.stop()
