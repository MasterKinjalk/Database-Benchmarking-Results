import argparse
import json
import time
from pyspark.sql import SparkSession
import os
import re
from pyhive import presto
from sql_metadata import Parser


# def remove_non_ascii(query):
#     """Remove non-ASCII characters from the SQL query string."""
#     # Replace non-ASCII characters with nothing (or you could replace with a space or other placeholder)
#     cleaned_query = re.sub(r'[^\x00-\x7F]', ' ', query)
#     return cleaned_query

# Function to replace table names in the query using sql_metadata
def replace_table_names(query, suffix='parquet_gzip'):
    parser = Parser(query)
    print("Parser Tables", parser.tables)
    for table in parser.tables:
        if suffix != "" and table != "INTERVAL":
            new_table_name = f"{table}_{suffix}"
        else:
            new_table_name = f"{table}"
        query = re.sub(r'\b' + re.escape(table) + r'\b', new_table_name, query)
    return query


def replace_table_names_prefix(query, prefix="realworld"):
    parser = Parser(query)
    for table in parser.tables:
        if prefix != "" and table != "INTERVAL":
            new_table_name = f"{prefix}_{table}"
        else:
            new_table_name = f"{table}"
        query = re.sub(r'\b' + re.escape(table) + r'\b', new_table_name, query)
    return query

def clean_query(query):
    """Modify the SQL query string to replace specific patterns and remove non-ASCII characters."""
    # Replace non-ASCII characters with a single space
    query = re.sub(r"[^\x00-\x7F]", " ", query)
    # Replace straight double quotes with backticks for SQL compatibility
    

    # Enhanced regex pattern to find and adjust date addition and subtraction patterns
    # Handles variations in date formatting and spaces
    pattern = r"\(cast\s*\(\s*'(\d{4}-\d{1,2}-\d{1,2})'\s+as\s+date\)\s*([-+])\s*(\d+)\s+days\)"
    # Replacement logic using a lambda function to decide between DATE_ADD and DATE_SUB
    replacement = lambda m: (  # noqa: E731
        f"DATE_ADD(CAST('{m.group(1)}' AS DATE), {m.group(3)})"
        if m.group(2) == "+"
        else f"DATE_SUB(CAST('{m.group(1)}' AS DATE), {m.group(3)})"
    )
    # Apply the regex to the query
    query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
    query = replace_table_names(query)
    
    if int(args.is_realworld) == 1:
        print("Realwaorld evaluation")
        query = replace_table_names_prefix(query)

    return query.strip()


def execute_query(cursor, query):
    """Executes a single SQL query using the provided Spark session and measures execution time."""
    start_time = time.time()
    try:
        start_time = time.time()
        print(query)
        cursor.execute(query)
        result = cursor.fetchone()
        execution_time = time.time() - start_time
        # print(f"Result: {result}")
        return execution_time
    except Exception as e:
        print(f"Error executing query: {query}")
        print(e)
        return None


def main(json_filename, query_type, query_index, is_realworld):
    """Main function to load a specific query from a JSON file, execute it, and log the results."""
    
    conn = presto.connect(host='localhost', port=8080, username='root', catalog='hive', schema='default')
    cursor = conn.cursor()
    
    # Load queries from the specified JSON file
    with open(json_filename, "r") as file:
        queries = json.load(file)

    # Directory to store the logs
    log_directory = "/mnt/data/logs"
    os.makedirs(log_directory, exist_ok=True)  # Ensure the directory exists

    # Execute the specific query and measure execution time
    if query_type in queries and len(queries[query_type]) > query_index:

        query = clean_query(queries[query_type][query_index])
        #print(query)
        execution_time = execute_query(cursor, query)
        if execution_time is not None:
            log_message = f"Query Type: {query_type}, Index: {query_index}, Execution Time: {execution_time:.4f} seconds"
            print(log_message)
            # Log the execution time to a text file within the specified directory
            log_filename = f"{json_filename.replace('.json', '')}_{query_type}_{query_index}_log.txt"
            full_log_path = os.path.join(log_directory, log_filename)
            with open(full_log_path, "w") as log_file:
                log_file.write(log_message)
    else:
        print("Query not found. Please check the query type and index.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute a specific query from a JSON file and log the execution time."
    )
    parser.add_argument(
        "json_filename",
        type=str,
        help="Filename of the JSON file containing the queries",
    )
    parser.add_argument(
        "query_type", type=str, help="Type of the query to execute (e.g., group_by)"
    )
    parser.add_argument(
        "query_index",
        type=int,
        help="Index of the query to execute within the type category",
    )
    parser.add_argument(
        "is_realworld",
        type=int,
        help="Indicate if the testing is to be done against realworld dataset",
    )
    args = parser.parse_args()

    main(args.json_filename, args.query_type, args.query_index, args.is_realworld)
