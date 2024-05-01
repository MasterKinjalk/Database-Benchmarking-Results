from pyhive import presto
import time
import csv

# Function to remove comments from SQL queries
def remove_comments(query):
    lines = query.split("\n")
    query_without_comments = ""
    for line in lines:
        if "--" in line:
            line = line[:line.index("--")]
        query_without_comments += line + "\n"
    return query_without_comments.strip()

# Read SQL queries from file
with open("queries.sql", "r") as file:
    sql_queries = file.read()

# Remove comments from SQL queries
sql_queries = remove_comments(sql_queries)

# Connect to Presto server using pyhive
conn = presto.connect(host='localhost', port=8080, username='root')
cursor = conn.cursor()

# Create a CSV file to log query execution time
csv_file = open("presto_query_execution_time.csv", "w", newline="")
csv_writer = csv.writer(csv_file)
csv_writer.writerow(["Query", "Execution Time (s)"])

# Execute each SQL query
for query in sql_queries.split(";"):
    if query.strip():
        start_time = time.time()
        cursor.execute(query)
        execution_time = time.time() - start_time
        csv_writer.writerow([query, execution_time])

# Close the cursor and connection
cursor.close()
conn.close()

# Close the CSV file
csv_file.close()

