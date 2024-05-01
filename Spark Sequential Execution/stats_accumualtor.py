import csv
import os


def accumulate_columns_separate_and_sort(files):
    # Prepare data dictionary
    data = {}

    # Process each file and collect data
    for csv_filename in files:
        file_key = os.path.splitext(os.path.basename(csv_filename))[0]
        data[file_key] = []

        with open(csv_filename, "r", newline="") as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header
            for row in reader:
                if len(row) > 1:  # Ensure there is at least a second column
                    try:
                        value = float(row[1])  # Convert second column to float
                        data[file_key].append(value)
                    except ValueError:
                        # Handle the case where conversion to float fails
                        print(
                            f"Warning: Could not convert {row[1]} to float in {csv_filename}."
                        )

    # Calculate sums for each column
    sums = {key: sum(filter(None, values)) for key, values in data.items()}

    # Sort keys by their sums in descending order
    sorted_keys = sorted(sums, key=sums.get, reverse=True)

    # Determine the maximum number of rows any column will have
    max_length = max(len(values) for values in data.values())

    # Normalize lengths by filling with None (or another suitable placeholder)
    for key in data:
        data[key] += [None] * (max_length - len(data[key]))

    # Write the collected and sorted data to a new CSV file
    output_filename = "accumulated_data_sorted_columns.csv"
    with open(output_filename, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        # Write the header sorted by descending sum
        writer.writerow(sorted_keys)

        # Write the data rows based on the sorted keys
        for i in range(max_length):
            writer.writerow([data[key][i] for key in sorted_keys])


# List of CSV files
csv_files = [
    "Spark_query_exec_stats_orc_none.csv",
    "Spark_query_exec_stats_orc_snappy.csv",
    "Spark_query_exec_stats_orc_zlib.csv",
    "Spark_query_exec_stats_parquet_snappy.csv",
    "Spark_query_exec_stats_parquet_none.csv",
    "Spark_query_exec_stats_parquet_gzip.csv",
    "Spark_query_exec_stats_parquet_gzip_iceberg_.csv",
]

# Call the function
accumulate_columns_separate_and_sort(csv_files)
