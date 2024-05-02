# SQLBigBench - Database Benchmarking Results

## Description

This project aims to benchmark different database systems and analyze their performance. It provides a set of tools and scripts to run benchmarks, collect results, and generate reports. The project focuses on identifying combination of storage, table and compression formats for running various SQL workloads.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)

## Installation

To install and set up the project, follow these steps:

1. Run the ansible script in the `Hadoop Setup` to setup the hadoop cluster.
2. Use the official database documentations for Hive, Presto, Flink and Spark to setup the query engines on top of Hadoop.


## Usage

Using relevant queries and datasets follow the following steps to get the benchmarking results and arrive to a desired configuration:

1. Run TPC-DS queries, clustered by their query characteristics, on tables with different combinations of storage formats and compression techniques.
2. Identify the combination with the lowest query execution time across engines and freeze this configuration.
3. Add the Iceberg layer on top of the frozen configurations to analyze its impact. If the inclusion of Iceberg leads to better performance, include it in the set of frozen configurations; otherwise, exclude it.
4. Once the best candidate storage format, compression technique, and presence or absence of Iceberg layer are ascertained, use this to benchmark each SQL query engines on concurrent or parallel queries.
5. Perform similar benchmarking as in Step 4 on the real-world datasets using the associated queries.
6. Check the level of conformity between the results obtained from the evaluation conducted with synthetic datasets and real-world datasets.
7. Arrive at the best-performing SQL query engine and associated storage format.

Use the python scripts in the respective folders to run your evaluations. 


## Testing Conducted on a Single Query Engine

This walkthrough guides you through our approach to evaluating various queries, compression methods, and table formats using Spark as an example.

### Setting up the Spark Query Engine

Execute the following script to set up the Spark Query Engine:

```bash
ansible-playbook spark_setup.yml
```

### Running Queries

Once Spark is successfully set up, you can begin running the codes:

1. **Sequential Task Execution:**  
   Use `pysparkfordiffthings.py` from the spark folder to execute tasks sequentially.

   ```bash
   spark-submit pysparkfordiffthings.py --suffix "orc_none"
   ```
   You can modify the type of compression and storage format by specifying its name, such as "storage_compression". If you do not specify a suffix, it defaults to "parquet_snappy".

2. **Parallel Load Sharing:**  
   After obtaining sequential results, we tested the performance under parallel load sharing conditions. For this, we utilized `pysparkParallelexec.py` to run the specified queries of each type.

   We have organized our queries as JSON files for each combiantion of compressiona nd storage format, stored inside `Queries_Jason`. Each JSON is indexed by their query type. This JSON information, along with the query type and index, can be passed through a bash script we created, `run_spark_jobs.sh`. This script executes tasks in parallel and stores the results of each execution as a text file.

    This is a sample code snippet from `testspark.sh`. This snippet utilizes queries from the Real World Trading dataset that we have used for our evaluations.


    ```
    spark-submit pysparkParallelexec.py real_world_queries.json group_by 1 &
    spark-submit pysparkParallelexec.py real_world_queries.json group_by 2 &
    spark-submit pysparkParallelexec.py real_world_queries.json group_by 3 &
    spark-submit pysparkParallelexec.py real_world_queries.json group_by 4 &
    ```

    Inside the `run_spark_jobs.sh` you can specify the query type and the number of queries of that type that you want to execute by chnaging the following paramters:

    ```
    # Declare an associative array where keys are job types and values are the number of jobs
    declare -A job_types=(
        [group_by]=3
        [set_operations]=3
        [aggregations]=3
        [joins]=3
        [nested]=3
    )    
    ```

    To run both the bashscripts for execution you can follow the following commands:

    ```
    chmod +x run_spark_jobs.sh
    ./run_spark_jobs.sh
    ```

    or To run `testspark.sh`

    ```
    chmod +x testspark.sh
    ./testspark.sh
    ```

You can follow similar steps using the Python and bash files inside the Presto, Hive, and Flink folders to reproduce our benchmarking results.



