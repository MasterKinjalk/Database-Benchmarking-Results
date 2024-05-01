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
