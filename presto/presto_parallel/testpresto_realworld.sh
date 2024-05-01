#!/usr/bin/env bash

# Start timing
start_time=$(date +%s)

# Run Python script with different JSON files concurrently using spark-submit
python prestoParallelexec.py realworld_queries.json group_by 0 1 &
python prestoParallelexec.py realworld_queries.json set_operations 0 1 &
python prestoParallelexec.py realworld_queries.json aggregations 0 1 &
python prestoParallelexec.py realworld_queries.json joins 0 1 &
python prestoParallelexec.py realworld_queries.json nested 0 1 &
python prestoParallelexec.py realworld_queries.json group_by 1 1 &
python prestoParallelexec.py realworld_queries.json set_operations 1 1 &
python prestoParallelexec.py realworld_queries.json aggregations 1 1 &
python prestoParallelexec.py realworld_queries.json joins 1 1 &
python prestoParallelexec.py realworld_queries.json nested 1 1 &
python prestoParallelexec.py realworld_queries.json group_by 2 1 &
python prestoParallelexec.py realworld_queries.json set_operations 2 1 &
python prestoParallelexec.py realworld_queries.json aggregations 2 1 &
python prestoParallelexec.py realworld_queries.json joins 3 1 &
python prestoParallelexec.py realworld_queries.json nested 2 1 &


# Wait for all background processes to finish
wait

# End timing
end_time=$(date +%s)

# Calculate duration
duration=$((end_time - start_time))

echo "All queries have been executed."
echo "Total execution time: $duration seconds"

# Optionally, log the time to a file
echo "Total execution time: $duration seconds" > /mnt/data/logs/realworld_presto_execution_1_at_a_time.log
