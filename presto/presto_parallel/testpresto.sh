#!/usr/bin/env bash

# Start timing
start_time=$(date +%s)

# Run Python script with different JSON files concurrently using spark-submit
python prestoParallelexec.py presto_parallel.json group_by 0 0 &
python prestoParallelexec.py presto_parallel.json set_operations 0 0 &
python prestoParallelexec.py presto_parallel.json aggregations 0 0 &
python prestoParallelexec.py presto_parallel.json joins 0 0 &
python prestoParallelexec.py presto_parallel.json nested 0 0 &
# python prestoParallelexec.py presto_parallel.json group_by 1 0 &
# python prestoParallelexec.py presto_parallel.json set_operations 1 0 &
# python prestoParallelexec.py presto_parallel.json aggregations 1 0 &
# python prestoParallelexec.py presto_parallel.json joins 1 0 &
# python prestoParallelexec.py presto_parallel.json nested 1 0 &
# python prestoParallelexec.py presto_parallel.json group_by 2 0 &
# python prestoParallelexec.py presto_parallel.json set_operations 2 0 &
# python prestoParallelexec.py presto_parallel.json aggregations 2 0 &
# python prestoParallelexec.py presto_parallel.json joins 3 0 &
# python prestoParallelexec.py presto_parallel.json nested 2 0 &


# Wait for all background processes to finish
wait

# End timing
end_time=$(date +%s)

# Calculate duration
duration=$((end_time - start_time))

echo "All queries have been executed."
echo "Total execution time: $duration seconds"

# Optionally, log the time to a file
echo "Total execution time: $duration seconds" > /mnt/data/logs/presto_execution_time_1_at_a_time_realworld.log
