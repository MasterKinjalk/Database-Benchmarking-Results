#!/usr/bin/env bash

# Start timing
start_time=$(date +%s)

# Run Python script with different JSON files concurrently using spark-submit
spark-submit pysparkParallelexec.py real_world_queries.json group_by 0 &
spark-submit pysparkParallelexec.py real_world_queries.json group_by 1 &
spark-submit pysparkParallelexec.py real_world_queries.json group_by 2 &
spark-submit pysparkParallelexec.py real_world_queries.json group_by 3 &
spark-submit pysparkParallelexec.py real_world_queries.json group_by 4 &
spark-submit pysparkParallelexec.py real_world_queries.json set_operation 0 &
spark-submit pysparkParallelexec.py real_world_queries.json set_operation 1 &
spark-submit pysparkParallelexec.py real_world_queries.json set_operation 2 &
spark-submit pysparkParallelexec.py real_world_queries.json set_operation 3 &
spark-submit pysparkParallelexec.py real_world_queries.json set_operation 4 &
spark-submit pysparkParallelexec.py real_world_queries.json aggregated 0 &
spark-submit pysparkParallelexec.py real_world_queries.json aggregated 1 &
spark-submit pysparkParallelexec.py real_world_queries.json aggregated 2 &
spark-submit pysparkParallelexec.py real_world_queries.json aggregated 3 &
spark-submit pysparkParallelexec.py real_world_queries.json aggregated 4 &
spark-submit pysparkParallelexec.py real_world_queries.json join 0 &
spark-submit pysparkParallelexec.py real_world_queries.json join 1 &
spark-submit pysparkParallelexec.py real_world_queries.json join 2 &
spark-submit pysparkParallelexec.py real_world_queries.json join 3 &
spark-submit pysparkParallelexec.py real_world_queries.json join 4 &
spark-submit pysparkParallelexec.py real_world_queries.json nested 0 &
spark-submit pysparkParallelexec.py real_world_queries.json nested 1 &
spark-submit pysparkParallelexec.py real_world_queries.json nested 2 &
spark-submit pysparkParallelexec.py real_world_queries.json nested 3 &
spark-submit pysparkParallelexec.py real_world_queries.json nested 4 &


# Wait for all background processes to finish
wait

# End timing
end_time=$(date +%s)

# Calculate duration
duration=$((end_time - start_time))

echo "All queries have been executed."
echo "Total execution time: $duration seconds"

# Optionally, log the time to a file
echo "Total execution time: $duration seconds" > /mnt/data/logs/execution_time.log