#!/usr/bin/env bash

# Start timing
start_time=$(date +%s)

# Path to the JSON file
json_file="real_world_queries.json"

# Declare an associative array where keys are job types and values are the number of jobs
declare -A job_types=(
    [group_by]=3
    [set_operations]=3
    [aggregations]=3
    [joins]=3
    [nested]=3
)

# Function to submit spark jobs
submit_job() {
    local job_type=$1
    local index=$2
    echo "Submitting $job_type job $index..."
    spark-submit pysparkParallelexec.py "$json_file" "$job_type" "$index" &
}

# Submit jobs based on specified counts
for type in "${!job_types[@]}"; do
    count=${job_types[$type]}
    for ((i = 0; i < count; i++)); do
        submit_job "$type" "$i"
    done
done

# Wait for all background processes to finish
wait

# End timing
end_time=$(date +%s)
duration=$((end_time - start_time))

# Output results
echo "All queries have been executed."
echo "Total execution time: $duration seconds"

echo "Total execution time: $duration seconds" > ./logs/execution_time.log