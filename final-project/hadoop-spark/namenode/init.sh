#!/bin/bash

# Create /input directory on HDFS
echo "Creating /input directory on HDFS..."
hadoop fs -mkdir -p /input

# Set permissions for /input directory
echo "Setting permissions for /input directory on HDFS..."
hadoop fs -chmod g+w /input

# Copy data from /data to /input on HDFS
echo "Copying data from /data to /input on HDFS..."
hadoop fs -copyFromLocal /data/* /input/

echo "Data copied successfully."
