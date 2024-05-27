#!/bin/bash

# Extract namenode name directory from HDFS_CONF_dfs_namenode_name_dir environment variable
namedir=$(echo "$HDFS_CONF_dfs_namenode_name_dir" | perl -pe 's#file://##')

# Check if namenode name directory exists
if [ ! -d "$namedir" ]; then
  echo "Namenode name directory not found: $namedir"
  exit 2
fi

# Check if CLUSTER_NAME environment variable is set
if [ -z "$CLUSTER_NAME" ]; then
  echo "Cluster name not specified"
  exit 2
fi

# Remove lost+found directory from namenode name directory
echo "Removing lost+found directory from $namedir"
rm -r "$namedir/lost+found"

# Check if namenode name directory is empty
if [ ! "$(ls -A "$namedir")" ]; then
  echo "Formatting namenode name directory: $namedir"
  $HADOOP_HOME/bin/hdfs --config "$HADOOP_CONF_DIR" namenode -format "$CLUSTER_NAME"
fi


# Start namenode
$HADOOP_HOME/bin/hdfs --config "$HADOOP_CONF_DIR" namenode &

# Wait for namenode to start
echo "Waiting for Namenode to start..."
until $(curl --output /dev/null --silent --head --fail http://localhost:9870); do
    sleep 5
done

# Run initialization script
/init.sh

# Keep the container running
tail -f /dev/null




