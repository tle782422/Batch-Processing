#!/bin/bash

# Wait for the Hive Metastore to be ready
until nc -z -v -w30 hive-metastore 9083
do
  echo "Waiting for hive-metastore connection..."
  sleep 5
done

echo "hive-metastore is up and running"

# Execute the Hive schema script
hive -f /hive/hive-schema.hql
