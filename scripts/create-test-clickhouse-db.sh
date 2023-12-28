#!/usr/bin/env bash

# Spins up a test clickhouse docker container and loads it with data.
#
# By default, the container will start up a 'default' database an not require a
# username or password.
#
# Requires `clickhouse`: <https://clickhouse.com/docs/en/install>

set -e

CONTAINER_NAME="glaredb_clickhouse_test"

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

# Start container.
docker run \
       --name $CONTAINER_NAME \
       -p 9000:9000 \
       -d \
       --rm \
       clickhouse/clickhouse-server:23 &> /dev/null

# Wait until clickhouse is ready.
INIT_TIME=$(date +%s)
EXIT_CODE=1
while [[ $EXIT_CODE -ne 0 ]]; do
  set +e
  clickhouse client --query "select 1" &> /dev/null
  EXIT_CODE=$?
  set -e

  CURRENT_TIME=$(date +%s)
  CURRENT_TIME=$((CURRENT_TIME - 60))
  if [[ "$CURRENT_TIME" -gt "$INIT_TIME" ]]; then
    echo "Timed out waiting for Clickhouse to start!"
    exit 1
  fi
done

# Create tables.
clickhouse client --multiquery < ./testdata/sqllogictests_clickhouse/data/setup-clickhouse.sql

# Load data into tables.
clickhouse client \
    --query="INSERT INTO bikeshare_stations FORMAT CSVWithNames" < ./testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv
clickhouse client \
    --query="INSERT INTO bikeshare_trips FORMAT CSVWithNames" < ./testdata/sqllogictests_datasources_common/data/gcs-artifacts/bikeshare_trips.csv

echo "clickhouse://localhost:9000/default"
