#!/usr/bin/env bash

# Spins up a test cassandra docker container and loads it with data.
#
# By default, the container will start up a 'default' database an not require a
# username or password.
#
# Requires `cassandra`: <https://cassandra.apache.org/_/quickstart.html>

set -e
CONTAINER_NAME="glaredb_cassandra_test"
NETWORK_NAME="cassandra"

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

if docker network ls | grep -q "$network_name"; then
    # Remove the network
    docker network rm "$NETWORK_NAME" > /dev/null
fi

# Create network
docker network create $NETWORK_NAME > /dev/null

# Start container.
docker run --platform linux/amd64  --network $NETWORK_NAME --name $CONTAINER_NAME  -p 9042:9042  --rm -d cassandra &> /dev/null



# # Wait until cassandra is ready.
INIT_TIME=$(date +%s)
EXIT_CODE=1
while [[ $EXIT_CODE -ne 0 ]]; do
  set +e
  docker run --platform linux/amd64 \
        --rm \
        -it \
        --network \
        $NETWORK_NAME nuvo/docker-cqlsh \
        cqlsh $CONTAINER_NAME 9042 --cqlversion='3.4.6' \
        -e "SELECT now() FROM system.local;" > /dev/null
  EXIT_CODE=$?
  set -e

  CURRENT_TIME=$(date +%s)
  CURRENT_TIME=$((CURRENT_TIME - 90))
  if [[ "$CURRENT_TIME" -gt "$INIT_TIME" ]]; then
    echo "Timed out waiting for CASSANDRA to start!"
    exit 1
  fi
done


script_dir="$(realpath ./testdata/sqllogictests_cassandra/data/setup-cassandra.cql)"
bikeshare_stations="$(realpath ./testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv)"

docker run --platform linux/amd64 \
    --rm \
    --network $NETWORK_NAME \
    -v "$script_dir:/scripts/data.cql" \
    -v "$bikeshare_stations:/data/bikeshare_stations.csv" \
    cassandra cqlsh $CONTAINER_NAME 9042 --cqlversion='3.4.6' -f /scripts/data.cql > /dev/null

echo "127.0.0.1:9042"
