#!/usr/bin/env bash
#
# Spins up a Cassandra docker container and loads it with test data.
# To access the database, use the default user/password 'cassandra'.

set -e
CONTAINER_NAME="glaredb_cassandra_test"
NETWORK_NAME="cassandra"

# Remove container if it exists
if [[ -n "$(docker ps -aqf name=$CONTAINER_NAME)" ]]; then
  docker rm -f $CONTAINER_NAME >/dev/null
fi

# Remove network if it exists
if docker network ls | grep -q "$NETWORK_NAME"; then
  docker network rm "$NETWORK_NAME" >/dev/null
fi

# Create network
docker network create $NETWORK_NAME >/dev/null

# Volume mounts
setup_script="$(realpath ./testdata/sqllogictests_cassandra/data/setup-cassandra.cql)"
bikeshare_stations="$(realpath ./testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv)"

# Build image
docker build --quiet --tag $CONTAINER_NAME ./scripts/images/cassandra >/dev/null

# Start Cassandra container
docker run --rm \
  --network $NETWORK_NAME \
  --name $CONTAINER_NAME \
  -p 9042:9042 \
  -v "$setup_script:/scripts/data.cql" \
  -v "$bikeshare_stations:/data/bikeshare_stations.csv" \
  --detach \
  $CONTAINER_NAME >/dev/null

# Wait until cassandra is ready
# SEE https://hub.docker.com/_/cassandra
INIT_TIME=$(date +%s)
STARTUP=1
SUPERUSER_ROLE=1

while [[ $STARTUP -ne 0 ]] && [[ $SUPERUSER_ROLE -ne 0 ]]; do
  set +e
  docker logs $CONTAINER_NAME 2>&1 | grep -q 'Startup complete'
  STARTUP=$?
  docker logs $CONTAINER_NAME 2>&1 | grep -q "Created default superuser role 'cassandra'"
  SUPERUSER_ROLE=$?
  set -e

  CURRENT_TIME=$(date +%s)
  CURRENT_TIME=$((CURRENT_TIME - 300))
  REMAINING_TIME=$((INIT_TIME - CURRENT_TIME))
  echo "Waiting for Cassandra to start... $REMAINING_TIME seconds remaining" >&2
  sleep 5
  if [[ "$CURRENT_TIME" -gt "$INIT_TIME" ]]; then
    echo "Timed out waiting for Cassandra to start!"
    exit 1
  fi
done

# NOTE: Additional buffer time once superuser role is created. Couldn't find any
# meaningful log or trigger to wait for :(. If we don't wait, the following
# `exec` will fail
sleep 5

# Now that Cassandra is ready, seed with data.cql that was mounted in
docker exec "$(docker ps -aqf name=$CONTAINER_NAME)" \
  cqlsh \
  -u cassandra \
  -p cassandra \
  --cqlversion='3.4.7' \
  -f /scripts/data.cql

echo "--- Cassandra is ready and seeded"
echo "127.0.0.1:9042"
