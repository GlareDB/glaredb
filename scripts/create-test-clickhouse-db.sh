#!/usr/bin/env bash

# Spins up a test clickhouse docker container and loads it with data.
#
# Requires `clickhouse-client`: <https://clickhouse.com/docs/en/install>

set -e

REPO_ROOT="$(git rev-parse --show-toplevel)"
CONTAINER_NAME="glaredb_clickhouse_test"

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

docker run \
       --name $CONTAINER_NAME \
       -p 9000:9000 \
       -d \
       --rm \
       clickhouse/clickhouse-server:23
