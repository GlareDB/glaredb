#!/usr/bin/env bash

# Spins up a MongoDB docker container and loads it with data.

set -e

MONGODB_IMAGE="mongo:6"
CONTAINER_NAME="glaredb_mongodb_test"

# Default database that the mongo container creates.
DB_NAME="test"

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

# Start mongo.
CONTAINER_ID="$(docker run \
       -p 27017:27017 \
       --rm \
       --name $CONTAINER_NAME \
       -d \
       $MONGODB_IMAGE)"

echo "Container: ${CONTAINER_ID}" 1>&2

REPO_ROOT="$(git rev-parse --show-toplevel)"

# Copy in test data.
docker cp \
       ${REPO_ROOT}/testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv \
       ${CONTAINER_ID}:/tmp/.

# Exec into container to load test data.
docker exec $CONTAINER_ID mongoimport \
       --type csv \
       --headerline \
       --ignoreBlanks \
       "mongodb://localhost:27017/${DB_NAME}" \
       /tmp/bikeshare_stations.csv 1>&2

# The mongo docker container is kinda bad. The MONGO_INITDB_... environment vars
# might look like the obvious solution, but they don't work as you would expect.
#
# See https://github.com/docker-library/mongo/issues/329
#
# Creating non-default databases and users requires some additional setup that I
# don't really want to figure out right now.
echo "mongodb://localhost:27017/${DB_NAME}"
