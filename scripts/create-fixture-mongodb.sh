#!/usr/bin/env bash

set -e

CONTAINER_NAME="glaredb_mongodb_test"

CONTAINER_ID=$(docker ps -aqf "name=glaredb_mongodb_test")

docker exec "${CONTAINER_ID}" mongosh \
       "${MONGO_CONN_STRING}" \
       --quiet /tmp/mdb-fixture.js  1>&2
