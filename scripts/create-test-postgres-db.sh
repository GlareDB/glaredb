#!/usr/bin/env bash

# Spins up a postgres docker container and loads it with data to test external
# postgres connection against it.

set -e

POSTGRES_IMAGE="postgres:15"
CONTAINER_NAME="glaredb_postgres_test"

DB_USER="glaredb"
DB_NAME="glaredb_test"
DB_PASSWORD="password"
DB_HOST="localhost"
DB_PORT=5432

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

_CONTAINER_ID=$(docker run -p $DB_PORT:5432 --name "${CONTAINER_NAME}" -e POSTGRES_USER="${DB_USER}" -e POSTGRES_DB="${DB_NAME}" -e POSTGRES_PASSWORD="${DB_PASSWORD}" -d $POSTGRES_IMAGE)

CONN_STRING="host=${DB_HOST} port=${DB_PORT} user=${DB_USER} password=${DB_PASSWORD} dbname=${DB_NAME} sslmode=disable"

# Let the database server start
#
# This loop basically waits for the database to start by testing the connection
# through psql. It keeps on retrying until it times out (set to 60s).
INIT_TIME=$(date +%s)
CONNECTED="not yet"
while [[ -n "$CONNECTED" ]]; do
  set +e
  CONNECTED=$(psql "${CONN_STRING}" -c "select 1" 2>&1 > /dev/null)
  set -e
  
  CURRENT_TIME=$(date +%s)
  CURRENT_TIME=$((CURRENT_TIME - 60))
  if [[ "$CURRENT_TIME" -gt "$INIT_TIME" ]]; then
    echo "Timed out waiting for postgres server to start!"
    exit 1
  fi
done 

# Load data into the test container
#
# The psql command does not error if there is an error while running the SQL.
# Conveniently though, it does output it to stderr. So capture it and error.
SETUP_OUTPUT=$(psql "${CONN_STRING}" -f "testdata/sqllogictests_postgres/data/setup-test-postgres-db.sql" 2>&1 > /dev/null)

if [[ -n "$SETUP_OUTPUT" ]]; then
  echo "$SETUP_OUTPUT"
  exit 1
fi

echo "$CONN_STRING"
