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
# Expose external database to another port than "default".
# We can use the default port to test inter-container networks.
DB_PORT=5433

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

CONTAINER_PORT=5432
CONTAINER_ID=$(docker run -p $DB_PORT:$CONTAINER_PORT --name "${CONTAINER_NAME}" -e POSTGRES_USER="${DB_USER}" -e POSTGRES_DB="${DB_NAME}" -e POSTGRES_PASSWORD="${DB_PASSWORD}" -d $POSTGRES_IMAGE)

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
SETUP_OUTPUT=$(psql "${CONN_STRING}" -c "create table t1 as (select 23 a, 45 b, 'test' c);" 2>&1 > /dev/null)

if [[ -n "$SETUP_OUTPUT" ]]; then
  echo "$SETUP_OUTPUT"
  exit 1
fi

# This URI is expected by sqllogictests_postgres.
echo "$CONN_STRING"

# This URI is expected by sqllogictests_postgres/tunnels/ssh.
CONTAINER_HOST=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $CONTAINER_ID)
CONN_STRING="host=${CONTAINER_HOST} port=${CONTAINER_PORT} user=${DB_USER} password=${DB_PASSWORD} dbname=${DB_NAME} sslmode=disable"
echo "$CONN_STRING"
