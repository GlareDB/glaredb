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

CONTAINER_ID=$(docker run -p $DB_PORT:5432 --name "${CONTAINER_NAME}" -e POSTGRES_USER="${DB_USER}" -e POSTGRES_DB="${DB_NAME}" -e POSTGRES_PASSWORD="${DB_PASSWORD}" -d $POSTGRES_IMAGE)

CONN_STRING="host=${DB_HOST} port=${DB_PORT} user=${DB_USER} password=${DB_PASSWORD} dbname=${DB_NAME} sslmode=disable"

# Let the database server start
sleep 2

# Load data into the test container
#
# The psql command does not error if there is an error while running the SQL.
# Conveniently though, it does output it to stderr. So capture it and error.
SETUP_OUTPUT=$(psql "${CONN_STRING}" -f "testdata/sqllogictests_postgres/data/setup-test-postgres-db.sql" 2>&1 > /dev/null)

if [[ ! -z "$SETUP_OUTPUT" ]]; then
  echo "$SETUP_OUTPUT"
  exit 1
fi

echo $CONN_STRING
