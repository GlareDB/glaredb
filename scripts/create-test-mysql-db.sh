#!/usr/bin/env bash

# Spins up a mysql docker container and loads it with data to test external
# mysql connection against it.

set -e

MYSQL_IMAGE="mysql:8"
CONTAINER_NAME="glaredb_mysql_test"

DB_USER="root"
DB_NAME="glaredb_test"
DB_HOST="127.0.0.1"
DB_PORT=3307

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

# Create new container for mysql
_CONTAINER_ID=$(docker run -p $DB_PORT:3306 --name "${CONTAINER_NAME}" -e MYSQL_DATABASE="${DB_NAME}" -e MYSQL_ALLOW_EMPTY_PASSWORD=YES -d $MYSQL_IMAGE)

CONN_STRING="mysql --local-infile=1 -u $DB_USER -h $DB_HOST --port=$DB_PORT -D $DB_NAME"
# Let the database server start
#
# This loop basically waits for the database to start by testing the connection
# through mysql. It keeps on retrying until it times out (set to 60s).
INIT_TIME=$(date +%s)
CONNECTED="not yet"
while [[ -n "$CONNECTED" ]]; do
  set +e
  CONNECTED=$($CONN_STRING -e "select 1" 2>&1 > /dev/null)
  set -e

  CURRENT_TIME=$(date +%s)
  CURRENT_TIME=$((CURRENT_TIME - 60))
  if [[ "$CURRENT_TIME" -gt "$INIT_TIME" ]]; then
    echo "Timed out waiting for mysql server to start!"
    echo "$CONN_STRING"
    exit 1
  fi
done

# Load data into the test container
$CONN_STRING -e 'source testdata/sqllogictests_mysql/data/setup-test-mysql-db.sql'

# This URI is expected by sqllogictests_mysql.
echo "mysql://${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
