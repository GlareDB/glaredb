#!/usr/bin/env bash

# Spins up a SQL Server docker container and loads it with data.
#
# Requires `sqlcmd` to be installed: <https://github.com/microsoft/go-sqlcmd>.
# This is already installed on github runners:
# <https://github.com/actions/runner-images/blob/main/images/linux/Ubuntu2004-Readme.md#ms-sql>
#
# The created container will have the root of the repo mounted in for easily
# loading test data.

set -e

REPO_ROOT="$(git rev-parse --show-toplevel)"
CONTAINER_NAME="glaredb_sqlserver_test"

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

# Start mssql.
#
# Note for mac, I was running into the following error on startup:
# /opt/mssql/bin/sqlservr: Invalid mapping of address 0x4004bac000 in reserved address space below 0x400000000000. Possible causes:
#
# Upgrading my docker desktop version fixed it (also note that the desktop app
# didn't seem to detect that it needed to be updated, so I had to reinstall from
# the docker website).
docker run \
       -e "ACCEPT_EULA=Y" \
       -e "MSSQL_SA_PASSWORD=Password123" \
       -e "MSSQL_PID=developer" \
       -p 1433:1433 \
       -v "$REPO_ROOT":/repo \
       --rm \
       --name $CONTAINER_NAME \
       -d \
       mcr.microsoft.com/mssql/server:2022-latest &> /dev/null

# Let the database server start
#
# This loop basically waits for the database to start by testing the connection
# through sqlcmd. It keeps on retrying until it times out (set to 60s).
INIT_TIME=$(date +%s)
EXIT_CODE=1
while [[ $EXIT_CODE -ne 0 ]]; do
  set +e
  sqlcmd -S localhost -U SA -P Password123 -Q "select 1" &> /dev/null
  EXIT_CODE=$?
  set -e

  CURRENT_TIME=$(date +%s)
  CURRENT_TIME=$((CURRENT_TIME - 60))
  if [[ "$CURRENT_TIME" -gt "$INIT_TIME" ]]; then
    echo "Timed out waiting for SQL Server to start!"
    exit 1
  fi
done

# Run the queries to load test data.
sqlcmd -S localhost \
       -U SA \
       -P Password123 \
       -i testdata/sqllogictests_sqlserver/data/setup-sqlserver.sql &> /dev/null

echo "server=tcp:localhost,1433;user=SA;password=Password123;TrustServerCertificate=true"

