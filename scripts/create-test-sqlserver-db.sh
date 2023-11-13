#!/usr/bin/env bash

# Spins up a SQL Server docker container and loads it with data.
#
# Requires `sqlcmd` to be installed: <https://github.com/microsoft/go-sqlcmd>

set -e

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
CONTAINER_ID=$(docker run \
       -e "ACCEPT_EULA=Y" \
       -e "MSSQL_SA_PASSWORD=Password123" \
       -e "MSSQL_PID=developer" \
       -p 1433:1433 \
       --rm \
       --name $CONTAINER_NAME \
       -d \
       mcr.microsoft.com/mssql/server:2022-latest)

# sqlcmd -S localhost -U SA -P Password123 -i testdata/sqllogictests_sqlserver/data/setup-sqlserver.sql

echo "server=tcp:localhost,1433;user=SA;password=Password123;TrustServerCertificate=true"

