#!/usr/bin/env bash

set -e

# Check and delete the old sqlite db (if it exists)
# This is just to ensure cleanup locally.
test -f "${SQLITE_DB_LOCATION}" && rm "${SQLITE_DB_LOCATION}"

DB_PATH=`mktemp`

sqlite3 "${DB_PATH}" ".read testdata/sqllogictests_sqlite/data/setup-test-sqlite-db.sql"

echo "${DB_PATH}"
