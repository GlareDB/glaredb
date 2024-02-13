#!/usr/bin/env bash

set -e

DB_PATH=`mktemp`

sqlite3 "${DB_PATH}" ".read testdata/sqllogictests_sqlite/data/setup-test-sqlite-db.sql"

echo "${DB_PATH}"
