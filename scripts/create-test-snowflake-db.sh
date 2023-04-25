#!/usr/bin/env bash

# Create and load the data into Snowflake in the database

set -e

# If override is set, use the main db instead (only for CI).
if [ -z "$SNOWFLAKE_USE_MAIN_DATABASE" ]; then
	GIT_BRANCH=$(git branch --show-current)
	SNOWFLAKE_DB=$(./scripts/get-sanitized-name.sh "$GIT_BRANCH")
	SNOWFLAKE_DB="glaredb_test_$SNOWFLAKE_DB"
else
	SNOWFLAKE_DB="glaredb_test"
fi

# This will be recognized by SnowSQL
export SNOWSQL_PWD="$SNOWFLAKE_PASSWORD"
export SNOWSQL_USER="$SNOWFLAKE_USERNAME"
export SNOWSQL_ACCOUNT="${SNOWFLAKE_ACCOUNTNAME:-hmpfscx-xo23956}"
export SNOWSQL_ROLE="${SNOWFLAKE_ROLENAME:-accountadmin}"
export SNOWSQL_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-compute_wh}"

# Create or replace the database
#
# TODO: Set a short retention period for branch specific databases (make them
# transient).
snowsql -q "CREATE OR REPLACE DATABASE ${SNOWFLAKE_DB}" 1>&2

# Use default values for the following
export SNOWSQL_DATABASE="$SNOWFLAKE_DB"
export SNOWSQL_SCHEMA="public"

# Setup the database
snowsql -f 'testdata/sqllogictests_snowflake/data/setup-test-snowflake-db.sql' 1>&2

echo "$SNOWFLAKE_DB"
