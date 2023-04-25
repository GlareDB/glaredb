#!/usr/bin/env bash

# Create and load the data into Snowflake in the database

set -e

GIT_BRANCH="$1"

SNOWFLAKE_DB=$(./scripts/get-sanitized-name.sh "$GIT_BRANCH")
SNOWFLAKE_DB="glaredb_test_$BQ_DATASET"

# This will be recognized by SnowSQL
export SNOWSQL_PWD="$SNOWFLAKE_PASSWORD"
export SNOWSQL_USER="$SNOWFLAKE_USERNAME"
export SNOWSQL_ACCOUNT="${SNOWFLAKE_ACCOUNTNAME:-hmpfscx-xo23956}"
export SNOWSQL_ROLE="${SNOWFLAKE_ROLENAME:-accountadmin}"
export SNOWSQL_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-compute_wh}"

snowsql -q "DROP DATABASE IF EXISTS ${SNOWFLAKE_DB} CASCADE" 1>&2
