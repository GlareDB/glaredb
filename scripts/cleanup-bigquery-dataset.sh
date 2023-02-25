#!/usr/bin/env bash

# Create and load the data into BigQuery in a dataset

set -e

# BIGQUERY_DATASET_ID is later used to run the tests.
# Override exists for CI, not recommended to be used by dev.
if [ -z "$BIGQUERY_DATASET_ID" ]; then
	BQ_DATASET=$(git branch --show-current | base64)
	BQ_DATASET="glaredb_test_$BQ_DATASET"
else
	BQ_DATASET="$BIGQUERY_DATASET_ID"
fi

LOCATION="US"
BQ="bq --location=${LOCATION} --project_id=${GCP_PROJECT_ID}"

$BQ rm -r -f "$BQ_DATASET" 1>&2
