#!/usr/bin/env bash

# Create and load the data into BigQuery in a dataset

set -e

GIT_BRANCH=$1

BQ_DATASET=$(echo "$GIT_BRANCH" | base64)
BQ_DATASET="glaredb_test_$BQ_DATASET"

LOCATION="US"
BQ="bq --location=${LOCATION} --project_id=${GCP_PROJECT_ID}"

$BQ rm -r -f "$BQ_DATASET" 1>&2
