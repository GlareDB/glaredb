#!/usr/bin/env bash

# Create and load the data into BigQuery in a dataset

set -e

# BIGQUERY_DATASET_ID is later used to run the tests.
# Override exists for CI, not recommended to be used by dev.
if [ -z "$BIGQUERY_DATASET_ID" ]; then
	BQ_DATASET=$(git branch --show-current | base64)
	BQ_DATASET="glaredb_test_$BQ_DATASET"
	# Use the dev playground to create this temporary dataset
	GCP_PROJECT_ID="glaredb-dev-playground"
else
	BQ_DATASET="$BIGQUERY_DATASET_ID"
fi

LOCATION="US"
BQ="bq --location=${LOCATION}"

# Create the dataset (force if it already exists)
$BQ mk --force --dataset "$BQ_DATASET" 1>&2

# Load tables with schema from JSONs and CSVs
#
# We can do this in SQL but since we need custom dataset names, doing this
# with command-line makes more sense.
TABLES_TO_LOAD=(
	datatypes:testdata/sqllogictests_bigquery/data/datatypes-data.csv
	bikeshare_stations:testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv
	bikeshare_trips:testdata/sqllogictests_datasources_common/data/gcs-artifacts/bikeshare_trips.csv
)
for TABLE_INFO in "${TABLES_TO_LOAD[@]}"; do
	# Create all the tables from their schema represented as JSON.

	TABLE=$(echo "$TABLE_INFO" | cut -d ':' -f1)
	DATA_FILE=$(echo "$TABLE_INFO" | cut -d ':' -f2)

	echo "Uploading table '$TABLE' from '$DATA_FILE'" 1>&2

	$BQ load --replace \
	  --source_format=CSV \
	  --skip_leading_rows=1 \
		"${BQ_DATASET}.${TABLE}" \
		"$DATA_FILE" \
		"testdata/sqllogictests_bigquery/data/${TABLE}-schema.json" 1>&2
done

echo "$BQ_DATASET"
