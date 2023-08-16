#!/usr/bin/env bash

# Create and load the data into BigQuery in a dataset

set -e

# If override is set, use the main dataset instead (only for CI).
if [ -z "$BIGQUERY_USE_MAIN_DATASET" ]; then
	GIT_BRANCH=$(git branch --show-current)
	BQ_DATASET=$(./scripts/get-sanitized-name.sh "$GIT_BRANCH")
	BQ_DATASET="glaredb_test_$BQ_DATASET"
else
	BQ_DATASET="glaredb_test"
fi

LOCATION="US"
BQ="bq --location=${LOCATION} --project_id=${GCP_PROJECT_ID}"

# Create the dataset (force if it already exists)
$BQ mk --force --dataset "$BQ_DATASET" 1>&2

# Load tables with schema from JSONs and CSVs
#
# We can do this in SQL but since we need custom dataset names, doing this
# with command-line makes more sense.
#
# Entries of the format:
#     <table name>:<csv file path>:<json schema path>
TABLES_TO_LOAD=(
	"datatypes:testdata/sqllogictests_bigquery/data/datatypes-data.json:testdata/sqllogictests_bigquery/data/datatypes-schema.json:NEWLINE_DELIMITED_JSON"
	"bikeshare_stations:testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv:testdata/sqllogictests_datasources_common/data/bikeshare_stations-bq-schema.json:CSV"
	"bikeshare_trips:testdata/sqllogictests_datasources_common/data/gcs-artifacts/bikeshare_trips.csv:testdata/sqllogictests_datasources_common/data/bikeshare_trips-bq-schema.json:CSV"
)
for TABLE_INFO in "${TABLES_TO_LOAD[@]}"; do
	# Create all the tables from their schema represented as JSON.

	TABLE=$(echo "$TABLE_INFO" | cut -d ':' -f1)
	DATA_FILE=$(echo "$TABLE_INFO" | cut -d ':' -f2)
	SCHEMA_FILE=$(echo "$TABLE_INFO" | cut -d ':' -f3)
	DATA_FILE_TYPE=$(echo "$TABLE_INFO" | cut -d ':' -f4)
	SKIP_LEADING_ROWS=""

	if [ "$DATA_FILE_TYPE" == "CSV" ]
	then
		SKIP_LEADING_ROWS="--skip_leading_rows=1"
	fi

	echo "Uploading table '$TABLE' from '$DATA_FILE' using schema from '$SCHEMA_FILE'" 1>&2

	$BQ load --replace \
	  --source_format=${DATA_FILE_TYPE} \
	  ${SKIP_LEADING_ROWS} \
		"${BQ_DATASET}.${TABLE}" \
		"$DATA_FILE" \
		"$SCHEMA_FILE" 1>&2
done

echo "$BQ_DATASET"
