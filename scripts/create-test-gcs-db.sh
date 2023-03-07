#!/usr/bin/env bash

# Create and load the data into a GCS bucket

set -e

# If override is set, use the main dataset instead (only for CI).
if [ -z "$GCS_USE_MAIN_BUCKET" ]; then
	GIT_BRANCH=$(git branch --show-current)
	GCS_BUCKET=$(./scripts/get-sanitized-name.sh "$GIT_BRANCH")
	GCS_BUCKET="glaredb_test_$GCS_BUCKET"
else
	GCS_BUCKET="glaredb_test"
fi

BUCKET_URI="gs://${GCS_BUCKET}"

GCS="gcloud storage"
FLAGS="--project=${GCP_PROJECT_ID}"

set +e
# We don't want to error in case the bucket doesn't exist
$GCS rm "$BUCKET_URI" -r -c "$FLAGS" 1>&2
set -e
$GCS buckets create "$BUCKET_URI" "$FLAGS" 1>&2

# Files to load to the dataset.
FILES_TO_LOAD=(
	"testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv"
	"testdata/sqllogictests_datasources_common/data/gcs-artifacts/bikeshare_trips.csv"
)
for FILE in "${FILES_TO_LOAD[@]}"; do
	echo "Uploading $FILE to bucket $GCS_BUCKET" 1>&2
	$GCS cp "$FILE" "$BUCKET_URI" "$FLAGS" 1>&2
done

echo "$GCS_BUCKET"
