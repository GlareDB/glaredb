#!/usr/bin/env bash

GCP_PROJECT_ID="glaredb-artifacts"
GCP_BUCKET_NAME="glaredb-testdata"

# Specify the testdata as "<directory to store in repo>:<object name>"
#
# Keep appending the files and write their index (as a comment) to make
# it easier for everyone to specify an index in arguments to pull specific
# files when working. 
OBJECTS_TO_PULL=(
	"testdata/sqllogictests_datasources_common/data:bikeshare_trips.csv" # 0
)

# Can provide the "index" of object to pull. 
if [ -n $1 ]; then
	OBJECTS_TO_PULL=("${OBJECTS_TO_PULL[$1]}")
fi

for OBJ in "${OBJECTS_TO_PULL[@]}"; do
	OBJECT_NAME=$(echo $OBJ | cut -d ':' -f2)
	# Files will be downloaded into a directory called gcs-artifacts making it
	# somewhat easier to gitignore.
	LOCAL_PATH=$(echo $OBJ | cut -d ':' -f1)/gcs-artifacts/

	# Create the directory if it doesn't exist
	test -d "$LOCAL_PATH" || mkdir -p "$LOCAL_PATH"

	echo "Attempting to pull $GCP_BUCKET_NAME/$OBJECT_NAME into $LOCAL_PATH"
	gcloud storage cp "gs://$GCP_BUCKET_NAME/$OBJECT_NAME" "$LOCAL_PATH"
done
