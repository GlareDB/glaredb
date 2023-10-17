#!/usr/bin/env bash

# Spins up a fake GCS server container to test external native storage against it.
# Adapted from https://github.com/apache/arrow-rs/blob/master/object_store/CONTRIBUTING.md#gcp

set -e

FAKE_GCS_IMAGE="tustvold/fake-gcs-server"
CONTAINER_NAME="glaredb_gcs_test"

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

# Start minio.
CONTAINER_ID="$(docker run \
   -p 4443:4443 \
   --rm \
   --name $CONTAINER_NAME \
   -d \
   $FAKE_GCS_IMAGE -scheme http -backend memory -public-host localhost:4443)"

curl --retry 5 -f --retry-all-errors --retry-delay 1 \
    -X POST --data-binary "{\"name\":\"$TEST_BUCKET\"}" -H "Content-Type: application/json" "http://localhost:4443/storage/v1/b"

echo '{"gcs_base_url": "http://localhost:4443", "disable_oauth": true, "client_email": "", "private_key": ""}' > /tmp/fake-gcs-creds.json
