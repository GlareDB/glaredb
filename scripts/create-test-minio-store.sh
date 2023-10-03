#!/usr/bin/env bash

# Spins up a MinIO docker container to test external databes/catalog storage against it.

set -e

MINIO_IMAGE="minio/minio:latest"
CONTAINER_NAME="glaredb_minio_test"

MINIO_CONSOLE_ADDRESS=:9001

# Remove container if it exists
if [[ -n "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]]; then
    docker rm -f $CONTAINER_NAME > /dev/null
fi

# Start minio.
CONTAINER_ID="$(docker run \
   -p 9000:9000 \
   -p 9001:9001 \
   -e MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}" \
   -e MINIO_SECRET_KEY="${MINIO_SECRET_KEY}" \
   -e MINIO_CONSOLE_ADDRESS="${MINIO_CONSOLE_ADDRESS}" \
   --rm \
   --name $CONTAINER_NAME \
   -d \
   $MINIO_IMAGE server /data)"

# Create the test container using the minio client
docker run --rm --net=host --entrypoint=/bin/sh -i minio/mc:latest <<EOF
#!/usr/bin/mc

# Wait for minio server to become ready
curl --retry 10 -f --retry-connrefused --retry-delay 1 http://localhost:9000/minio/health/live

# Configure mc to connect to our above container as host
mc config host add glaredb_minio http://localhost:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

# Remove the bucket if it already exists
mc rm -r --force glaredb_minio/"$MINIO_BUCKET"

# Finally create the test bucket
mc mb glaredb_minio/"$MINIO_BUCKET"
EOF
