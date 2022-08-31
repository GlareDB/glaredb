#!/usr/bin/env bash

# Build, tag, and push the 'glaredb' docker image.
#
# Requires that docker be configured with the cloud project.
#
# Required env vars:
# GITHUB_REF_NAME - Branch name or tag.
# GCP_PROJECT_ID - Project id for google cloud.

set -e

: ${GITHUB_REF?"GITHUB_REF needs to be set"}
: ${GCP_PROJECT_ID?"GCP_PROJECT_ID needs to be set"}

nix build .#server_image
docker load --input result

image_id=$(docker images --filter=reference=glaredb --format "{{.ID}}")
image_tag=$(echo ${GITHUB_REF} | sed -r 's#/+#-#g')

docker tag ${image_id} ${image_id}

image_repo="gcr.io/${GCP_PROJECT_ID}/glaredb:${image_tag}"
docker tag ${image_id} ${image_repo}

docker push ${image_repo}
