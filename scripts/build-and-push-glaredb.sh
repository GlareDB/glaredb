#!/usr/bin/env bash

# Build, tag, and push the 'glaredb' docker image.
#
# Requires that docker be configured with the cloud project.
#
# Required env vars:
# GITHUB_REF_NAME - Branch name or tag.
# GCP_PROJECT_ID - Project id for google cloud.

set -ex

: ${GITHUB_REF_NAME?"GITHUB_REF_NAME needs to be set"}
: ${GCP_PROJECT_ID?"GCP_PROJECT_ID needs to be set"}

nix build .#server_image
docker load --input result

image_id=$(docker images --filter=reference=glaredb --format "{{.ID}}")
image_tag=$(echo ${GITHUB_REF_NAME} | sed -r 's#/+#-#g')

docker tag "${image_id}" "${image_tag}"

# Tag image as latest if we're on main.
branch_name=$(git rev-parse --abbrev-ref HEAD);
if [[ "${branch_name}" = "main" ]]; then
    docker tag ${image_id} latest
fi

image_repo="gcr.io/${GCP_PROJECT_ID}/glaredb:${image_tag}"
docker tag ${image_id} ${image_repo}

docker push ${image_repo}
