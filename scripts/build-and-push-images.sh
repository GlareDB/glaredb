#!/usr/bin/env bash

# Build, tag, and push the 'glaredb' docker image.
#
# Requires that docker be configured with the cloud project.
#
# Required env vars:
# GITHUB_REF_NAME - Branch name or tag.
# Optional env vars:
# GCP_PROJECT_ID - Project id for google cloud. defaults to `glaredb-artifacts`.

set -ex

: ${GITHUB_REF_NAME?"GITHUB_REF_NAME needs to be set"}
GCP_PROJECT_ID=${GCP_PROJECT_ID:-glaredb-artifacts}

git_rev=$(git rev-parse HEAD)
image_tag=$(echo "${GITHUB_REF_NAME}" | sed -r 's#/+#-#g')
image_repo="gcr.io/${GCP_PROJECT_ID}/glaredb"

# Try to pull tagged image first for getting a cache.
docker pull "${image_repo}:${image_tag}"

# Build image with tags pointing to this git revision.
docker build \
       -t "${image_repo}:${image_tag}" \
       -t "${image_repo}:${git_rev}" \
       .

# Check that the image runs.
docker run "${image_repo}:${image_tag}" glaredb --help

# Push it.
docker push --all-tags "${image_repo}"
