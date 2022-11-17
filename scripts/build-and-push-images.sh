#!/usr/bin/env bash

# Build, tag, and push the 'glaredb' and 'pgsrv' docker images.
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

# Copy the containers to GCR
GCP_AUTH_TOKEN=$(gcloud auth print-access-token)

push_image() {
    local container_archive
    local registry_name
    container_archive=$1
    registry_name=$2

    local git_rev
    git_rev=$(git rev-parse HEAD)

    image_repo="gcr.io/${GCP_PROJECT_ID}/${registry_name}"
    skopeo copy \
        --insecure-policy \
        --dest-registry-token "${GCP_AUTH_TOKEN}" \
        "docker-archive:${container_archive}" \
        "docker://${image_repo}:${git_rev}"

    # Copy the image to add other tags
    skopeo copy \
        --insecure-policy \
        --dest-registry-token "${GCP_AUTH_TOKEN}" \
        --src-registry-token "${GCP_AUTH_TOKEN}" \
        "docker://${image_repo}:${git_rev}" \
        "docker://${image_repo}:${GITHUB_REF_NAME}"
}

# build the container archives
nix build .#glaredb_image --out-link glaredb_image
nix build .#pgsrv_image --out-link pgsrv_image

# TODO: ensure that the command can be executed inside the containers before pushing
# docker load --input glaredb_image
# docker run -it 

push_image "glaredb_image" "glaredb"
push_image "pgsrv_image" "pgsrv"
