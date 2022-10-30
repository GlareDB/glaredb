#!/usr/bin/env bash

# Build, tag, and push the 'glaredb' and 'pgsrv' docker images.
#
# Requires that docker be configured with the cloud project.
#
# Required env vars:
# GITHUB_REF_NAME - Branch name or tag.
# GCP_PROJECT_ID - Project id for google cloud.

set -ex

: ${GITHUB_REF_NAME?"GITHUB_REF_NAME needs to be set"}
: ${GCP_PROJECT_ID?"GCP_PROJECT_ID needs to be set"}

build_and_push() {
    local nix_target
    local image_ref
    nix_target=$1
    image_ref=$2

    nix build "${nix_target}"
    docker load --input result

    local image_id
    local image_tag
    image_id=$(docker images --filter=reference=$image_ref --format "{{.ID}}")
    image_tag=$(echo ${GITHUB_REF_NAME} | sed -r 's#/+#-#g')

    docker tag "${image_id}" "${image_tag}"

    local image_repo
    image_repo="gcr.io/${GCP_PROJECT_ID}/${image_ref}:${image_tag}"
    docker tag ${image_id} ${image_repo}

    docker push ${image_repo}
}

build_and_push .#glaredb_image "glaredb"
build_and_push .#pgsrv_image "pgsrv"
