#!/usr/bin/env bash

set -eux

: "${GITHUB_PAT:?Github access token needs to be set}"

REPO_ROOT=$(git rev-parse --show-toplevel)

INSTALLATION_NAME="arc-runners-set-bench"
NAMESPACE="arc-runners-bench"

helm install arc \
    --namespace "${NAMESPACE}" \
    --create-namespace \
    --version "0.9.3" \
    oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set-controller

helm install "${INSTALLATION_NAME}" \
    --namespace "${NAMESPACE}" \
    --create-namespace \
    --values "${REPO_ROOT}/infra/bench-arc-values.yaml" \
    --set githubConfigSecret.github_token="${GITHUB_PAT}" \
    --version "0.9.3" \
    oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set
