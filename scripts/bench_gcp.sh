#!/usr/bin/env bash

set -e

# GCP variables
GCP_PROJECT="${GCP_PROJECT:-glaredb-dev-playground}"
GCP_ZONE="${GCP_ZONE:-us-central1-c}"
# Defaults to 'glaredb-bench', a public bucket with CORS configured.
GCP_BUCKET="${GCP_BUCKET:-glaredb-bench}"
# TODO: 2 -> 32 once everything is verified working.
GCP_MACHINE_TYPE="${GCP_MACHINE_TYPE:-c4-standard-2}"

# Commit to benchmark.
GIT_COMMIT="${GIT_HASH:-$(git rev-parse --short HEAD)}"

# Current timestamp
unix_timestamp_s=$(date +%s)

instance_name="bench-${unix_timestamp_s}"

# hyperdisk-balanced
# 500G, 6000 iops, 890 throughput
# TODO: Probably paramterize disk.
# TODO: Make service account configurable. Currently the github actions
# one has write access to the bucket, so just use it for now (which
# makes sense since we're calling this from github actions).
gcloud compute instances create "$instance_name" \
    --project="$GCP_PROJECT" \
    --zone="$GCP_ZONE" \
    --machine-type="$GCP_MACHINE_TYPE" \
    --network-interface=network-tier=PREMIUM,nic-type=GVNIC,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=github-actions@glaredb-dev-playground.iam.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_write \
    --create-disk=auto-delete=yes,\
boot=yes,\
image=projects/ubuntu-os-cloud/global/images/ubuntu-minimal-2504-plucky-amd64-v20250430,\
mode=rw,\
provisioned-iops=6000,\
provisioned-throughput=890,\
size=500,\
type=hyperdisk-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --reservation-affinity=any


function cleanup() {
  echo "Deleting instance ${instance_name}..."
  gcloud compute instances delete "$instance_name" \
    --project="$GCP_PROJECT" --zone="$GCP_ZONE" --quiet || true
}
# Delete instance on exit
trap cleanup EXIT

# Run a command on the remote GCP instance.
function gc_run() {
    local cmd="$*"
    gcloud compute ssh "$instance_name" \
      --project="$GCP_PROJECT" --zone="$GCP_ZONE" \
      --command "$cmd"
}

# Wait until instance is RUNNING
echo "Waiting for ${instance_name} to enter RUNNING state..."
while [[ "$(gcloud compute instances describe "$instance_name" \
    --project="$GCP_PROJECT" --zone="$GCP_ZONE" \
    --format='get(status)')" != "RUNNING" ]]; do
  sleep 5
  echo "..."
done

# Wait until SSH available.
echo "Waiting for SSH to become available on ${instance_name}..."
until gcloud compute ssh "$instance_name" \
      --project="$GCP_PROJECT" --zone="$GCP_ZONE" \
      --command "echo SSH is up" --quiet &> /dev/null; do
  sleep 5
  echo "..."
done

# Get tools.
gc_run "sudo apt update \
        && sudo apt install -y \
          rustup \
          protobuf-compiler \
          build-essential \
          gcc \
          git \
       && rustup install stable"

# Clone repo.
gc_run "git clone https://github.com/glaredb/glaredb && cd glaredb && git checkout ${GIT_COMMIT}"

# Benchmarks are ran with sudo since they try to drop caches by writing to
# /proc/fs/...
#
# The '-E' just inherits the running user's environment, so it's able to find
# the correct cargo, etc.

gc_run "cd glaredb \
          && sudo -E cargo bench --bench bench_runner -- bench/micro --drop-cache"

# TODO: Other benchmark suites...

# Upload results to gcs.
gc_run "cd glaredb && \
        gsutil cp ./bench_bin/results-*.tsv gs://${GCP_BUCKET}/results/${GCP_MACHINE_TYPE}/${unix_timestamp_s}/"
