#!/usr/bin/env bash

set -e

GCP_PROJECT="${GCP_PROJECT:-glaredb-dev-playground}"
GCP_ZONE="${GCP_ZONE:-us-central1-c}"

instance_name="bench-${RANDOM}"

# c4-standard-32-hyperdisk-balanced-500
# TODO: 2 -> 32 once the script works
gcloud compute instances create $instance_name \
    --project=$GCP_PROJECT \
    --zone=$GCP_ZONE \
    --machine-type=c4-standard-2 \
    --network-interface=network-tier=PREMIUM,nic-type=GVNIC,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=810251374963-compute@developer.gserviceaccount.com \
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
gc_run sudo apt update && sudo apt install -y \
                               rustup \
                               protobuf-compiler \
                               build-essential \
                               gcc \
                               git

# Clone repo
gc_run git clone https://github.com/glaredb/glaredb
