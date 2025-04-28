#!/usr/bin/env bash

set -euo pipefail

exec > >(tee /var/log/user-data.log) 2>&1

# Install rustup
apt update
apt install -y rustup protobuf-compiler build-essential gcc

# Install rust stable
# Switch to ubuntu user so it picks up the right rustup dir.
su - ubuntu -c "rustup install stable"

# Clone repo into our workdir.
WORKDIR=/home/ubuntu/glaredb
if [[ -d "$WORKDIR/.git" ]]; then
  echo "Not cloning, directory already exists"
else
  # Switch for perms
  su - ubuntu -c "git clone https://github.com/GlareDB/glaredb.git $WORKDIR"
fi

echo "Finished"
