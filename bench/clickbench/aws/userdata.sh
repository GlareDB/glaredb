#!/usr/bin/env bash

set -euo pipefail

exec > >(tee /var/log/user-data.log) 2>&1

# Clone repo into our workdir.
WORKDIR=/opt/glaredb
if [[ -d "$WORKDIR/.git" ]]; then
  cd $WORKDIR && git pull
else
  git clone https://github.com/GlareDB/glaredb.git $WORKDIR
  cd $WORKDIR
fi
