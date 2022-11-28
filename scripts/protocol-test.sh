#!/usr/bin/env bash

# Postgres protocol testing for CI.
#
# This will spin up a GlareDB instance on its default port (6543) and execute
# the protocol tests against it.

set -e

# Build first so that `nix run ...` can start right away.
nix build .#cli

# Start up GlareDB.
log_file="/tmp/glaredb.log-${RANDOM}"
nohup nix run .#cli -- -v server > "${log_file}" 2>&1 &

# Give it some time. Eventually we could wait on a signal or poll system status
# through an endpoint.
sleep 5

# Run protocol tests.
ret=0
nix run .#pgprototest -- \
    --dir ./testdata/pgprototest \
    --addr localhost:6543 \
    --user glaredb \
    --password dummy \
    --database glaredb || ret=$?

# Print out log if failed.
if [[ "${ret}" -ne 0 ]]; then
    echo "--- GlareDB Logs ---"
    cat "${log_file}"
    exit "${ret}"
fi

