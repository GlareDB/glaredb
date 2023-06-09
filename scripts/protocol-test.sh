#!/usr/bin/env bash

# Postgres protocol testing for CI.
#
# This will spin up a GlareDB instance on its default port (6543) and execute
# the protocol tests against it.

set -e

run_id=${RANDOM}

# Start up Metastore.
metastore_log_file="/tmp/metastore.log-${run_id}"
nohup cargo run --bin glaredb -- -v metastore > "${metastore_log_file}" 2>&1 &

# Get pids so we can shut it down at the end of this script.
metastore_pid=$!

# Give Metastore some time to startup.
sleep 5

# Start up GlareDB.
glaredb_log_file="/tmp/glaredb.log-${run_id}"
nohup cargo run --bin glaredb -- -v server --user glaredb --password dummy > "${glaredb_log_file}" 2>&1 &

glaredb_pid=$!

# Give it some time. Eventually we could wait on a signal or poll system status
# through an endpoint.
sleep 5

# Run protocol tests.
ret=0
cargo run --bin pgprototest -- \
    --dir ./testdata/pgprototest \
    --addr localhost:6543 \
    --user glaredb \
    --password dummy \
    --database glaredb \
    -v || ret=$?

# Kill Metastore and GlareDB.
kill "${glaredb_pid}"
kill "${metastore_pid}"

# Print out log if failed.
if [[ "${ret}" -ne 0 ]]; then
    echo "--- Metastore Logs ---"
    cat "${metastore_log_file}"

    echo "--- GlareDB Logs ---"
    cat "${glaredb_log_file}"

    exit "${ret}"
fi

