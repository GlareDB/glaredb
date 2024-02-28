#!/usr/bin/env bash

# Postgres protocol testing for CI.
#
# This will spin up a GlareDB instance on its default port (6543) and execute
# the protocol tests against it.

set -exo pipefail

export RUST_BACKTRACE=1

run_id=${RANDOM}

# Start up GlareDB.
glaredb_log_file="/tmp/glaredb.log-${run_id}"
if [ ! -f ./target/debug/glaredb ]; then
    just build
fi
./target/debug/glaredb -v server --user glaredb --password dummy > "${glaredb_log_file}" 2>&1 &

glaredb_pid=$!

# build the prototest build in the background if needed
pgproto_test_build=-1
if [ ! -f ./target/debug/glaredb ]; then
    cargo build --bin pgprototest &
    pgproto_test_build=$!
fi

# Give it some time. Eventually we could wait on a signal or poll system status
# through an endpoint.
sleep 5

# wait for the build to be complete, if it was running
if [ $pgproto_test_build -ne -1 ]; then
    wait $pgproto_test_build
fi

# Run protocol tests.
ret=0
./target/debug/pgprototest \
    --dir ./testdata/pgprototest \
    --dir ./testdata/pgprototest_glaredb \
    --addr localhost:6543 \
    --user glaredb \
    --password dummy \
    --database glaredb \
    -v || ret=$?

# Kill GlareDB.
kill "${glaredb_pid}" || true

# Print out log if failed.
if [[ "${ret}" -ne 0 ]]; then
    echo "--- GlareDB Logs ---"
    cat "${glaredb_log_file}"

    exit "${ret}"
fi
