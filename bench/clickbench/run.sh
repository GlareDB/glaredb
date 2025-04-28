#!/usr/bin/env bash

set -e

TRIES=3
QUERY_NUM=1

cat queries.sql | while read -r query; do
    sync
    if [[ -r /proc/sys/vm/drop_caches ]]; then
        # Only try to run this if we have a proc file system.
        echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    fi

    echo "${QUERY_NUM}: ${query}"

    for i in $(seq 1 $TRIES); do
        ./glaredb --init ./create.sql -c ".timer on" -c "${query}"
    done

    QUERY_NUM=$((QUERY_NUM + 1))
done
