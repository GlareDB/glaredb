#!/usr/bin/env bash

set -e

TRIES=3
QUERY_NUM=1

cat queries.sql | while read -r query; do
    sync
    # echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo "${QUERY_NUM}: ${query}"

    for i in $(seq 1 $TRIES); do
        ./glaredb --init ./create.sql -c ".timer on" -c "${query}"
    done

    QUERY_NUM=$((QUERY_NUM + 1))
done
