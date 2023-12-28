#!/usr/bin/env bash

# Script for installing clickhouse in CI. This script is meant to be `source`d.
#
# Usage: source ./script/ci-install-clickhouse.sh

curl https://clickhouse.com/ | sh
mkdir -p ${HOME}/bin
cp ./clickhouse ${HOME}/bin/.

export PATH=$HOME/bin:$PATH

