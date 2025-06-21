#!/usr/bin/env bash

set -e

cd ./crates/glaredb_node

npm install

npm run build

npm test
