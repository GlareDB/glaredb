#!/usr/bin/env bash

set -xeuo pipefail

# manylinux2014-cross:aarch doesn't have unzip, install it.
apt-get update && apt-get install unzip

curl -L "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-linux-aarch_64.zip" -o protoc.zip
unzip -o protoc.zip
mv bin/protoc /bin
