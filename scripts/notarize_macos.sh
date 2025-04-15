#!/usr/bin/env bash

# Notarizes the macos binary.
#
# Assumes the binary has already been built **and signed**, and is located at
# 'target/release/glaredb'.

set -e

: ${APPLE_ID?"Need to set APPLE_ID"}
: ${APPLE_TEAM_ID?"Need to set APPLE_TEAM_ID"}
: ${APPLE_APP_PASSWORD?"Need to set APPLE_APP_PASSWORD"}

# Zip the signed binary.
pushd ./target/release
zip glaredb.zip glaredb
popd

# Notarize the binary.
xcrun notarytool submit ./target/release/glaredb.zip \
  --apple-id "$APPLE_ID" \
  --team-id "$APPLE_TEAM_ID" \
  --password "$APPLE_APP_PASSWORD" \
  --wait
