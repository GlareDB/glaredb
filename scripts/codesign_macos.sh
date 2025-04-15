#!/usr/bin/env bash

# Sign macos binaries.
#
# Assumes the binary has already been built, and is located at
# 'target/release/glaredb'.
#
# Keychain steps taken from <https://docs.github.com/en/actions/use-cases-and-examples/deploying/installing-an-apple-certificate-on-macos-runners-for-xcode-development>

set -e

: ${BUILD_CERTIFICATE_BASE64?"Need to set BUILD_CERTIFICATE_BASE64"}
: ${P12_PASSWORD?"Need to set P12_PASSWORD"}
: ${BUILD_PROVISION_PROFILE_BASE64?"Need to set BUILD_PROVISION_PROFILE_BASE64"}
: ${KEYCHAIN_PASSWORD?"Need to set KEYCHAIN_PASSWORD"}
: ${CERT_NAME?"Need to set CERT_NAME"}

# create variables
CERTIFICATE_PATH=$RUNNER_TEMP/build_certificate.p12
KEYCHAIN_PATH=$RUNNER_TEMP/app-signing.keychain-db

# import certificate and provisioning profile from secrets
echo -n "$BUILD_CERTIFICATE_BASE64" | base64 --decode -o $CERTIFICATE_PATH

# create temporary keychain
security create-keychain -p "$KEYCHAIN_PASSWORD" $KEYCHAIN_PATH
security set-keychain-settings -lut 21600 $KEYCHAIN_PATH
security unlock-keychain -p "$KEYCHAIN_PASSWORD" $KEYCHAIN_PATH

# import certificate to keychain
security import $CERTIFICATE_PATH -P "$P12_PASSWORD" -A -t cert -f pkcs12 -k $KEYCHAIN_PATH
security set-key-partition-list -S apple-tool:,apple: -k "$KEYCHAIN_PASSWORD" $KEYCHAIN_PATH
security list-keychain -d user -s $KEYCHAIN_PATH

# TODO:
# --options runtime: For notarization
# --timestamp: Attach trusted timestamp
codesign --verbose \
         --sign "${CERT_NAME}" \
         ./target/release/glaredb
