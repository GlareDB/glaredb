#!/usr/bin/env bash

# Generate certificates using OpenSSL.
#
# This is use to generate certificates to include in the pgsrv image. Note that
# these certs are not signed by a CA, so attempting to use `verify-ca` and
# `verify-full` will fail.

openssl req -new -x509 -days 365 -nodes -text \
        -out server.crt \
        -keyout server.key \
        -subj "/CN=glaredb.com"
