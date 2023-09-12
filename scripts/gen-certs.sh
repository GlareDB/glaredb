#!/bin/sh

# Generate certificates using OpenSSL.
#
# This is use to generate certificates to include in the pgsrv image. Note that
# these certs are not signed by a CA, so attempting to use `verify-ca` and
# `verify-full` will fail.

openssl req -new -x509 -days 365 -nodes -text \
        -out server.crt \
        -keyout server.key \
        -subj "/CN=glaredb.com"

openssl req -noenc -newkey rsa -keyout client.key -out client.csr -subj '/CN=glaredb.com' -addext subjectAltName=DNS:glaredb.com

openssl x509 -req -in client.csr -CA server.crt -CAkey server.key -days 365 -out client.crt -copy_extensions copy
