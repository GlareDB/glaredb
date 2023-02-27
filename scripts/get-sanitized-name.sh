#!/usr/bin/env bash

# Get a name only consisting of alphanumeric chars and underscores.

printf "$1" | tr -c -s '[:alnum:]' '_'
