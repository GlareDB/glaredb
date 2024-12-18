#!/usr/bin/env bash

# Check if the repo is clean.

set -e

if [ -n "$(git status --porcelain)" ]; then
    git diff
    exit 1
fi
