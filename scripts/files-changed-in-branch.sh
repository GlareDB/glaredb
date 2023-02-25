#!/usr/bin/env bash

# Util for detecting changes compared to the main branch.

FILES_CHANGED=$(git diff --name-only origin/main -- "$@")

# If there are no files changed exit with error
[[ -z "$FILES_CHANGED" ]] && exit 1

echo "Files changed:"
echo "$FILES_CHANGED"
