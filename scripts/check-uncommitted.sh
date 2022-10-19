#!/usr/bin/env bash

# Check for uncommitted changes in the repo. Exits with a non-zero code if there
# are uncommitted changes.

set -e

# Diff files in the index.
git diff --exit-code

# Check for untracked files.
test -z "$(git status --porcelain)"
