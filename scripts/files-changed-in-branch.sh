#!/usr/bin/env bash

# Util for detecting changes compared to the main branch.

FILES_CHANGED=$(git diff --name-only origin/main)

ARGUMENTS="$@"

for ARG in "$ARGUMENTS"; do
	# This will echo the first file changed it finds and exit
	echo "$FILES_CHANGED" | grep "$ARG" && exit 0
done

exit 1
