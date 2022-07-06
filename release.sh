#!/usr/bin/env bash
#
#::BEGIN::
# USAGE
#
#  release.sh
#
# DESCRIPTION
#
#  Release the project.
#
# Copyright (c) 2021, Nicolas Despres
# Report any problem to <nicolas.despres@gmail.com>
#::END::
#

set -o errexit
set -o nounset

export LC_ALL=C
unset CDPATH

NAME="picosqlite"
ORIGINAL_FILE="$NAME.py"
VERSION="$(./describe.sh)"
DIST_DIR=dist
mkdir -p "$DIST_DIR"
RELEASE_FILE="${DIST_DIR}/${NAME}.py"
sed -e "s/^__version__ = 'git'/__version__ = '$VERSION'/" \
    < "$ORIGINAL_FILE" \
    > "$RELEASE_FILE"
chmod a+x "$RELEASE_FILE"
