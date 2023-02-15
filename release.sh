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

# =========
# Functions
# =========

# Print its arguments on stderr prefixed by the base name of this script.
stderr()
{
  echo >&2 "`basename "$0"`: $@"
}

# Print its arguments on stderr prefixed by the base name of this script and
# a 'fatal' tag.
fatal()
{
  stderr "fatal: $@"
  exit 1
}

# =======================
# Script main entry point
# =======================

NAME="picosqlite"
ORIGINAL_FILE="$NAME.py"
VERSION="$(./describe.sh)"
[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] \
  || fatal "malformed version number: '$VERSION'"
DIST_DIR=dist
mkdir -p "$DIST_DIR"
RELEASE_FILE="${DIST_DIR}/${NAME}.py"
sed -e "s/^__version__ = 'git'/__version__ = '$VERSION'/" \
    < "$ORIGINAL_FILE" \
    > "$RELEASE_FILE"
chmod a+x "$RELEASE_FILE"
