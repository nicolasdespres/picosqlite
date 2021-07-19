#!/usr/bin/env bash
#
#::BEGIN::
# USAGE
#
#  release.sh <version>
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

# Print the message in the header of this file.
usage()
{
  sed -ne '/^#::BEGIN::/,/^#::END::/p' < "$0" \
    | sed -e '/^#::BEGIN::/d;/^#::END::/d' \
    | sed -e 's/^# //; s/^#//'
}

if [ $# -ne 1 ]
then
  usage
  exit 1
fi

VERSION="$1"

NAME="picosqlite"
ORIGINAL_FILE="$NAME.py"
RELEASE_FILE="${NAME}_$VERSION.py"
git tag -F "RelNotes/v$VERSION.txt" "v$VERSION"
sed -e "s/^__version__ = 'git'/__version__ = '$VERSION'/" \
    < "$ORIGINAL_FILE" \
    > "$RELEASE_FILE"
chmod a+x "$RELEASE_FILE"
