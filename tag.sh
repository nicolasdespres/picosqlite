#!/usr/bin/env bash
#
#::BEGIN::
# USAGE
#
#  tag.sh <version>
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

if [ $# -ne 1 ]
then
  usage
  exit 1
fi

VERSION="$1"
RELEASE_NOTES_FILE="RelNotes/v$VERSION.txt"
[ -r "$RELEASE_NOTES_FILE" ] \
  || fatal "Please write the release note file: '$RELEASE_NOTES_FILE'"
git tag -F "RelNotes/v$VERSION.txt" "v$VERSION"
