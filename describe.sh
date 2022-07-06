#!/bin/sh

git describe --match='v*' --dirty "$@"
