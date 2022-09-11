#!/usr/bin/env bash
set -ex

cd /kvrocks

if [ -n "$1" ]; then
args=$@
else
args="-c ./conf/kvrocks.conf"
fi

exec ./bin/kvrocks $args
