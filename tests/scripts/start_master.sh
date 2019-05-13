#!/usr/bin/env bash

# start master
sed -i -e 's/daemonize no/daemonize yes/g' ./kvrocks.conf
/cache/kvrocks -c ./kvrocks.conf
yum install -y nc
until nc -z 127.0.0.1 6666; do echo "kvrocks is not ready"; sleep 1; done
