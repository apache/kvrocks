#!/bin/bash
if [ $# -eq 0 ]; then
    echo "usage: ./setup-env.sh bin_dir"
    exit 0
fi

yum install -y nc

BIN="$1/kvrocks"

# setup the master and wait for ready
$BIN -c tests/scripts/test-master.conf
until nc -z 127.0.0.1 6666; do echo "master is not ready"; sleep 1; done

# setup the slave and wait for ready
$BIN -c tests/scripts/test-slave.conf
until nc -z 127.0.0.1 6668; do echo "slave is not ready"; sleep 1; done
