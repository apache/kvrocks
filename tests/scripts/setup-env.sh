#!/bin/bash
if [ $# -eq 0 ]; then
    echo "usage: ./setup-env.sh bin_dir"
    exit 0
fi

BIN="$1/kvrocks"

# clean the old data dir
rm -rf /tmp/*
# setup the master and wait for ready
$BIN -c tests/scripts/test-master.conf
until nc -z 127.0.0.1 6666; do echo "master is not ready"; sleep 1; done

# setup the slave and wait for ready
$BIN -c tests/scripts/test-slave.conf
until nc -z 127.0.0.1 6668; do echo "slave is not ready"; sleep 1; done

# setup the codis test server group1 and wait for ready
$BIN -c tests/scripts/test-codis-group1.conf
until nc -z 127.0.0.1 6670; do echo "codis test server group1 is not ready"; sleep 1; done

# setup the codis test server group2 and wait for ready
$BIN -c tests/scripts/test-codis-group2.conf
until nc -z 127.0.0.1 6672; do echo "codis test server group2 is not ready"; sleep 1; done
