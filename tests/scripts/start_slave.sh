#!/usr/bin/env bash

# start slave
sed -i -e 's/port 6666/port 6668/g' ./kvrocks.conf
sed -i -e 's/# slaveof 127.0.0.1 6379/slaveof 127.0.0.1 6666/g' ./kvrocks.conf
sed -i -e 's/dir \/data\/kvrocks/dir \/data\/kvrocks_slave/g' ./kvrocks.conf
/cache/kvrocks -c ./kvrocks.conf -p /var/run/kvrocks-slave.pid
until nc -z 127.0.0.1 6668; do echo "kvrocks slave is not ready"; sleep 1; done
