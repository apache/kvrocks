#!/bin/sh
nohup node /kvrocks/web/main.js > web.log &
/kvrocks/bin/kvrocks -c /var/lib/kvrocks/kvrocks.conf --dir /var/lib/kvrocks
