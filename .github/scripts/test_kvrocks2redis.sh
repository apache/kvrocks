#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

redis_cli="$HOME/local/bin/redis-cli"
data_dir="$PWD/kvrocks2redis-ci-data"

fail() {
  echo "$1" >&2
  for log in ./kvrocks2redis*.log "$data_dir"/kvrocks*.log; do
    if [[ -f "$log" ]]; then
      printf '\n%s\n' "==> $log <==" >&2
      tail -n 100 "$log" >&2
    fi
  done
  exit 1
}

servers_ready() {
  [[ "$(timeout 1 "$redis_cli" -p 6379 ping 2>/dev/null)" == "PONG" &&
     "$(timeout 1 "$redis_cli" -p 6666 ping 2>/dev/null)" == "PONG" ]]
}

replicated_value() {
  timeout 1 "$redis_cli" -p 6379 "$@" 2>/dev/null
}

sentinel_synced() {
  [[ "$(replicated_value get __ci_k2r_ready)" == "1" ]]
}

sentinel_deletion_synced() {
  [[ "$(replicated_value exists __ci_k2r_ready)" == "0" ]]
}

append_cases_synced() {
  [[ "$(replicated_value get foo2)" == "2" &&
     "$(replicated_value getbit bfoo 900000)" == "1" &&
     "$(replicated_value exists foo)" == "0" &&
     "$(replicated_value exists zfoo)" == "0" ]]
}

wait_for_sync() {
  local deadline=$((SECONDS + 15))
  until "$1"; do
    if (( SECONDS >= deadline )); then
      fail "$2"
    fi
    sleep 0.1
  done
}

"$HOME/local/bin/redis-server" --daemonize yes
mkdir -p "$data_dir"
./build/kvrocks --dir "$data_dir" --pidfile "$PWD/kvrocks.pid" --daemonize yes
for _ in {1..100}; do
  if servers_ready; then
    break
  fi
  sleep 0.1
done
servers_ready || fail "Redis or Kvrocks failed to respond to PING."

printf 'data-dir %s\ndaemonize yes\noutput-dir ./\nnamespace.__namespace 127.0.0.1 6379\n' "$data_dir" > ./kvrocks2redis-ci.conf
./build/kvrocks2redis -c ./kvrocks2redis-ci.conf
if [[ "$(timeout 1 "$redis_cli" -p 6666 set __ci_k2r_ready 1)" != "OK" ]]; then
  fail "Failed to write the replication readiness key to Kvrocks."
fi
wait_for_sync sentinel_synced "Timed out waiting for kvrocks2redis to sync the readiness key."

# kvrocks2redis skips the range deletes emitted by FLUSHDB.
if [[ "$(timeout 1 "$redis_cli" -p 6666 del __ci_k2r_ready)" != "1" ]]; then
  fail "Failed to delete the replication readiness key from Kvrocks."
fi
wait_for_sync sentinel_deletion_synced "Timed out waiting for kvrocks2redis to sync deletion of the readiness key."

python3 utils/kvrocks2redis/tests/populate-kvrocks.py --password="" --flushdb=true
wait_for_sync append_cases_synced "Timed out waiting for kvrocks2redis to sync the final test writes."
python3 utils/kvrocks2redis/tests/check_consistency.py --src_password=""
