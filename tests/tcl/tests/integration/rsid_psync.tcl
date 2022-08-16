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

start_server {tags {"repl"} overrides {use-rsid-psync yes}} {
    set A [srv 0 client]
    r set a b

    start_server {overrides {use-rsid-psync yes}} {
        r set c d
        set B [srv 0 client]

        test {replica psync sequence is in the range the master wal but full sync} {
            # same sequence
            assert_equal [s -1 master_repl_offset] [s master_repl_offset]

            $A slaveof [srv host] [srv port]
            wait_for_sync $A

            assert_equal 1 [s sync_full]
            assert_equal 1 [s sync_partial_ok]
        }

        # A -->->-- B
        # C -->->-- B
        start_server {overrides {use-rsid-psync yes}} {
            set C [srv 0 client]
            set C_host [srv 0 host]
            set C_port [srv 0 port]
            
            $C slaveof [srv -1 host] [srv -1 port]
            wait_for_sync $C

            test {chained replication can partially resync} {
                # C never sync with any slave
                assert_equal 0 [s sync_partial_ok]

                # A -->>-- C, currently topology is A -->>-- C -->>-- B
                $A slaveof $C_host $C_port
                wait_for_sync $A

                assert_equal 0 [s sync_full]
                assert_equal 1 [s sync_partial_ok]
            }

            test {chained replication can propagate updates} {
                $B set master B
                wait_for_ofs_sync $A $B
                assert_equal [$A get master] {B}
            }

            test {replica can partially resync after changing master but having the same history} {
                $A slaveof 127.0.0.1 1025
                after 1000

                # now topology is
                # A -->->-- B
                # C -->->-- B
                $A slaveof [srv -1 host] [srv -1 port]
                wait_for_sync $A

                # only partial sync, no full sync
                assert_equal 2 [s -1 sync_full]
                assert_equal 3 [s -1 sync_partial_ok]
            }

            test {replica can partially resync again after restarting} {
                r config rewrite
                restart_server 0 true false
                wait_for_condition 200 500 {
                    [s master_link_status] eq {up}
                } else {
                    fail "Replica C failed to resync with master B"
                }

                # don't increase sync_full, but increase sync_partial_ok
                assert_equal 2 [s -1 sync_full]
                assert_equal 4 [s -1 sync_partial_ok]
            }
        }
    }
}

start_server {tags {"repl"} overrides {use-rsid-psync yes}} {
    set replica [srv client]
    start_server {} {
        test {Replica(use-rsid-psync yes) can slaveof the master (use-rsid-psync no)} {
            $replica slaveof [srv host] [srv port]
            wait_for_sync $replica

            assert_equal 1 [s sync_full]
            assert_equal 1 [s sync_partial_ok]
        }
    }
}

start_server {tags {"repl"} overrides {use-rsid-psync no}} {
    set replica [srv client]
    start_server {overrides {use-rsid-psync yes}} {
        test {Replica(use-rsid-psync no) can slaveof the master (use-rsid-psync yes)} {
            $replica slaveof [srv host] [srv port]
            wait_for_sync $replica

            assert_equal 0 [s sync_full]
            assert_equal 1 [s sync_partial_ok]
        }
    }
}

