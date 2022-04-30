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

# Copyright (c) 2006-2020, Salvatore Sanfilippo
# See bundled license file licenses/LICENSE.redis for details.

# This file is copied and modified from the Redis project,
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/unit/keyspace.tcl

start_server {tags {"keyspace"}} {
    test {DEL against a single item} {
        r set x foo
        assert {[r get x] eq "foo"}
        r del x
        r get x
    } {}

    test {Vararg DEL} {
        r set foo1 a
        r set foo2 b
        r set foo3 c
        list [r del foo1 foo2 foo3 foo4] [r mget foo1 foo2 foo3]
    } {3 {{} {} {}}}

    test {KEYS with pattern} {
        foreach key {key_x key_y key_z foo_a foo_b foo_c} {
            r set $key hello
        }
        lsort [r keys foo*]
    } {foo_a foo_b foo_c}

    test {KEYS to get all keys} {
        lsort [r keys *]
    } {foo_a foo_b foo_c key_x key_y key_z}

    test {DBSIZE} {
        r dbsize scan
        after 100
        r dbsize
    } {6}

    test {DEL all keys} {
        foreach key [r keys *] {r del $key}
        r dbsize scan
        after 100
        r dbsize
    } {0}

    test {EXISTS} {
        set res {}
        r set newkey test
        append res [r exists newkey]
        r del newkey
        append res [r exists newkey]
    } {10}

    test {Zero length value in key. SET/GET/EXISTS} {
        r set emptykey {}
        set res [r get emptykey]
        append res [r exists emptykey]
        r del emptykey
        append res [r exists emptykey]
    } {10}

    test {Commands pipelining} {
        set fd [r channel]
        puts -nonewline $fd "SET k1 xyzk\r\nGET k1\r\nPING\r\n"
        flush $fd
        set res {}
        append res [string match OK* [r read]]
        append res [r read]
        append res [string match PONG* [r read]]
        format $res
    } {1xyzk1}

    test {Non existing command} {
        catch {r foobaredcommand} err
        string match ERR* $err
    } {1}

    test {RANDOMKEY} {
        r flushdb
        r set foo x
        r set bar y
        set foo_seen 0
        set bar_seen 0
        for {set i 0} {$i < 1000} {incr i} {
            set rkey [r randomkey]
            if {$rkey eq {foo}} {
                set foo_seen 1
            }
            if {$rkey eq {bar}} {
                set bar_seen 1
         
            }
        }
        set sum [expr $foo_seen + $bar_seen]
        assert {$sum > 0}
    }

    test {RANDOMKEY against empty DB} {
        r flushdb
        r randomkey
    } {}

    test {RANDOMKEY regression 1} {
        r flushdb
        r set x 10
        r del x
        r randomkey
    } {}

    test {KEYS * two times with long key, Github issue #1208} {
        r flushdb
        r set dlskeriewrioeuwqoirueioqwrueoqwrueqw test
        r keys *
        r keys *
    } {dlskeriewrioeuwqoirueioqwrueoqwrueqw}

    test {KEYS with multi namespace} {
        r flushdb
        r config set requirepass foobared

        set namespaces {test_ns1 test_ns2}
        set tokens {test_ns_token1 test_ns_token2}

        set ns_keyspaces {{foo_a foo_b foo_c key_l} {foo_d foo_e foo_f key_m}}
        set foo_prefix_keyspaces {{foo_a foo_b foo_c} {foo_d foo_e foo_f}}

        # Add namespaces and write key
        set index 0
        foreach ns $namespaces {
            r auth foobared
            r namespace add $ns [lindex $tokens $index]

            r auth [lindex $tokens $index]
            foreach key [lindex $ns_keyspaces $index] {
                r set $key hello
            }

            incr index
        }

        # Check KEYS and KEYS MATCH in different namespace
        set index 0
        foreach token $tokens {
            r auth $token
            assert_equal [lsort [r keys *]]  [lindex $ns_keyspaces $index]
            assert_equal [lsort [r keys foo*]]  [lindex $foo_prefix_keyspaces $index]

            incr index
        }
    }
}
