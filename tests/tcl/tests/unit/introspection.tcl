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
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/unit/introspection.tcl

start_server {tags {"introspection"}} {
    test {CLIENT LIST} {
        r client list
    } {id=* addr=*:* fd=* name=* age=* idle=* flags=N namespace=* qbuf=* *obuf=* cmd=client*}

    test {MONITOR can log executed commands} {
        set rd [redis_deferring_client]
        $rd monitor
        assert_match {*OK*} [$rd read]
        r set foo bar
        r get foo
        list [$rd read] [$rd read]
    } {*"set" "foo"*"get" "foo"*}

    test {CLIENT GETNAME should return NIL if name is not assigned} {
        r client getname
    } {}

    test {CLIENT LIST shows empty fields for unassigned names} {
        r client list
    } {*name= *}

    test {CLIENT SETNAME does not accept spaces} {
        catch {r client setname "foo bar"} e
        set e
    } {ERR*}

    test {CLIENT SETNAME can assign a name to this connection} {
        assert_equal [r client setname myname] {OK}
        r client list
    } {*name=myname*}

    test {CLIENT SETNAME can change the name of an existing connection} {
        assert_equal [r client setname someothername] {OK}
        r client list
    } {*name=someothername*}

    test {After CLIENT SETNAME, connection can still be closed} {
        set rd [redis_deferring_client]
        $rd client setname foobar
        assert_equal [$rd read] "OK"
        assert_match {*foobar*} [r client list]
        $rd close
        # Now the client should no longer be listed
        wait_for_condition 50 100 {
            [string match {*foobar*} [r client list]] == 0
        } else {
            fail "Client still listed in CLIENT LIST after SETNAME."
        }
    }

    test {Kill normal client} {
        set rd [redis_deferring_client]
        $rd client setname normal
        assert_equal [$rd read] "OK"
        assert_match {*normal*} [r client list]

        assert_equal {1} [r client kill skipme yes type normal]
        assert_equal {1} [r client kill skipme no type normal]
        reconnect
        # Now the client should no longer be listed
        wait_for_condition 50 100 {
            [string match {*normal*} [r client list]] == 0
        } else {
            fail "Killed client still listed in CLIENT LIST after killing."
        }
    }

    test {Kill pubsub client} {
        # subscribe clients
        set rd [redis_deferring_client]
        $rd client setname pubsub
        assert_equal [$rd read] "OK"
        $rd subscribe foo
        assert_equal [$rd read] {subscribe foo 1}
        assert_match {*pubsub*} [r client list]

        # psubscribe clients
        set rd1 [redis_deferring_client]
        $rd1 client setname pubsub_patterns
        assert_equal [$rd1 read] "OK"
        $rd1 psubscribe bar.*
        assert_equal [$rd1 read] {psubscribe bar.* 1}

        # normal clients
        set rd2 [redis_deferring_client]
        $rd2 client setname normal
        assert_equal [$rd2 read] "OK"

        assert_equal {2} [r client kill type pubsub]
        # Now the pubsub client should no longer be listed
        # but normal client should not be dropped 
        wait_for_condition 50 100 {
            [string match {*pubsub*} [r client list]] == 0 &&
            [string match {*normal*} [r client list]] == 1
        } else {
             fail "Killed client still listed in CLIENT LIST after killing."
        }
    }

    start_server {} {
        r slaveof [srv -1 host] [srv -1 port]
        wait_for_condition 500 100 {
            [string match {*connected*} [r role]]
        } else {
            fail "Slaves can't sync with master"
        }
            
        test {Kill slave client} {
            set partial_ok [s -1 sync_partial_ok]
            # Kill slave connection
            assert_equal {1} [r -1 client kill type slave]
            # Incr sync_partial_ok since slave reconnects
            wait_for_condition 50 100 {
                [expr $partial_ok+1] eq [s -1 sync_partial_ok]
            } else {
                fail "Slave should reconnect after disconnecting "
            }
        }

        test {Kill master client} {
            set partial_ok [s -1 sync_partial_ok]
            # Kill master connection by type
            assert_equal {1} [r client kill type master]
            # Incr sync_partial_ok since slave reconnects
            wait_for_condition 50 100 {
                [expr $partial_ok+1] eq [s -1 sync_partial_ok]
            } else {
                fail "Slave should reconnect after disconnecting "
            }

            set partial_ok [s -1 sync_partial_ok]
            # Kill master connection by addr
            set masteraddr [srv -1 host]:[srv -1 port]
            assert_equal {OK} [r client kill $masteraddr]
            # Incr sync_partial_ok since slave reconnects
            wait_for_condition 50 100 {
                [expr $partial_ok+1] eq [s -1 sync_partial_ok]
            } else {
                fail "Slave should reconnect after disconnecting "
            }
        }
    }

    test {DEBUG will freeze server} {
        set rd [redis_deferring_client]
        $rd DEBUG sleep 2.2
        $rd flush
        after 100

        set start_time [clock seconds]
        r set a b
        set time_elapsed [expr {[clock seconds]-$start_time}]
        assert {$time_elapsed >= 2}
    }
}
