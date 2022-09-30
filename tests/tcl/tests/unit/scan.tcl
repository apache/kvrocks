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
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/unit/scan.tcl

start_server {tags {"scan network"}} {

    foreach enc {ziplist hashtable} {
        test "HSCAN with encoding $enc" {
            # Create the Hash
            r del hash
            if {$enc eq {ziplist}} {
                set count 30
            } else {
                set count 1000
            }
            set elements {}
            for {set j 0} {$j < $count} {incr j} {
                lappend elements key:$j $j
            }
            r hmset hash {*}$elements

            # Test HSCAN
            set cur 0
            set keys {}
            while 1 {
                set res [r hscan hash $cur]
                set cur [lindex $res 0]
                set k [lindex $res 1]
                lappend keys {*}$k
                if {$cur == 0} break
            }

            set keys2 {}
            foreach {k v} $keys {
                assert {$k eq "key:$v"}
                lappend keys2 $k
            }

            set keys2 [lsort -unique $keys2]
            assert_equal $count [llength $keys2]
        }
    }

    foreach enc {ziplist skiplist} {
        test "ZSCAN with encoding $enc" {
            # Create the Sorted Set
            r del zset
            if {$enc eq {ziplist}} {
                set count 30
            } else {
                set count 1000
            }
            set elements {}
            for {set j 0} {$j < $count} {incr j} {
                lappend elements $j key:$j
            }
            r zadd zset {*}$elements

            # Test ZSCAN
            set cur 0
            set keys {}
            while 1 {
                set res [r zscan zset $cur]
                set cur [lindex $res 0]
                set k [lindex $res 1]
                lappend keys {*}$k
                if {$cur == 0} break
            }

            set keys2 {}
            foreach {k v} $keys {
                assert {$k eq "key:$v"}
                lappend keys2 $k
            }

            set keys2 [lsort -unique $keys2]
            assert_equal $count [llength $keys2]
        }
    }


    test "SSCAN with PATTERN" {
        r del mykey
        r sadd mykey foo fab fiz foobar 1 2 3 4
        set res [r sscan mykey 0 MATCH foo* COUNT 10000]
        lsort -unique [lindex $res 1]
    } {foo foobar}

    test "HSCAN with PATTERN" {
        r del mykey
        r hmset mykey foo 1 fab 2 fiz 3 foobar 10 1 a 2 b 3 c 4 d
        set res [r hscan mykey 0 MATCH foo* COUNT 10000]
        lsort -unique [lindex $res 1]
    } {1 10 foo foobar}

    test "ZSCAN with PATTERN" {
        r del mykey
        r zadd mykey 1 foo 2 fab 3 fiz 10 foobar
        set res [r zscan mykey 0 MATCH foo* COUNT 10000]
        lsort -unique [lindex $res 1]
    }
}
