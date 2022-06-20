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
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/unit/type/list-2.tcl

start_server {
    tags {"list"}
    overrides {
        "list-max-ziplist-size" 4
    }
} {
    source "tests/unit/type/list-common.tcl"

    foreach {type large} [array get largevalue] {
        tags {"slow"} {
            test "LTRIM stress testing - $type" {
                set mylist {}
                set startlen 32
                r del mylist

                # Start with the large value to ensure the
                # right encoding is used.
                r rpush mylist $large
                lappend mylist $large

                for {set i 0} {$i < $startlen} {incr i} {
                    set str [randomInt 9223372036854775807]
                    r rpush mylist $str
                    lappend mylist $str
                }

                for {set i 0} {$i < 1000} {incr i} {
                    set min [expr {int(rand()*$startlen)}]
                    set max [expr {$min+int(rand()*$startlen)}]
                    set before_len [llength $mylist]
                    set before_len_r [r llen mylist]
                    set mylist [lrange $mylist $min $max]
                    r ltrim mylist $min $max
                    assert_equal $mylist [r lrange mylist 0 -1] "failed trim"

                    set starting [r llen mylist]
                    for {set j [r llen mylist]} {$j < $startlen} {incr j} {
                        set str [randomInt 9223372036854775807]
                        r rpush mylist $str
                        lappend mylist $str
                        assert_equal $mylist [r lrange mylist 0 -1] "failed append match"
                    }
                }
            }
        }
    }
}
