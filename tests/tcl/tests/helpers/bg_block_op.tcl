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
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/helpers/bg_block_op.tcl

source tests/support/redis.tcl
source tests/support/util.tcl

set ::tlsdir "tests/tls"

# This function sometimes writes sometimes blocking-reads from lists/sorted
# sets. There are multiple processes like this executing at the same time
# so that we have some chance to trap some corner condition if there is
# a regression. For this to happen it is important that we narrow the key
# space to just a few elements, and balance the operations so that it is
# unlikely that lists and zsets just get more data without ever causing
# blocking.
proc bg_block_op {host port db ops tls} {
    set r [redis $host $port 0 $tls]
    $r select $db

    for {set j 0} {$j < $ops} {incr j} {

        # List side
        set k list_[randomInt 10]
        set k2 list_[randomInt 10]
        set v [randomValue]

        randpath {
            randpath {
                $r rpush $k $v
            } {
                $r lpush $k $v
            }
        } {
            $r blpop $k 2
        } {
            $r blpop $k $k2 2
        }

        # Zset side
        set k zset_[randomInt 10]
        set k2 zset_[randomInt 10]
        set v1 [randomValue]
        set v2 [randomValue]

        randpath {
            $r zadd $k [randomInt 10000] $v
        } {
            $r zadd $k [randomInt 10000] $v [randomInt 10000] $v2
        } {
            $r bzpopmin $k 2
        } {
            $r bzpopmax $k 2
        }
    }
}

bg_block_op [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4]
