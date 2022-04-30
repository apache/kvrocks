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
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/helpers/gen_write_load.tcl

source tests/support/redis.tcl

set ::tlsdir "tests/tls"

proc gen_write_load {host port seconds tls} {
    set start_time [clock seconds]
    set r [redis $host $port 1 $tls]
    $r select 9
    while 1 {
        $r set [expr rand()] [expr rand()]
        if {[clock seconds]-$start_time > $seconds} {
            exit 0
        }
    }
}

gen_write_load [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3]
