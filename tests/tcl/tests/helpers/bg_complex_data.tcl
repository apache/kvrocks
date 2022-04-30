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
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/helpers/bg_complex_data.tcl

source tests/support/redis.tcl
source tests/support/util.tcl

set ::tlsdir "tests/tls"

proc bg_complex_data {host port db ops tls} {
    set r [redis $host $port 0 $tls]
    $r select $db
    createComplexDataset $r $ops
}

bg_complex_data [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4]
