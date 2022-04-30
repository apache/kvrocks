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
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/unit/limits.tcl

start_server {tags {"limits network"} overrides {maxclients 10}} {
    if {$::tls} {
        set expected_code "*I/O error*"
    } else {
        set expected_code "*ERR max*reached*"
    }
    test {Check if maxclients works refusing connections} {
        set c 0
        catch {
            while {$c < 50} {
                incr c
                set rd [redis_deferring_client]
                $rd ping
                $rd read
                after 100
            }
        } e
        assert {$c > 8 && $c <= 10}
        set e
    } $expected_code
}
