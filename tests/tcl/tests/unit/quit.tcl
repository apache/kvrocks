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
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/unit/quit.tcl

start_server {tags {"quit"}} {
    proc format_command {args} {
        set cmd "*[llength $args]\r\n"
        foreach a $args {
            append cmd "$[string length $a]\r\n$a\r\n"
        }
        set _ $cmd
    }

    test "QUIT returns OK" {
        reconnect
        assert_equal OK [r quit]
        assert_error * {r ping}
    }

    test "Pipelined commands after QUIT must not be executed" {
        reconnect
        r write [format_command quit]
        r write [format_command set foo bar]
        r flush
        assert_equal OK [r read]
        assert_error * {r read}

        reconnect
        assert_equal {} [r get foo]
    }

    test "Pipelined commands after QUIT that exceed read buffer size" {
        reconnect
        r write [format_command quit]
        r write [format_command set foo [string repeat "x" 1024]]
        r flush
        assert_equal OK [r read]
        assert_error * {r read}

        reconnect
        assert_equal {} [r get foo]

    }
}
