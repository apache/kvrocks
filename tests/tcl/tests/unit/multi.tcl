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
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/unit/multi.tcl

start_server {tags {"multi"}} {
    test {MUTLI / EXEC basics} {
        r del mylist
        r rpush mylist a
        r rpush mylist b
        r rpush mylist c
        r multi
        set v1 [r lrange mylist 0 -1]
        set v2 [r ping]
        set v3 [r exec]
        list $v1 $v2 $v3
    } {QUEUED QUEUED {{a b c} PONG}}

    test {DISCARD} {
        r del mylist
        r rpush mylist a
        r rpush mylist b
        r rpush mylist c
        r multi
        set v1 [r del mylist]
        set v2 [r discard]
        set v3 [r lrange mylist 0 -1]
        list $v1 $v2 $v3
    } {QUEUED OK {a b c}}

    test {Nested MULTI are not allowed} {
        set err {}
        r multi
        catch {[r multi]} err
        r exec
        set _ $err
    } {*ERR MULTI*}

    test {MULTI where commands alter argc/argv} {
        r sadd myset a
        r multi
        r spop myset
        list [r exec] [r exists myset]
    } {a 0}

    test {EXEC fails if there are errors while queueing commands #1} {
        r del foo1 foo2
        r multi
        r set foo1 bar1
        catch {r non-existing-command}
        r set foo2 bar2
        catch {r exec} e
        assert_match {EXECABORT*} $e
        list [r exists foo1] [r exists foo2]
    } {0 0}

    test {If EXEC aborts, the client MULTI state is cleared} {
        r del foo1 foo2
        r multi
        r set foo1 bar1
        catch {r non-existing-command}
        r set foo2 bar2
        catch {r exec} e
        assert_match {EXECABORT*} $e
        r ping
    } {PONG}

    test {Blocking commands ignores the timeout} {
        set m [r multi]
        r blpop empty_list 0
        r brpop empty_list 0
        set res [r exec]

        list $m $res
    } {OK {{} {}}}

    test {If EXEC aborts if commands are not allowed in MULTI-EXEC} {
        r multi
        r set foo1 bar1
        catch {r monitor}
        r set foo2 bar2
        catch {r exec} e
        assert_match {EXECABORT*} $e
        r ping
    } {PONG}

    test {MULTI-EXEC used in redis-sentinel for failover} {
        start_server {} {
            r MULTI
            r SLAVEOF [srv -1 host] [srv -1 port]
            r CONFIG REWRITE
            r CLIENT KILL TYPE normal
            r CLIENT KILL TYPE PUBSUB
            r EXEC
            reconnect
            assert_equal "slave" [s role]

            r MULTI
            r SLAVEOF NO ONE
            r CONFIG REWRITE
            r CLIENT kill TYPE NORMAL
            r client KILL type pubsub
            r EXEC
            reconnect
            assert_equal "master" [s role]
        }
    }
}
