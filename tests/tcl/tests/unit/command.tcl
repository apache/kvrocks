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

start_server {tags {"command"}} {
    test {kvrocks has 168 commands currently} {
        r command count
    } {169}

    test {acquire GET command info by COMMAND INFO} {
        set e [lindex [r command info get] 0]
        assert_equal [llength $e] 6
        assert_equal [lindex $e 0] get
        assert_equal [lindex $e 1] 2
        assert_equal [lindex $e 2] {readonly}
        assert_equal [lindex $e 3] 1
        assert_equal [lindex $e 4] 1
        assert_equal [lindex $e 5] 1
    }

    test {COMMAND - command entry length check} {
        set e [lindex [r command] 0]
        assert_equal [llength $e] 6
    }

    test {get keys of commands by COMMAND GETKEYS} {
        assert_equal {test} [r command getkeys get test]
        assert_equal {test test2} [r command getkeys mget test test2]
        assert_equal {test} [r command getkeys zadd test 1 m1]
    }

    test {get rocksdb ops by COMMAND INFO} {
        # Write data for 5 seconds to ensure accurate and stable QPS.
        for {set i 0} {$i < 25} {incr i} {
            for {set j 0} {$j < 100} {incr j} {
                r lpush key$i value$i
                r lrange key$i 0 1
            }
            after 200
        }
        set cmd_qps [s instantaneous_ops_per_sec]
        set put_qps [s put_per_sec]
        set get_qps [s get_per_sec]
        set seek_qps [s seek_per_sec]
        set next_qps [s next_per_sec]
        # Based on the encoding of list, we can calculate the relationship
        # between Rocksdb QPS and Command QPS.
        assert {[expr abs($cmd_qps - $put_qps)] < 10}
        assert {[expr abs($cmd_qps - $get_qps)] < 10}
        assert {[expr abs($cmd_qps/2 - $seek_qps)] < 10}
        # prev_per_sec is almost the same as next_per_sec
        assert {[expr abs($cmd_qps - $next_qps)] < 10}
    }

    test {get bgsave information from INFO command} {
        assert_equal 0 [s bgsave_in_progress]
        assert_equal -1 [s last_bgsave_time]
        assert_equal ok [s last_bgsave_status]
        assert_equal -1 [s last_bgsave_time_sec]

        assert_equal {OK} [r bgsave]
        wait_for_condition 100 500 {
            [s bgsave_in_progress] == 0
        } else {
            fail "Fail to bgsave"
        }

        set last_bgsave_time [s last_bgsave_time]
        assert {$last_bgsave_time > 1640507660}
        assert_equal ok [s last_bgsave_status]
        set last_bgsave_time_sec [s last_bgsave_time_sec]
        assert {$last_bgsave_time_sec < 3 && $last_bgsave_time_sec >= 0}
    }
}
