start_server {tags {"command"}} {
    test {kvrocks has 164 commands currently} {
        r command count
    } {164}

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
        set written_qps [s written_per_sec]
        set read_qps [s read_per_sec]
        set seek_qps [s seek_per_sec]
        set next_qps [s next_per_sec]
        # Based on the encoding of list, we can calculate the relationship
        # between Rocksdb QPS and Command QPS.
        assert {[expr abs($cmd_qps - $written_qps)] < 10}
        assert {[expr abs($cmd_qps - $read_qps)] < 10}
        assert {[expr abs($cmd_qps/2 - $seek_qps)] < 10}
        # prev_per_sec is almost the same as next_per_sec
        assert {[expr abs($cmd_qps - $next_qps)] < 10}
    }
}
