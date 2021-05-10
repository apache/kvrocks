start_server {tags {"command"}} {
    test {the total commands count is 155} {
        r command count
    } {155}

    test {COMMAND info} {
        set e [lindex [r command info get mget] 0]
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

    test {COMMAND getkeys} {
        assert_equal {test} [r command getkeys get test]
        assert_equal {test test2} [r command getkeys mget test test2]
        assert_equal {test} [r command getkeys zadd test 1 m1]
    }
}
