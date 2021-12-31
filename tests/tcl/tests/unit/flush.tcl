start_server {tags {"flushdb and flushall"}} {
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    set flush_flags {sync async {}}
    set flush_cmds {flushall flushdb}

    foreach flag $flush_flags {
        foreach cmd $flush_cmds {
            start_server {} {
                r slaveof $master_host $master_port
                set dir [lindex [r config get dir] 1]

                # write data
                r -1 set k1 v1
                r -1 set k2 v2
                r -1 set k3 v3

                after 1000
                assert_equal {v1 v2 v3} [r -1 mget k1 k2 k3]
                assert_equal {v1 v2 v3} [r mget k1 k2 k3]

                # exec flushall or flushdb [sync|async|]
                if {$flag eq {}} {
                    assert_equal "OK" [r -1 $cmd]
                } else {
                    assert_equal "OK" [r -1 $cmd $flag]
                }

                # check log file of slave
                if {$flag eq "sync"} {
                    wait_for_condition 50 1000 {
                        [log_file_matches $dir/PegaDB.INFO "*exec propagate compact*"]
                    } else {
                        fail "Fail to propagate compact to salve when flushall/db sync"
                    }  
                }

                # check data
                after 1000
                assert_equal {{} {} {}} [r -1 mget k1 k2 k3]
                assert_equal {{} {} {}} [r mget k1 k2 k3]
            }
        }
    }
}
