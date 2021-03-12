start_server {tags {"repl"}} {
    set A [srv 0 client]
    set A_host [srv 0 host]
    set A_port [srv 0 port]
    start_server {} {
        set B [srv 0 client]
        set B_host [srv 0 host]
        set B_port [srv 0 port]

        test {Set instance A as slave of B} {
            $A slaveof $B_host $B_port
            wait_for_condition 50 100 {
                [lindex [$A role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$A info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }
        }
    }
}

start_server {tags {"repl"}} {
    r set mykey foo
    r set mystring a
    r lpush mylist a b c
    r sadd myset a b c
    r hmset myhash a 1 b 2 c 3
    r zadd myzset 1 a 2 b 3 c

    start_server {} {
        test {Second server should have role master at first} {
            # Can't statify partial replication
            for {set i 0} {$i < 1000} {incr i} {
                r set $i $i
            }
            s role
        } {master}

        test {The role should immediately be changed to "replica"} {
            r slaveof [srv -1 host] [srv -1 port]
            s role
        } {slave}

        wait_for_sync r
        test {Sync should have transferred keys from master} {
            after 100
            assert_equal [r -1 get mykey] [r get mykey]
            assert_equal [r -1 get mystring] [r get mystring]
            assert_equal [r -1 lrange mylist 0 -1] [r lrange mylist 0 -1]
            assert_equal [r -1 hgetall myhash] [r hgetall myhash]
            assert_equal [r -1 zrange myzset 0 -1 WITHSCORES] [r ZRANGE myzset 0 -1 WITHSCORES]
            assert_equal [r -1 smembers myset] [r smembers myset]
        }

        test {The link status should be up} {
            s master_link_status
        } {up}

        test {SET on the master should immediately propagate} {
            r -1 set mykey bar

            wait_for_condition 500 100 {
                [r  0 get mykey] eq {bar}
            } else {
                fail "SET on master did not propagated on replica"
            }
        }

        test {FLUSHALL should replicate} {
            r -1 flushall
            after 100
            r -1 dbsize scan
            r 0 dbsize scan
            after 200
            if {$::valgrind} {after 2000}
            list [r -1 dbsize] [r 0 dbsize]
        } {0 0}

        test {ROLE in master reports master with a slave} {
            set res [r -1 role]
            lassign $res role offset slaves
            assert {$role eq {master}}
            assert {$offset > 0}
            assert {[llength $slaves] == 1}
            lassign [lindex $slaves 0] master_host master_port slave_offset
            assert {$slave_offset <= $offset}
        }

        test {ROLE in slave reports slave in connected state} {
            set res [r role]
            lassign $res role master_host master_port slave_state slave_offset
            assert {$role eq {slave}}
            assert {$slave_state eq {connected}}
        }
    }
}
