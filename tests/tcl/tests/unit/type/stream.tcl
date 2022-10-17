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

# This file is copied and modified from the Redis project
# https://github.com/redis/redis/blob/unstable/tests/unit/type/stream.tcl

# return value is like strcmp() and similar.
proc streamCompareID {a b} {
    if {$a eq $b} {return 0}
    lassign [split $a -] a_ms a_seq
    lassign [split $b -] b_ms b_seq
    if {$a_ms > $b_ms} {return 1}
    if {$a_ms < $b_ms} {return -1}
    # Same ms case, compare seq.
    if {$a_seq > $b_seq} {return 1}
    if {$a_seq < $b_seq} {return -1}
}

# Generate a random stream entry ID with the ms part between min and max
# and a low sequence number (0 - 999 range), in order to stress test
# XRANGE against a Tcl implementation implementing the same concept
# with Tcl-only code in a linear array.
proc streamRandomID {min_id max_id} {
    lassign [split $min_id -] min_ms min_seq
    lassign [split $max_id -] max_ms max_seq
    set delta [expr {$max_ms-$min_ms+1}]
    set ms [expr {$min_ms+[randomInt $delta]}]
    set seq [randomInt 1000]
    return $ms-$seq
}

# Tcl-side implementation of XRANGE to perform fuzz testing in the Redis
# XRANGE implementation.
proc streamSimulateXRANGE {items start end} {
    set res {}
    foreach i $items  {
        set this_id [lindex $i 0]
        if {[streamCompareID $this_id $start] >= 0} {
            if {[streamCompareID $this_id $end] <= 0} {
                lappend res $i
            }
        }
    }
    return $res
}

set content {} ;# Will be populated with Tcl side copy of the stream content.

start_server {
    tags {"stream"}
} {
    test {Blocking XREAD waiting new data} {
        r XADD s2{t} * old abcd1234
        set rd [redis_deferring_client]
        $rd XREAD BLOCK 20000 STREAMS s1{t} s2{t} s3{t} $ $ $
        wait_for_blocked_client
        r XADD s2{t} * new abcd1234
        set res [$rd read]
        assert {[lindex $res 0 0] eq {s2{t}}}
        assert {[lindex $res 0 1 0 1] eq {new abcd1234}}
        $rd close
    }

    test {Blocking XREAD waiting old data} {
        set rd [redis_deferring_client]
        $rd XREAD BLOCK 20000 STREAMS s1{t} s2{t} s3{t} $ 0-0 $
        r XADD s2{t} * foo abcd1234
        set res [$rd read]
        assert {[lindex $res 0 0] eq {s2{t}}}
        assert {[lindex $res 0 1 0 1] eq {old abcd1234}}
        $rd close
    }

    test {Blocking XREAD will not reply with an empty array} {
        r del s1
        r XADD s1 666 f v
        r XADD s1 667 f2 v2
        r XDEL s1 667
        set rd [redis_deferring_client]
        $rd XREAD BLOCK 10 STREAMS s1 666
        after 20
        assert {[$rd read] == {}} ;# before the fix, client didn't even block, but was served synchronously with {s1 {}}
        $rd close
    }

    test "Blocking XREAD for stream that ran dry (issue #5299)" {
        set rd [redis_deferring_client]

        # Add a entry then delete it, now stream's last_id is 666.
        r DEL mystream
        r XADD mystream 666 key value
        r XDEL mystream 666

        # Pass a ID smaller than stream's last_id, released on timeout.
        $rd XREAD BLOCK 10 STREAMS mystream 665
        assert_equal [$rd read] {}

        # Throw an error if the ID equal or smaller than the last_id.
        assert_error ERR*equal*smaller* {r XADD mystream 665 key value}
        assert_error ERR*equal*smaller* {r XADD mystream 666 key value}

        # Entered blocking state and then release because of the new entry.
        $rd XREAD BLOCK 0 STREAMS mystream 665
        wait_for_blocked_clients_count 1
        r XADD mystream 667 key value
        assert_equal [$rd read] {{mystream {{667-0 {key value}}}}}

        $rd close
    }

    #test "XREAD: XADD + DEL should not awake client" {
    #    set rd [redis_deferring_client]
    #    r del s1
    #    $rd XREAD BLOCK 20000 STREAMS s1 $
    #    wait_for_blocked_clients_count 1
    #    r multi
    #    r XADD s1 * old abcd1234
    #    r DEL s1
    #    r exec
    #    r XADD s1 * new abcd1234
    #    set res [$rd read]
    #    assert {[lindex $res 0 0] eq {s1}}
    #    assert {[lindex $res 0 1 0 1] eq {new abcd1234}}
    #    $rd close
    #}

    #test "XREAD: XADD + DEL + LPUSH should not awake client" {
    #    set rd [redis_deferring_client]
    #    r del s1
    #    $rd XREAD BLOCK 20000 STREAMS s1 $
    #    wait_for_blocked_clients_count 1
    #    r multi
    #    r XADD s1 * old abcd1234
    #    r DEL s1
    #    r LPUSH s1 foo bar
    #    r exec
    #    r DEL s1
    #    r XADD s1 * new abcd1234
    #    set res [$rd read]
    #    assert {[lindex $res 0 0] eq {s1}}
    #    assert {[lindex $res 0 1 0 1] eq {new abcd1234}}
    #    $rd close
    #}

    test {XREAD with same stream name multiple times should work} {
        r XADD s2 * old abcd1234
        set rd [redis_deferring_client]
        $rd XREAD BLOCK 20000 STREAMS s2 s2 s2 $ $ $
        wait_for_blocked_clients_count 1
        r XADD s2 * new abcd1234
        set res [$rd read]
        assert {[lindex $res 0 0] eq {s2}}
        assert {[lindex $res 0 1 0 1] eq {new abcd1234}}
        $rd close
    }

    #test {XREAD + multiple XADD inside transaction} {
    #    r XADD s2 * old abcd1234
    #    set rd [redis_deferring_client]
    #    $rd XREAD BLOCK 20000 STREAMS s2 s2 s2 $ $ $
    #    wait_for_blocked_clients_count 1
    #    r MULTI
    #    r XADD s2 * field one
    #    r XADD s2 * field two
    #    r XADD s2 * field three
    #    r EXEC
    #    set res [$rd read]
    #    assert {[lindex $res 0 0] eq {s2}}
    #    assert {[lindex $res 0 1 0 1] eq {field one}}
    #    assert {[lindex $res 0 1 1 1] eq {field two}}
    #    $rd close
    #}

    test {XDEL basic test} {
        r del somestream
        r xadd somestream * foo value0
        set id [r xadd somestream * foo value1]
        r xadd somestream * foo value2
        r xdel somestream $id
        assert {[r xlen somestream] == 2}
        set result [r xrange somestream - +]
        assert {[lindex $result 0 1 1] eq {value0}}
        assert {[lindex $result 1 1 1] eq {value2}}
    }

    # Here the idea is to check the consistency of the stream data structure
    # as we remove all the elements down to zero elements.
    test {XDEL fuzz test} {
        r del somestream
        set ids {}
        set x 0; # Length of the stream
        while 1 {
            lappend ids [r xadd somestream * item $x]
            incr x
            # Add enough elements to have a few radix tree nodes inside the stream.
            if {[dict get [r xinfo stream somestream] length] > 500} break
        }

        # Now remove all the elements till we reach an empty stream
        # and after every deletion, check that the stream is sane enough
        # to report the right number of elements with XRANGE: this will also
        # force accessing the whole data structure to check sanity.
        assert {[r xlen somestream] == $x}

        # We want to remove elements in random order to really test the
        # implementation in a better way.
        set ids [lshuffle $ids]
        foreach id $ids {
            assert {[r xdel somestream $id] == 1}
            incr x -1
            assert {[r xlen somestream] == $x}
            # The test would be too slow calling XRANGE for every iteration.
            # Do it every 100 removal.
            if {$x % 100 == 0} {
                set res [r xrange somestream - +]
                assert {[llength $res] == $x}
            }
        }
    }

    test {XRANGE fuzzing} {
        set items [r XRANGE mystream{t} - +]
        set low_id [lindex $items 0 0]
        set high_id [lindex $items end 0]
        for {set j 0} {$j < 100} {incr j} {
            set start [streamRandomID $low_id $high_id]
            set end [streamRandomID $low_id $high_id]
            set range [r xrange mystream{t} $start $end]
            set tcl_range [streamSimulateXRANGE $items $start $end]
            if {$range ne $tcl_range} {
                puts "*** WARNING *** - XRANGE fuzzing mismatch: $start - $end"
                puts "---"
                puts "XRANGE: '$range'"
                puts "---"
                puts "TCL: '$tcl_range'"
                puts "---"
                fail "XRANGE fuzzing failed, check logs for details"
            }
        }
    }

    test {XREVRANGE regression test for issue #5006} {
        # Add non compressed entries
        r xadd teststream 1234567891230 key1 value1
        r xadd teststream 1234567891240 key2 value2
        r xadd teststream 1234567891250 key3 value3

        # Add SAMEFIELD compressed entries
        r xadd teststream2 1234567891230 key1 value1
        r xadd teststream2 1234567891240 key1 value2
        r xadd teststream2 1234567891250 key1 value3

        assert_equal [r xrevrange teststream 1234567891245 -] {{1234567891240-0 {key2 value2}} {1234567891230-0 {key1 value1}}}

        assert_equal [r xrevrange teststream2 1234567891245 -] {{1234567891240-0 {key1 value2}} {1234567891230-0 {key1 value1}}}
    }

    test {XREAD streamID edge (no-blocking)} {
        r del x
        r XADD x 1-1 f v
        r XADD x 1-18446744073709551615 f v
        r XADD x 2-1 f v
        set res [r XREAD BLOCK 0 STREAMS x 1-18446744073709551615]
        assert {[lindex $res 0 1 0] == {2-1 {f v}}}
    }

    test {XREAD streamID edge (blocking)} {
        r del x
        set rd [redis_deferring_client]
        $rd XREAD BLOCK 0 STREAMS x 1-18446744073709551615
        wait_for_blocked_clients_count 1
        r XADD x 1-1 f v
        r XADD x 1-18446744073709551615 f v
        r XADD x 2-1 f v
        set res [$rd read]
        assert {[lindex $res 0 1 0] == {2-1 {f v}}}
        $rd close
    }

    test {XADD streamID edge} {
        r del x
        r XADD x 2577343934890-18446744073709551615 f v ;# we need the timestamp to be in the future
        r XADD x * f2 v2
        assert_equal [r XRANGE x - +] {{2577343934890-18446744073709551615 {f v}} {2577343934891-0 {f2 v2}}}
    }

    test {XTRIM with MAXLEN option basic test} {
        r DEL mystream
        for {set j 0} {$j < 1000} {incr j} {
            if {rand() < 0.9} {
                r XADD mystream * xitem $j
            } else {
                r XADD mystream * yitem $j
            }
        }
        r XTRIM mystream MAXLEN 666
        assert {[r XLEN mystream] == 666}
        r XTRIM mystream MAXLEN = 555
        assert {[r XLEN mystream] == 555}
    }

    test {XADD with LIMIT consecutive calls} {
        r del mystream
        for {set j 0} {$j < 100} {incr j} {
            r XADD mystream * xitem v
        }
        r XADD mystream MAXLEN = 55 * xitem v
        assert {[r xlen mystream] == 55}
        r XADD mystream MAXLEN = 55 * xitem v
        assert {[r xlen mystream] == 55}
    }
}
