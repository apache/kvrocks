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

proc simulate_bit_op {op args} {
    set maxlen 0
    set j 0
    set count [llength $args]
    foreach a $args {
        binary scan $a b* bits
        set b($j) $bits
        if {[string length $bits] > $maxlen} {
            set maxlen [string length $bits]
        }
        incr j
    }
    for {set j 0} {$j < $count} {incr j} {
        if {[string length $b($j)] < $maxlen} {
            append b($j) [string repeat 0 [expr $maxlen-[string length $b($j)]]]
        }
    }
    set out {}
    for {set x 0} {$x < $maxlen} {incr x} {
        set bit [string range $b(0) $x $x]
        if {$op eq {not}} {set bit [expr {!$bit}]}
        for {set j 1} {$j < $count} {incr j} {
            set bit2 [string range $b($j) $x $x]
            switch $op {
                and {set bit [expr {$bit & $bit2}]}
                or  {set bit [expr {$bit | $bit2}]}
                xor {set bit [expr {$bit ^ $bit2}]}
            }
        }
        append out $bit
    }
    binary format b* $out
}

start_server {tags {"bitmap"}} {
    proc set2setbit {key str} {
        set bitlen [expr {8 * [string length $str]}]
        binary scan $str B$bitlen bit_str
        for {set x 0} {$x < $bitlen} {incr x} {
            r setbit $key $x [string index $bit_str $x]
        }
    }

    test {GET bitmap string after setbit} {
        r setbit b0 0 0
        r setbit b1 35 0
        set2setbit b2 "\xac\x81\x32\x5d\xfe"
        set2setbit b3 "\xff\xff\xff\xff"
        list [r get b0] [r get b1] [r get b2] [r get b3]
    } [list "\x00" "\x00\x00\x00\x00\x00" "\xac\x81\x32\x5d\xfe" "\xff\xff\xff\xff"]

    test {GET bitmap with out of max size} {
        r config set max-bitmap-to-string-mb 1
        r setbit b0 8388609 0
        catch {r get b0} e
        set e
    } {ERR Operation aborted: The size of the bitmap *}

    test "SETBIT/GETBIT/BITCOUNT/BITPOS boundary check (type bitmap)" {
        r del b0
        set max_offset [expr 4*1024*1024*1024-1]
        assert_error "*out of range*" {r setbit b0 [expr $max_offset+1] 1}
        r setbit b0 $max_offset 1
        assert_equal 1 [r getbit b0 $max_offset ]
        assert_equal 1 [r bitcount b0 0 [expr $max_offset / 8] ]
        assert_equal $max_offset  [r bitpos b0 1 ]
    }

    test {BITOP NOT (known string)} {
        set2setbit s "\xaa\x00\xff\x55"
        r bitop not dest s
        r get dest
    } "\x55\xff\x00\xaa"

    test {BITOP where dest and target are the same key} {
        set2setbit s "\xaa\x00\xff\x55"
        r bitop not s s
        r get s
    } "\x55\xff\x00\xaa"

    test {BITOP AND|OR|XOR don't change the string with single input key} {
        set2setbit a "\x01\x02\xff"
        r bitop and res1 a
        r bitop or  res2 a
        r bitop xor res3 a
        list [r get res1] [r get res2] [r get res3]
    } [list "\x01\x02\xff" "\x01\x02\xff" "\x01\x02\xff"]

    test {BITOP missing key is considered a stream of zero} {
        set2setbit a "\x01\x02\xff"
        r bitop and res1 no-suck-key a
        r bitop or  res2 no-suck-key a no-such-key
        r bitop xor res3 no-such-key a
        list [r get res1] [r get res2] [r get res3]
    } [list "\x00\x00\x00" "\x01\x02\xff" "\x01\x02\xff"]

    test {BITOP shorter keys are zero-padded to the key with max length} {
        set2setbit a "\x01\x02\xff\xff"
        set2setbit b "\x01\x02\xff"
        r bitop and res1 a b
        r bitop or  res2 a b
        r bitop xor res3 a b
        list [r get res1] [r get res2] [r get res3]
    } [list "\x01\x02\xff\x00" "\x01\x02\xff\xff" "\x00\x00\x00\xff"]

    foreach op {and or xor} {
        test "BITOP $op fuzzing" {
            for {set i 0} {$i < 10} {incr i} {
                r flushall
                set vec {}
                set veckeys {}
                set numvec [expr {[randomInt 10]+1}]
                for {set j 0} {$j < $numvec} {incr j} {
                    set str [randstring 0 1000]
                    lappend vec $str
                    lappend veckeys vector_$j
                    set2setbit vector_$j $str
                }
                r bitop $op target {*}$veckeys
                assert_equal [r get target] [simulate_bit_op $op {*}$vec]
            }
        }
    }

    test {BITOP NOT fuzzing} {
        for {set i 0} {$i < 10} {incr i} {
            r del str
            set str [randstring 0 1000]
            set2setbit str $str
            r bitop not target str
            assert_equal [r get target] [simulate_bit_op not $str]
        }
    }

    test {BITOP with non string source key} {
        r del c
        set2setbit a "\xaa\x00\xff\x55"
        set2setbit b "\xaa\x00\xff\x55"
        r lpush c foo
        catch {r bitop xor dest a b c d} e
        set e
    } {*WRONGTYPE*}
    
    test {BITOP with empty string after non empty string (Redis issue #529)} {
        r flushdb
        set2setbit a "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        r bitop or x a b
    } {32}
}