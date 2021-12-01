start_server {tags {"exhash"}} {
    test {EXHSET/EXHSETEX/EXHLEN - Exhash creation} {
        array set exhash_arr {}
        array set exist_arr {}
        for {set i 0} {$i < 100} {incr i} {
            set key __avoid_collisions__[randstring 0 8 alpha]
            set val __avoid_collisions__[randstring 0 8 alpha]
            set ttl 0
            if {[info exists exhash_arr($key)]} {
                incr i -1
                continue
            }
            if {$i < 50} {
                set ttl 1
                r exhsetex exhash_arr $key $val $ttl
            } else {
                r exhset exhash_arr $key $val
                set exist_arr($key) $val
            }
            set exhash_arr($key) $val
        }
        after 2000
        list [r exhlen exhash_arr] [r exhlen exhash_arr exact]
    } {100 50}

    test {EXHSET supports multiple fields} {
        assert_equal "2" [r exhset hmsetmulti key1 val1 key2 val2]
        assert_equal "0" [r exhset hmsetmulti key1 val1 key2 val2]
        assert_equal "1" [r exhset hmsetmulti key1 val1 key3 val3]
    }

    test {EXHEXPIRE return 0 against non existing key or field} {
        r del k1
        r exhset k1 f1 v1
        list [r exhexpire k2 f1 10] [r exhexpire k1 f2 10]
    } {0 0}

    test {EXHTTL check return value} {
        r del k1
        r exhset k1 f1 v1
        list [r exhttl k2 f1] [r exhttl k1 f2] [r exhttl k1 f1]
    } {-2 -2 -1}

    test {EXHEXPIRE write on expire should work} {
        r del k1
        r exhset k1 f1 v1
        list [r exhget k1 f1] [r exhexpire k1 f1 15] [r exhttl k1 f1]
    } {v1 1 1[345]}

    test {EXHEXPIREAT check for EXHEXPIRE alike behavior} {
        r del k1
        r exhset k1 f1 v1
        r exhexpireat k1 f1 [expr [clock seconds]+15]
        r exhttl k1 f1
    } {1[345]}

    test {EXHEXPIREAT can not set ttl if timestamp less then now} {
        r del k1
        r exhset k1 f1 v1
        r exhexpireat k1 f1 [expr [clock seconds]-15]
        r exhttl k1 f1
    } {-1}

    test {EXHPERSIST check return value} {
        r del k1
        r exhset k1 f1 v1
        list [r exhpersist k2 f1] [r exhpersist k1 f2] [r exhpersist k1 f1]
    } {0 0 0}

    test {EXHPERSIST remove field expire should work} {
        r del k1
        r exhsetex k1 f1 v1 100
        r exhpersist k1 f1
        r exhttl k1 f1
    } {-1}

    test {EXHGET against non existing key} {
        set rv {}
        lappend rv [r exhget exhash_arr __123123123__]
        set _ $rv
    } {{}}

    test {EXHSETNX target key missing} {
        r exhsetnx exhash_arr __123123123__ foo
        r exhget exhash_arr __123123123__
    } {foo}

    test {EXHSETNX target key exists} {
        r exhsetnx exhash_arr __123123123__ bar
        set result [r exhget exhash_arr __123123123__]
        r exhdel exhash_arr __123123123__
        set _ $result
    } {foo}

    test {EXHMGET against non existing key and fields} {
        set rv {}
        lappend rv [r exhmget doesntexist __123123123__ __456456456__]
        lappend rv [r exhmget exhash_arr __123123123__ __456456456__]
        set _ $rv
    } {{{} {}} {{} {}}}

    test {EXHMGET against wrong type} {
        r set wrongtype somevalue
        assert_error "*wrong*" {r exhmget wrongtype field1 field2}
    }

    test {EXHMGET get multiple fields} {
        set keys {}
        set vals {}
        foreach {k v} [array get exist_arr] {
            lappend keys $k
            lappend vals $v
        }
        set err {}
        set result [r exhmget exhash_arr {*}$keys]
        if {$vals ne $result} {
            set err "$vals != $result"
            break
        }
        set _ $err
    } {}

    test {EXHKEYS get all existing keys} {
        lsort [r exhkeys exhash_arr]
    } [lsort [array names exist_arr *]]

    test {EXHVALS get all existing values} {
        set vals {}
        foreach {k v} [array get exist_arr] {
            lappend vals $v
        }
        set _ [lsort $vals]
    } [lsort [r exhvals exhash_arr]]

    test {EXHGETALL get all exhash field} {
        r exhset getall_key f0 v0
        r exhsetex getall_key f1 v1 15
        r exhsetex getall_key f2 v2 15
        r exhgetall getall_key withttl
    } {f0 v0 -1 f1 v1 1[345] f2 v2 1[345]}

    test {EXHDEL and return value} {
        set rv {}
        lappend rv [r exhdel exhash_arr nokey]
        set k [lindex [array names exhash_arr *] 0]
        lappend rv [r exhdel exhash_arr $k]
        lappend rv [r exhdel exhash_arr $k]
        lappend rv [r exhget exhash_arr $k]
        unset exhash_arr($k)
        set _ $rv
    } {0 1 0 {}}

    test {EXHDEL - more than a single value} {
        set rv {}
        r del myexhash
        r exhset myexhash a 1 b 2 c 3
        assert_equal 0 [r exhdel myexhash x y]
        assert_equal 2 [r exhdel myexhash a c f]
        r exhgetall myexhash
    } {b 2}

    test {EXHDEL - exhash becomes empty before deleting all specified fields} {
        r del myexhash
        r exhset myexhash a 1 b 2 c 3
        assert_equal 3 [r exhdel myexhash a b c d e]
        assert_equal 0 [r exists myexhash]
    }

    test {EXHEXISTS whether the key or field exists} {
        set rv {}
        r del myexhash
        r exhset myexhash existkey val
        lappend rv [r exhexists myexhash existkey]
        lappend rv [r exhexists myexhash nokey]
    } {1 0}

    test {EXHSTRLEN against the exhash} {
        set err {}
        foreach k [array names exhash_arr *] {
            if {[string length $exhash_arr($k)] ne [r exhstrlen exhash_arr $k]} {
                set err "[string length $exhash_arr($k)] != [r hstrlen exhash_arr $k]"
                break
            }
        }
        set _ $err
    } {}

    test {EXHSTRLEN against non existing field} {
        set rv {}
        lappend rv [r exhstrlen exhash_arr __123123123__]
        set _ $rv
    } {0}

    test {EXHSTRLEN corner cases} {
        set vals {
            -9223372036854775808 9223372036854775807 9223372036854775808
            {} 0 -1 x
        }
        foreach v $vals {
            r exhset exhash_arr field $v
            set len1 [string length $v]
            set len2 [r exhstrlen exhash_arr field]
            assert {$len1 == $len2}
        }
    }

    test {EXHINCRBY against non existing database key} {
        r del htest
        list [r exhincrby htest foo 2]
    } {2}

    test {EXHINCRBY against non existing hash key} {
        set rv {}
        r exhdel exhash_arr tmp
        lappend rv [r exhincrby exhash_arr tmp 2]
        lappend rv [r exhget exhash_arr tmp]
    } {2 2}

    test {EXHINCRBY against hash key created by hincrby itself} {
        set rv {}
        lappend rv [r exhincrby exhash_arr tmp 3]
        lappend rv [r exhget exhash_arr tmp]
    } {5 5}

    test {EXHINCRBY against hash key originally set with EXHSET} {
        r exhset exhash_arr tmp 100
        r exhincrby exhash_arr tmp 2
    } {102}

    test {EXHINCRBY over 32bit value} {
        r exhset exhash_arr tmp 17179869184
        r exhincrby exhash_arr tmp 1
    } {17179869185}

    test {EXHINCRBY over 32bit value with over 32bit increment} {
        r exhset exhash_arr tmp 17179869184
        r exhincrby exhash_arr tmp 17179869184
    } {34359738368}

    test {EXHINCRBY fails against hash value with spaces (left)} {
        r exhset exhash_arr str " 11"
        catch {r exhincrby exhash_arr str 1} smallerr
        set rv {}
        lappend rv [string match "ERR*not an integer*" $smallerr]
    } {1}

    test {EXHINCRBY fails against hash value with spaces (right)} {
        r exhset exhash_arr str "11 "
        catch {r exhincrby exhash_arr str 1} smallerr
        set rv {}
        lappend rv [string match "ERR*not an integer*" $smallerr]
    } {1}

    test {EXHINCRBY can detect overflows} {
        set e {}
        r exhset exhash n -9223372036854775484
        assert {[r exhincrby exhash n -1] == -9223372036854775485}
        catch {r exhincrby exhash n -10000} e
        set e
    } {*overflow*}

    test {EXHINCRBYFLOAT against non existing database key} {
        r del htest
        list [r exhincrbyfloat htest foo 2.5]
    } {2.5}

    test {EXHINCRBYFLOAT against non existing hash key} {
        set rv {}
        r exhdel exhash_arr tmp
        lappend rv [roundFloat [r exhincrbyfloat exhash_arr tmp 2.5]]
        lappend rv [roundFloat [r exhget exhash_arr tmp]]
    } {2.5 2.5}

    test {EXHINCRBYFLOAT against hash key created by hincrby itself} {
        set rv {}
        lappend rv [roundFloat [r exhincrbyfloat exhash_arr tmp 3.5]]
        lappend rv [roundFloat [r exhget exhash_arr tmp]]
    } {6 6}

    test {EXHINCRBYFLOAT against hash key originally set with HSET} {
        r exhset exhash_arr tmp 100
        roundFloat [r exhincrbyfloat exhash_arr tmp 2.5]
    } {102.5}

    test {EXHINCRBYFLOAT over 32bit value} {
        r exhset exhash_arr tmp 17179869184
        r exhincrbyfloat exhash_arr tmp 1
    } {17179869185}

    test {EXHINCRBYFLOAT over 32bit value with over 32bit increment} {
        r exhset exhash_arr tmp 17179869184
        r exhincrbyfloat exhash_arr tmp 17179869184
    } {34359738368}

    test {EXHINCRBYFLOAT fails against hash value with spaces (left)} {
        r exhset exhash_arr str " 11"
        catch {r exhincrbyfloat exhash_arr str 1} smallerr
        set rv {}
        lappend rv [string match "ERR*not*float*" $smallerr]
    } {1}

    test {EXHINCRBYFLOAT fails against hash value with spaces (right)} {
        r exhset exhash_arr str "11 "
        catch {r exhincrbyfloat exhash_arr str 1} smallerr
        set rv {}
        lappend rv [string match "ERR*not*float*" $smallerr]
    } {1}
}
