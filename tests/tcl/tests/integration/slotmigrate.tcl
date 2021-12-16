# Import crc16 table
source "tests/helpers/crc16_slottable.tcl"

start_server {tags {"Migrate from slave server"} overrides {cluster-enabled yes}} {
    # M is master, S is slave
    set M [srv 0 client]
    set M_host [srv 0 host]
    set M_port [srv 0 port]
    set masterid "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
    $M clusterx SETNODEID $masterid
    start_server {overrides {cluster-enabled yes}} {
        set S [srv 0 client]
        set S_host [srv 0 host]
        set S_port [srv 0 port]
        set slaveid "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
        $S clusterx SETNODEID $slaveid

        set cluster_nodes "$masterid 127.0.0.1 $M_port master - 0-100"
        set cluster_nodes "$cluster_nodes\n$slaveid 127.0.0.1 $S_port slave $masterid"
        $M clusterx SETNODES $cluster_nodes 1
        $S clusterx SETNODES $cluster_nodes 1

        test {MIGRATE - Slave cannot migrate slot} {
            catch {$S clusterx migrate $masterid 1} e
            assert_match {*Can't migrate slot*} $e
        }

        test {MIGRATE - Cannot migrate slot to a slave} {
            catch {$M clusterx migrate $slaveid 0} e
            assert_match {*Can't migrate slot to a slave*} $e
        }
    }
}

start_server {tags {"Src migration server"} overrides {cluster-enabled yes}} {
    set r0 [srv 0 client]
    set node0_host [srv 0 host]
    set node0_port [srv 0 port]
    set node0_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
    $r0 clusterx setnodeid $node0_id
    start_server {tags {"Src migration server"} overrides {cluster-enabled yes}} {
        set r1 [srv 0 client]
        set node1_host [srv 0 host]
        set node1_port [srv 0 port]
        set node1_pid [srv 0 pid]
        set node1_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
        $r1 clusterx setnodeid $node1_id

        set cluster_nodes "$node0_id 127.0.0.1 $node0_port master - 0-10000"
        set cluster_nodes "$cluster_nodes\n$node1_id 127.0.0.1 $node1_port master - 10001-16383"
        $r0 clusterx setnodes $cluster_nodes 1
        $r1 clusterx setnodes $cluster_nodes 1

        test {MIGRATE - Slot is out of range} {
            # Migrate slot -1
            catch {$r0 clusterx migrate $node1_id -1} e
            assert_match {*Slot is out of range*} $e

            # Migrate slot 16384
            catch {$r0 clusterx migrate $node1_id 16384} e
            assert_match {*Slot is out of range*} $e
        }

        test {MIGRATE - Cannot migrate slot to itself} {
            catch {$r0 clusterx migrate $node0_id 1} e
            assert_match {*Can't migrate slot to myself*} $e
        }

        test {MIGRATE - Fail to migrate slot if destination server is not running} {
            # Kill node1
            exec kill -9 $node1_pid
            after 50
            # Try migrating slot to node1
            set ret [$r0 clusterx migrate $node1_id 1]
            assert {$ret == "OK"}
            # Migrating started
            catch {[$r0 cluster migratestatus 1]} e
            assert_match {*START*} $e
            after 50
            # Migrating failed
            catch {[$r0 cluster migratestatus 1]} e
            assert_match {*no migrating*} $e
        }
    }
}

start_server {tags {"Src migration server"} overrides {cluster-enabled yes}} {
    set r0 [srv 0 client]
    set node0_host [srv 0 host]
    set node0_port [srv 0 port]
    set node0_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
    $r0 clusterx setnodeid $node0_id
    start_server {tags {"Dst migration server"} overrides {cluster-enabled yes}} {
        set r1 [srv 0 client]
        set node1_host [srv 0 host]
        set node1_port [srv 0 port]
        set node1_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
        $r1 clusterx setnodeid $node1_id

        set cluster_nodes "$node0_id 127.0.0.1 $node0_host master - 0-10000"
        set cluster_nodes "$cluster_nodes\n$node1_id 127.0.0.1 $node1_port master - 10001-16383"
        $r0 clusterx setnodes $cluster_nodes 1
        $r1 clusterx setnodes $cluster_nodes 1

        test {MIGRATE - Cannot migrate two slot at the same time} {
            # Write some keys
            set slot0_key [lindex $::CRC16_SLOT_TABLE 0]
            set count 2000
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot0_key $i
            }

            # Migrate slot 0
            set ret [$r0 clusterx migrate $node1_id 0]
            assert { $ret == "OK"}

            # Migrate slot 2
            catch {[$r0 clusterx migrate $node1_id 2]} e
            assert_match {*There is already a migrating slot*} $e

            # Migrate slot 0 success
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 0*migrating_state: SUCCESS*" [$r0 cluster migratestatus 0]]
            } else {
                fail "Fail to migrate slot 0"
            }

            # Check migrated data on destination server
            assert {[$r1 llen $slot0_key] == $count}
        }

        test {MIGRATE - Slot migrate all types of existing data} {
            # Set keys
            set slot1_tag [lindex $::CRC16_SLOT_TABLE 1]
            set slot1_key_string string_{$slot1_tag}
            set slot1_key_list list_{$slot1_tag}
            set slot1_key_hash hash_{$slot1_tag}
            set slot1_key_set set_{$slot1_tag}
            set slot1_key_zset zset_{$slot1_tag}
            set slot1_key_bitmap bitmap_{$slot1_tag}
            set slot1_key_sortint sortint_{$slot1_tag}

            # Clear keys
            $r0 del $slot1_key_string
            $r0 del $slot1_key_list
            $r0 del $slot1_key_hash
            $r0 del $slot1_key_set
            $r0 del $slot1_key_zset
            $r0 del $slot1_key_bitmap
            $r0 del $slot1_key_sortint

            # All keys belong to slot 1
            # Type: string
            $r0 set $slot1_key_string $slot1_key_string
            $r0 expire  $slot1_key_string 10000

            # Type: list
            $r0 rpush $slot1_key_list 0 1 2 3 4 5
            $r0 lpush $slot1_key_list 9 3 7 3 5 4
            $r0 lset $slot1_key_list 5 0
            $r0 linsert $slot1_key_list before 9 3
            $r0 ltrim $slot1_key_list 3 -3
            $r0 rpop $slot1_key_list
            $r0 lpop $slot1_key_list
            $r0 lrem $slot1_key_list 4 3
            $r0 expire  $slot1_key_list 10000

            # Type: hash
            $r0 hmset $slot1_key_hash 0 1 2 3 4 5 6 7
            $r0 hdel $slot1_key_hash 2
            $r0 expire $slot1_key_hash 10000

            # Type: set
            $r0 sadd $slot1_key_set 0 1 2 3 4 5
            $r0 srem $slot1_key_set 1 3
            $r0 expire  $slot1_key_set 10000

            # Type: zset
            $r0 zadd $slot1_key_zset 0 1 2 3 4 5 6 7
            $r0 zrem $slot1_key_zset 1 3
            $r0 expire  $slot1_key_zset 10000

            # Type: bitmap
            for {set i 1} {$i < 20} {incr i 2} {
                $r0 setbit $slot1_key_bitmap $i 1
            }
            for {set i 10000} {$i < 11000} {incr i 2} {
                $r0 setbit $slot1_key_bitmap $i 1
            }
            $r0 expire  $slot1_key_bitmap 10000

            # Type: sortint
            $r0 siadd $slot1_key_sortint 2 4 1 3
            $r0 sirem $slot1_key_sortint 1
            $r0 expire  $slot1_key_sortint 10000

            # Check src data
            assert {[$r0 exists $slot1_key_string] == 1}
            assert {[$r0 exists $slot1_key_list] == 1}
            assert {[$r0 exists $slot1_key_hash] == 1}
            assert {[$r0 exists $slot1_key_set] == 1}
            assert {[$r0 exists $slot1_key_zset] == 1}
            assert {[$r0 exists $slot1_key_bitmap] == 1}
            assert {[$r0 exists $slot1_key_sortint] == 1}

            # Get src data
            set lvalue [$r0 lrange $slot1_key_list 0 -1]
            set havlue [$r0 hgetall $slot1_key_hash]
            set svalue [$r0 smembers $slot1_key_set]
            set zvalue [$r0 zrange $slot1_key_zset 0 -1 WITHSCORES]
            set sivalue [$r0 sirange $slot1_key_sortint 0 -1]

            # Migrate slot 1, all keys above are belong to slot 1
            set ret [$r0 clusterx migrate $node1_id 1]
            assert {$ret == "OK"}

            # Wait finish slot migrating
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 1*migrating_state: SUCCESS*" [$r0 cluster migratestatus 1]]
            } else {
                fail "Fail to migrate slot 1"
            }
            after 10

            # Check dst data
            # Check string and expired time
            assert {[$r1 get $slot1_key_string] == "$slot1_key_string"}
            set expire_time [$r1 ttl $slot1_key_string]
            assert {$expire_time > 1000 && $expire_time <= 10000}

            # Check list and expired time
            assert {[$r1 lrange $slot1_key_list 0 -1] eq $lvalue}
            set expire_time [$r1 ttl $slot1_key_list]
            assert {$expire_time > 1000 && $expire_time <= 10000}

            # Check hash and expired time
            assert {[$r1 hgetall $slot1_key_hash] eq $havlue}
            set expire_time [$r1 ttl $slot1_key_hash]
            assert {$expire_time > 1000 && $expire_time <= 10000}

            # Check set and expired time
            assert {[$r1 smembers $slot1_key_set] eq $svalue}
            set expire_time [$r1 ttl $slot1_key_set]
            assert {$expire_time > 1000 && $expire_time <= 10000}

            # Check zset and expired time
            assert {[$r1 zrange $slot1_key_zset 0 -1 WITHSCORES] eq $zvalue}
            set expire_time [$r1 ttl $slot1_key_zset]
            assert {$expire_time > 1000 && $expire_time <= 10000}

            # Check bitmap and expired time
            for {set i 1} {$i < 20} {incr i 2} {
                assert {[$r1 getbit $slot1_key_bitmap $i] == {1}}
            }
            for {set i 10000} {$i < 11000} {incr i 2} {
                assert {[$r1 getbit $slot1_key_bitmap $i] == {1}}
            }
            for {set i 0} {$i < 20} {incr i 2} {
                assert {[$r1 getbit $slot1_key_bitmap $i] == {0}}
            }
            set expire_time [$r1 ttl $slot1_key_bitmap]
            assert {$expire_time > 1000 && $expire_time <= 10000}

            # Check sortint and expired time
            assert {[$r1 sirange $slot1_key_sortint 0 -1] eq $sivalue}
            set expire_time [$r1 ttl $slot1_key_sortint]
            assert {$expire_time > 1000 && $expire_time <= 10000}

            # Topology is changed on src server
            catch {$r0 exists $slot1_key_string} e
            assert_match {*MOVED*} $e
            catch {$r0 exists $slot1_key_list} e
            assert_match {*MOVED*} $e
            catch {$r0 exists $slot1_key_hash} e
            assert_match {*MOVED*} $e
            catch {$r0 exists $slot1_key_set} e
            assert_match {*MOVED*} $e
            catch {$r0 exists $slot1_key_zset} e
            assert_match {*MOVED*} $e
            catch {$r0 exists $slot1_key_bitmap} e
            assert_match {*MOVED*} $e
            catch {$r0 exists $slot1_key_sortint} e
            assert_match {*MOVED*} $e
        }


        test {MIGRATE - Accessing slot is forbiden on source server but not on destination server} {
            # migrate slot 3
            set slot3_key [lindex $::CRC16_SLOT_TABLE 3]
            $r0 set $slot3_key 3

            set ret [$r0 clusterx migrate $node1_id 3]
            assert {$ret == "OK"}
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 3*migrating_state: SUCCESS*" [$r0 cluster migratestatus 3]]
            } else {
                fail "Slot 3 can't migrate"
            }
            after 10

            # Writing the key belongs to slot 3 should be MOVED
            catch {$r0 set $slot3_key slot3} e
            assert_match {*MOVED*} $e

            catch {$r0 del $slot3_key} e
            assert_match {*MOVED*} $e

            # Reading the key belongs to slot 3 should be MOVED
            catch {$r0 exists $slot3_key} e
            assert_match {*MOVED*} $e

            # Writing the key belongs to slot 4 must be ok
            set slot4_key [lindex $::CRC16_SLOT_TABLE 4]
            set ret [$r0 set $slot4_key slot4]
            assert {$ret == "OK"}
        }

        test {MIGRATE - Slot isn't forbiden writing when starting migrating} {
            # Write much data for slot 5
            set slot5_key [lindex $::CRC16_SLOT_TABLE 5]
            set count 10000
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot5_key $i
            }

            # Migrate slot 5
            set ret [$r0 clusterx migrate $node1_id 5]
            assert {$ret == "OK"}

            # Migrate status START(doing)
            if {[string match "*migrating_slot: 5*migrating_state: START*" [$r0 cluster migratestatus 5]]} {
                # Write during migrating
                set num [$r0 lpush $slot5_key $count]
                assert {$num == [expr $count + 1]}
            } else {
                puts "Migrating too quickly? Please retry."
            }

            # Check dst server receiving all data when migrate slot snapshot
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 5*migrating_state: SUCCESS*" [$r0 cluster migratestatus 5]]
            } else {
                fail "Slot 5 migrating is not finished"
            }

            # Check value which is written during migrating
            set lastval [$r1 lpop $slot5_key]
            assert {$lastval == $count}
        }

        test {MIGRATE - Slot keys are cleared after migration} {
            set slot6_key [lindex $::CRC16_SLOT_TABLE 6]
            assert {[$r0 set $slot6_key "slot6"] == "OK"}
            set ret [$r0 clusterx migrate $node1_id 6]
            assert {$ret == "OK"}

            # Check key in src server
            assert {[$r0 get $slot6_key] == "slot6"}
            # Migrate slot
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 6*migrating_state: SUCCESS*" [$r0 cluster migratestatus 6]]
            } else {
                fail "Fail to migrate slot 6"
            }
            # Check key in destination server
            assert {[$r1 get $slot6_key] == "slot6"}
            # Check key in source server
            assert {[string match "*$slot6_key*" [$r0 keys *]] == 0}
        }

        test {MIGRATE - Migrate incremental data via parsing and filtering data in WAL} {
            # Slot15 keys
            set slot15_key_1 [lindex $::CRC16_SLOT_TABLE 15]
            set slot15_key_2 key:000042915392
            set slot15_key_3 key:000043146202
            set slot15_key_4 key:000044434182
            set slot15_key_5 key:000045189446
            set slot15_key_6 key:000047413016
            set slot15_key_7 key:000049190069
            set slot15_key_8 key:000049930003
            set slot15_key_9 key:000049980785
            set slot15_key_10 key:000056730838

            # Slot15 key for slowing migrate-speed when migrating existing data
            $r0 del $slot15_key_1
            # Slot15 all types keys string/hash/set/zset/list/sortint
            $r0 del $slot15_key_2
            $r0 del $slot15_key_3
            $r0 del $slot15_key_4
            $r0 del $slot15_key_5
            $r0 del $slot15_key_6
            $r0 del $slot15_key_7
            $r0 del $slot15_key_8
            $r0 del $slot15_key_9
            $r0 del $slot15_key_10

            # Set slow migrate
            $r0 config set migrate-speed 32
            catch {$r0 config get migrate-speed} e
            assert_match {*32*} $e

            set count 2000
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot15_key_1 $i
            }
            set ret [$r0 clusterx migrate $node1_id 15]
            assert {$ret == "OK"}

            # Write key that doesn't belong to this slot
            set slot12_key [lindex $::CRC16_SLOT_TABLE 12]
            $r0 del $slot12_key
            $r0 set $slot12_key slot12

            # Write increment operations include all kinds of types
            # Type: string
            # Use 'setex' to replace 'set' and 'expire', 'setex' will generate
            # set key and expire key
            $r0 setex $slot15_key_2 10000 15
            $r0 incrby $slot15_key_2 2
            $r0 decrby $slot15_key_2 1
            $r0 set $slot15_key_3 val
            $r0 del $slot15_key_3
            $r0 setbit $slot15_key_4 10086 1
            $r0 expire $slot15_key_4 1000
            set slot13_key [lindex $::CRC16_SLOT_TABLE 13]
            $r0 del $slot13_key
            # Just verify expireat binlog could be parsed
            $r0 set $slot13_key slot13
            $r0 expireat $slot13_key [expr 100 + [clock seconds]]
            # Verify del command
            set slot14_key [lindex $::CRC16_SLOT_TABLE 14]
            $r0 set $slot14_key slot14
            $r0 del $slot14_key

            # Type: hash
            $r0 hmset $slot15_key_5 f1 1 f2 2
            $r0 hdel $slot15_key_5 f1
            $r0 hincrby $slot15_key_5 f2 2
            $r0 hincrby $slot15_key_5 f2 -1

            # Type: set
            $r0 sadd $slot15_key_6 1 2
            $r0 srem $slot15_key_6 1

            # Type: zset
            $r0 zadd $slot15_key_7 2 m1
            $r0 zincrby $slot15_key_7 2 m1
            $r0 zincrby $slot15_key_7 -1 m1
            $r0 zadd $slot15_key_7 3 m3
            $r0 zrem $slot15_key_7 m3
            $r0 zadd $slot15_key_7 1 ""
            $r0 zrem $slot15_key_7 ""

            # Type: list
            $r0 lpush $slot15_key_8 item1
            $r0 rpush $slot15_key_8 item2
            $r0 lpush $slot15_key_8 item3
            $r0 rpush $slot15_key_8 item4
            assert {[$r0 lpop $slot15_key_8] == {item3}}
            assert {[$r0 rpop $slot15_key_8] == {item4}}
            $r0 lpush $slot15_key_8 item7
            $r0 rpush $slot15_key_8 item8
            $r0 lset $slot15_key_8 0 item5
            $r0 linsert $slot15_key_8 before item2 item6
            $r0 lrem $slot15_key_8 1 item7
            $r0 ltrim $slot15_key_8 1 -1

            # Type: bitmap
            for {set i 1} {$i < 20} {incr i 2} {
                $r0 setbit $slot15_key_9 $i 1
            }
            for {set i 10000} {$i < 11000} {incr i 2} {
                $r0 setbit $slot15_key_9 $i 1
            }

            # Type: sortint
            $r0 siadd $slot15_key_10 2 4 1 3
            $r0 sirem $slot15_key_10 2

            # Chek data in src server
            assert {[$r0 llen $slot15_key_1] == $count}
            set strvalue [$r0 get $slot15_key_2]
            set strttl [$r0 ttl $slot15_key_2]
            set bitvalue [$r0 getbit $slot15_key_4 10086]
            set bitttl [$r0 ttl $slot15_key_4]
            set hvalue [$r0 hgetall $slot15_key_5]
            set svalue [$r0 smembers $slot15_key_6]
            set zvalue [$r0 zrange $slot15_key_7 0 -1 withscores]
            set lvalue [$r0 lrange $slot15_key_8 0 -1]
            set sivalue [$r0 sirange $slot15_key_10 0 -1]

            # Wait for finishing
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 15*migrating_state: SUCCESS*" [$r0 cluster migratestatus 15]]
            } else {
                fail "Slot 15 migrating is not finished"
            }
            wait_for_condition 50 1000 {
                [string match "*15*SUCCESS*" [$r1 cluster importstatus 15]]
            } else {
                fail "Slot 15 importing is not finished"
            }

            # Check dst server receiving all data when migrate slot snapshot
            # Check data whether consistent
            # List key of existing data
            assert {[$r1 llen $slot15_key_1] == $count}
            # String
            assert {[$r1 get $slot15_key_2] eq $strvalue}
            assert {[expr $strttl - [$r1 ttl $slot15_key_2]] < 100}
            assert {[$r1 get $slot15_key_3] eq {}}
            assert {[$r1 getbit $slot15_key_4 10086] == $bitvalue}
            assert {[expr $bitttl - [$r1 ttl $slot15_key_4]] < 100}
            catch {$r1 exists $slot13_key} e
            assert_match {*MOVED*} $e
            # Hash
            assert {[$r1 hgetall $slot15_key_5] eq $hvalue}
            assert {[$r1 hget $slot15_key_5 f2] == 3}
            # Set
            assert {[$r1 smembers $slot15_key_6] eq $svalue}
            # Zset
            assert {[$r1 zrange $slot15_key_7 0 -1 withscores] eq $zvalue}
            assert {[$r1 zscore $slot15_key_7 m1] == 3}
            # List
            assert {[$r1 lrange $slot15_key_8 0 -1] eq $lvalue}
            # Bitmap
            for {set i 1} {$i < 20} {incr i 2} {
                assert {[$r1 getbit $slot15_key_9 $i] == {1}}
            }
            for {set i 10000} {$i < 11000} {incr i 2} {
                assert {[$r1 getbit $slot15_key_9 $i] == {1}}
            }
            for {set i 0} {$i < 20} {incr i 2} {
                assert {[$r1 getbit $slot15_key_9 $i] == {0}}
            }
            # Sortint
            assert {[$r1 sirange $slot15_key_10 0 -1] eq $sivalue}
            # Not migrate if the key doesn't belong to slot 1
            assert {[$r0 get $slot12_key] eq "slot12"}
            catch {$r1 exists $slot12_key} e
            assert_match {*MOVED*} $e
            assert {[$r0 exists $slot14_key] == 0}
        }

        test {MIGRATE - Slow migrate speed} {
            # Set slow speed
            $r0 config set migrate-speed 16
            catch {$r0 config get migrate-speed} e
            assert_match {*16*} $e

            # Construct key
            set slot16_key [lindex $::CRC16_SLOT_TABLE 16]
            $r0 del $slot16_key

            # More than pipeline size(16) and max items(16) in command
            set count 1000
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot16_key $i
            }
            # Migrate slot 16 from node1 to node0
            set ret [$r0 clusterx migrate $node1_id 16]
            assert {$ret == "OK"}

            # Should not finish 1.5s
            after 1500
            catch {[$r0 cluster migratestatus 16]} e
            assert_match {*migrating_slot: 16*START*} $e

            # Check if finish
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 16*SUCCESS*" [$r0 cluster migratestatus 16]]
            } else {
                fail "Slot 16 importing is not finished"
            }
        }
    }

}

start_server {tags {"Src migration server"} overrides {cluster-enabled yes}} {
    set r0 [srv 0 client]
    set node0_host [srv 0 host]
    set node0_port [srv 0 port]
    set node0_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
    $r0 clusterx setnodeid $node0_id
    start_server {tags {"Dst migration server"} overrides {cluster-enabled yes}} {
        set r1 [srv 0 client]
        set node1_host [srv 0 host]
        set node1_port [srv 0 port]
        set node1_pid [srv 0 pid]
        set node1_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
        $r1 clusterx setnodeid $node1_id

        set cluster_nodes "$node0_id 127.0.0.1 $node0_host master - 0-10000"
        set cluster_nodes "$cluster_nodes\n$node1_id 127.0.0.1 $node1_port master - 10001-16383"
        $r0 clusterx setnodes $cluster_nodes 1
        $r1 clusterx setnodes $cluster_nodes 1

        test {MIGRATE - Migrate slot with empty string key or value} {
            # Migrate slot 0
            $r0 set "" slot0
            set slot0_key [lindex $::CRC16_SLOT_TABLE 0]
            $r0 del $slot0_key
            $r0 set $slot0_key ""
            after 500
            set ret [$r0 clusterx migrate $node1_id 0]
            assert { $ret == "OK"}

            # Check migration task
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 0*migrating_state: SUCCESS*" [$r0 cluster migratestatus 0]]
            } else {
                fail "Fail to migrate slot 0"
            }

            # Check migrated data
            assert {[$r1 get ""] eq "slot0"}
            assert {[$r1 get $slot0_key] eq {}}
            $r1 del $slot0_key
        }

        test {MIGRATE - Migrate binary key-value} {
            # Slot 1 key using hash tag
            set slot1_key "\x3a\x88{key294989}\x3d\xaa"
            set count 257
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot1_key "\00\01"
            }
            set ret [$r0 clusterx migrate $node1_id 1]
            assert {$ret == "OK"}
            set slot1_key_2 "\x49\x1f\x7f{key294989}\xaf"
            $r0 set $slot1_key_2 "\00\01"
            after 1000
            # Check if finish
            wait_for_condition 50 100 {
                [string match "*1*SUCCESS*" [$r1 cluster importstatus 1]]
            } else {
                fail "Slot 1 importing is not finished"
            }
            # Check result
            assert {[$r1 llen $slot1_key] == $count}
            assert {[$r1 lpop $slot1_key] == "\00\01"}
            assert {[$r1 get $slot1_key_2] == "\00\01"}
        }

        test {MIGRATE - Migrate empty slot} {
            # Clear data of src and dst server
            $r0 flushall
            $r1 flushall
            after 500

            # Migrate slot 2
            set ret [$r0 clusterx migrate $node1_id 2]
            assert { $ret == "OK"}
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 2*migrating_state: SUCCESS*" [$r0 cluster migratestatus 2]]
            } else {
                fail "Fail to migrate slot 2"
            }

            # Check data of dst server
            catch {$r1 keys *} e
            assert_match {} $e
        }

        test {MIGRATE - Fail to migrate slot because destination server is killed while migrating} {
            set slot8_key [lindex $::CRC16_SLOT_TABLE 8]
            set count 20000
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot8_key $i
            }

            # Migrate data
            set ret [$r0 clusterx migrate $node1_id 8]
            assert {$ret == "OK"}

            # Check migrating start
            catch {[$r0 cluster migratestatus 8]} e
            assert_match {*migrating_slot: 8*START*} $e

            # Kill destination server itself
            exec kill -9 $node1_pid
            # Wait migration timeout
            after 1000
            # Can't success
            catch {[$r0 cluster migratestatus 8]} e
            assert_match {*no migrating*} $e
        }
    }
}

start_server {tags {"Source server will be changed to slave"} overrides {cluster-enabled yes}} {
    set r0 [srv 0 client]
    set node0_host [srv 0 host]
    set node0_port [srv 0 port]
    set node0_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
    $r0 clusterx SETNODEID $node0_id
    start_server {overrides {cluster-enabled yes}} {
        set r1 [srv 0 client]
        set node1_host [srv 0 host]
        set node1_port [srv 0 port]
        set node1_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
        $r1 clusterx SETNODEID $node1_id
        start_server {tags {"dst server"} overrides {cluster-enabled yes}} {
            set r2 [srv 0 client]
            set node2_host [srv 0 host]
            set node2_port [srv 0 port]
            set node2_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx02"
            $r2 clusterx SETNODEID $node2_id

            set cluster_nodes "$node0_id 127.0.0.1 $node0_port master - 0-10000"
            set cluster_nodes "$cluster_nodes\n$node1_id 127.0.0.1 $node1_port slave $node0_id"
            set cluster_nodes "$cluster_nodes\n$node2_id 127.0.0.1 $node2_port master - 10001-16383"
            $r0 clusterx SETNODES $cluster_nodes 1
            $r1 clusterx SETNODES $cluster_nodes 1
            $r2 clusterx SETNODES $cluster_nodes 1

            test {MIGRATE - Fail to migrate slot because source server is changed to slave during migrating} {
                set slot10_key [lindex $::CRC16_SLOT_TABLE 10]
                set count 10000
                for {set i 0} {$i < $count} {incr i} {
                    $r0 lpush $slot10_key $i
                }
                # Start migrating
                set ret [$r0 clusterx migrate $node2_id 10]
                assert {$ret == "OK"}
                catch {[$r0 cluster migratestatus 10]} e
                assert_match {*10*START*} $e
                # Change source server to slave
                $r0 slaveof $node1_host $node1_port
                after 1000
                # Check source migrating status
                catch {[$r0 cluster migratestatus 10]} e
                assert_match {*Slave can't migrate slot*} $e
                # Check destination importing status
                catch {[$r2 cluster importstatus 10]} e
                assert_match {*10*ERROR*} $e
            }

        }
    }
}

start_server {tags {"Source server will be flushed"} overrides {cluster-enabled yes}} {
    set r0 [srv 0 client]
    set node0_host [srv 0 host]
    set node0_port [srv 0 port]
    set node0_pid [srv 0 pid]
    set node0_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
    $r0 clusterx SETNODEID $node0_id
    start_server {overrides {cluster-enabled yes}} {
        set r1 [srv 0 client]
        set node1_host [srv 0 host]
        set node1_port [srv 0 port]
        set node1_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
        $r1 clusterx SETNODEID $node1_id

        set cluster_nodes "$node0_id 127.0.0.1 $node0_port master - 0-10000"
        set cluster_nodes "$cluster_nodes\n$node1_id 127.0.0.1 $node1_port master - 10001-16383"
        $r0 clusterx SETNODES $cluster_nodes 1
        $r1 clusterx SETNODES $cluster_nodes 1

        test {MIGRATE - Fail to migrate slot because source server is flushed} {
            set slot11_key [lindex $::CRC16_SLOT_TABLE 11]
            set count 2000
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot11_key $i
            }
            # Slow down migration speed
            $r0 config set migrate-speed 32
            catch {$r0 config get migrate-speed} e
            assert_match {*32*} $e
            # Migrate slot
            set ret [$r0 clusterx migrate $node1_id 11]
            assert {$ret == "OK"}
            # Ensure migration started
            wait_for_condition 10 50 {
                [string match "*11*START*" [$r0 cluster migratestatus 11]]
            } else {
                fail "Fail to start migrating slot 11"
            }
            # Flush source server
            set ret [$r0 flushdb]
            assert {$ret == "OK"}
            after 1000
            wait_for_condition 10 100 {
                [string match "*no migrating*" [$r0 cluster migratestatus 11]]
            } else {
                fail "Fail to flush server while migrating slot 11"
            }
        }

        test {MIGRATE - Fail to migrate slot because source server is killed while migrating} {
            set slot20_key [lindex $::CRC16_SLOT_TABLE 20]
            set count 2000
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot20_key $i
            }

            # Slow down migration speed
            $r0 config set migrate-speed 32
            catch {$r0 config get migrate-speed} e
            assert_match {*32*} $e

            # Migrate slot
            set ret [$r0 clusterx migrate $node1_id 20]
            assert {$ret == "OK"}
            # Check key has been migrated successfully
            wait_for_condition 10 100 {
                [string match "*$slot20_key*" [$r1 keys *]]
            } else {
                fail "Fail to migrate slot 20"
            }

            # Kill source server
            exec kill -9 $node0_pid
            after 100

            # Check that migrated data have been deleted in dst server
            assert {[string match "*$slot20_key*" [$r1 keys *]] == 0}
        }
    }
}

# Test migration and importing after full sycnchronization and failover
# 1. Let node1 as slave of node0, trigger full sycnchronization
# 2. Change node1 as a master
# 3. Migrate slot 0 from node1 to node2
# 4. Import slot 2 from node2 to node1
start_server {tags {"Server A"} overrides {cluster-enabled yes
                                           max-replication-mb 1
                                           rocksdb.write_buffer_size 1
                                           rocksdb.target_file_size_base 1}} {
    set r0 [srv 0 client]
    set node0_host [srv 0 host]
    set node0_port [srv 0 port]
    set node0_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
    $r0 clusterx setnodeid $node0_id
    start_server {tags {"Server B"} overrides {cluster-enabled yes}} {
        set r1 [srv 0 client]
        set node1_host [srv 0 host]
        set node1_port [srv 0 port]
        set node1_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
        $r1 clusterx setnodeid $node1_id
        start_server {tags {"Server C"} overrides {cluster-enabled yes}} {
            set r2 [srv 0 client]
            set node2_host [srv 0 host]
            set node2_port [srv 0 port]
            set node2_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx02"
            $r2 clusterx setnodeid $node2_id

            test {MIGRATE - Migrate and import slot after full sychronization and failover operation} {
                # Set cluster topology
                set cluster_nodes "$node0_id 127.0.0.1 $node0_port master - 0-10000"
                set cluster_nodes "$cluster_nodes\n$node2_id 127.0.0.1 $node2_port master - 10001-16383"
                $r0 clusterx setnodes $cluster_nodes 1
                $r1 clusterx setnodes $cluster_nodes 1
                $r2 clusterx setnodes $cluster_nodes 1

                # Write node0
                set count1 1000
                set slot0_key [lindex $::CRC16_SLOT_TABLE 0]
                for {set i 0} {$i < $count1} {incr i} {
                    $r0 lpush $slot0_key [string repeat A 10240]
                }
                set slot1_key [lindex $::CRC16_SLOT_TABLE 1]
                for {set i 0} {$i < $count1} {incr i} {
                    $r0 lpush $slot1_key [string repeat B 10240]
                }
                $r0 compact
                after 1000

                # Write node2
                set count3 2000
                set slot10003_key [lindex $::CRC16_SLOT_TABLE 10003]
                for {set i 0} {$i < $count3} {incr i} {
                    $r2 lpush $slot10003_key slot10003_key_$i
                }
                after 2000

                # Trigger full sychronization and failover operation
                $r1 slaveof $node0_host $node0_port
                # TODO: Check full sychronization is triggered successfully

                # waiting for completing replication
                wait_for_condition 50 500 {
                    [string match "*master_link_status:up*" [$r1 info replication]]
                } else {
                    fail "Fail to sychronize with master"
                }
                after 500
                $r1 slaveof no one

                # Change cluster topology
                set cluster_nodes "$node1_id 127.0.0.1 $node1_port master - 0-10000"
                set cluster_nodes "$cluster_nodes\n$node0_id 127.0.0.1 $node0_port slave $node1_id"
                set cluster_nodes "$cluster_nodes\n$node2_id 127.0.0.1 $node2_port master - 10001-16383"
                $r0 clusterx setnodes $cluster_nodes 2
                $r1 clusterx setnodes $cluster_nodes 2
                $r2 clusterx setnodes $cluster_nodes 2

                # Try to migrate slot 0 from node1
                assert {[$r1 clusterx migrate $node2_id 0] == "OK"}
                # Migrate slot 0 success
                wait_for_condition 50 200 {
                    [string match "*0*SUCCESS*" [$r1 cluster migratestatus 0]]
                } else {
                    fail "Fail to migrate slot 0"
                }
                # Check migrated data on destination server
                assert {[$r2 llen $slot0_key] == $count1}

                # Try to import slot 10003 to node1
                assert {[$r2 clusterx migrate $node1_id 10003] == "OK"}
                # Import slot 10003 success
                wait_for_condition 50 200 {
                    [string match "*10003*SUCCESS*" [$r2 cluster migratestatus 10003]]
                } else {
                    fail "Fail to migrate slot 10003"
                }
                # Check imported data on node1
                assert {[$r1 llen $slot10003_key] == $count3}
            }
        }
    }
}
