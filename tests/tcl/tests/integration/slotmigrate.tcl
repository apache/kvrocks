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
            catch {$S clusterx migrate 1 $masterid} e
            assert_match {*Can't migrate slot*} $e
        }

        test {MIGRATE - Cannot migrate slot to a slave} {
            catch {$M clusterx migrate 0 $slaveid} e
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
            catch {$r0 clusterx migrate -1 $node1_id} e
            assert_match {*Slot is out of range*} $e

            # Migrate slot 16384
            catch {$r0 clusterx migrate 16384 $node1_id} e
            assert_match {*Slot is out of range*} $e
        }

        test {MIGRATE - Cannot migrate slot to itself} {
            catch {$r0 clusterx migrate 1 $node0_id} e
            assert_match {*Can't migrate slot to myself*} $e
        }

        test {MIGRATE - Fail to migrate slot if destination server is not running} {
            # Kill node1
            exec kill -9 $node1_pid
            after 50
            # Try migrating slot to node1
            set ret [$r0 clusterx migrate 1 $node1_id]
            assert {$ret == "OK"}
            after 50
            # Migrating failed
            catch {[$r0 cluster info]} e
            assert_match {*1*fail*} $e
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
            set ret [$r0 clusterx migrate 0 $node1_id]
            assert { $ret == "OK"}

            # Migrate slot 2
            catch {[$r0 clusterx migrate 2 $node1_id]} e
            assert_match {*There is already a migrating slot*} $e

            # Migrate slot 0 success
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 0*migrating_state: success*" [$r0 cluster info]]
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
            set slot1_key_string2 string2_{$slot1_tag}
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

            # Expired string key
            $r0 set $slot1_key_string2 $slot1_key_string2 ex 1
            after 3000
            assert_equal [$r0 get $slot1_key_string2] {}

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
            set ret [$r0 clusterx migrate 1 $node1_id]
            assert {$ret == "OK"}

            # Wait finish slot migrating
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 1*migrating_state: success*" [$r0 cluster info]]
            } else {
                fail "Fail to migrate slot 1"
            }
            after 10

            # Check dst data
            # Check string and expired time
            assert {[$r1 get $slot1_key_string] == "$slot1_key_string"}
            set expire_time [$r1 ttl $slot1_key_string]
            assert {$expire_time > 1000 && $expire_time <= 10000}

            assert_equal [$r1 get $slot1_key_string2] {}

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


        test {MIGRATE - Accessing slot is forbidden on source server but not on destination server} {
            # migrate slot 3
            set slot3_key [lindex $::CRC16_SLOT_TABLE 3]
            $r0 set $slot3_key 3

            set ret [$r0 clusterx migrate 3 $node1_id]
            assert {$ret == "OK"}
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 3*migrating_state: success*" [$r0 cluster info]]
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

        test {MIGRATE - Slot isn't forbidden writing when starting migrating} {
            # Write much data for slot 5
            set slot5_key [lindex $::CRC16_SLOT_TABLE 5]
            set count 10000
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot5_key $i
            }

            # Migrate slot 5
            set ret [$r0 clusterx migrate 5 $node1_id]
            assert {$ret == "OK"}

            # Migrate status START(migrating)
            if {[string match "*migrating_slot: 5*migrating_state: start*" [$r0 cluster info]]} {
                # Write during migrating
                set num [$r0 lpush $slot5_key $count]
                assert {$num == [expr $count + 1]}
            } else {
                puts "Migrating too quickly? Please retry."
            }

            # Check dst server receiving all data when migrate slot snapshot
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 5*migrating_state: success*" [$r0 cluster info]]
            } else {
                fail "Slot 5 migrating is not finished"
            }

            # Check value which is written during migrating
            set lastval [$r1 lpop $slot5_key]
            assert {$lastval == $count}
        }

        test {MIGRATE - Slot keys are not cleared after migration but cleared after setslot} {
            set slot6_key [lindex $::CRC16_SLOT_TABLE 6]
            assert {[$r0 set $slot6_key "slot6"] == "OK"}
            # Check key in src server
            assert {[$r0 get $slot6_key] == "slot6"}
            set ret [$r0 clusterx migrate 6 $node1_id]
            assert {$ret == "OK"}

            # Migrate slot
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 6*migrating_state: success*" [$r0 cluster info]]
            } else {
                fail "Fail to migrate slot 6"
            }
            # Check key in destination server
            assert {[$r1 get $slot6_key] == "slot6"}
            # Check key in source server
            assert {[string match "*$slot6_key*" [$r0 keys *]] != 0}

            # Change topology by 'setslot'
            $r0 clusterx setslot 6 node $node1_id 2
            # Check key is cleared after 'setslot'
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
            $r0 config set migrate-speed 64
            catch {$r0 config get migrate-speed} e
            assert_match {*64*} $e

            set count 2000
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot15_key_1 $i
            }
            set ret [$r0 clusterx migrate 15 $node1_id]
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
            while 1 {
                if {[string match "*migrating_slot: 15*migrating_state: success*" [$r0 cluster info]]} {
                    break
                }
            }
            while 1 {
                if {[string match "*15*success*" [$r1 cluster info]]} {
                    break
                }
            }

            # Check if the data is consistent
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
            set ret [$r0 clusterx migrate 16 $node1_id]
            assert {$ret == "OK"}

            # Should not finish 1.5s
            after 1500
            catch {[$r0 cluster info]} e
            assert_match {*migrating_slot: 16*start*} $e

            # Check if finish
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 16*success*" [$r0 cluster info]]
            } else {
                fail "Slot 16 importing is not finished"
            }
        }

        test {MIGRATE - Data of migrated slot can't be written to source but can be written to destination} {
            # Construct key
            set slot17_key [lindex $::CRC16_SLOT_TABLE 17]
            $r0 del $slot17_key

            # Write data
            set count 100
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot17_key $i
            }
            # Migrate slot 17 from node0 to node1
            set ret [$r0 clusterx migrate 17 $node1_id]
            assert {$ret == "OK"}
            # Check if finished
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 17*success*" [$r0 cluster info]]
            } else {
                fail "Slot 17 importing is not finished"
            }
            # Check data
            assert {[$r1 llen $slot17_key] == $count}
            # Write the migrated slot to source server
            set slot17_key1 "{$slot17_key}_1"
            catch {$r0 set $slot17_key1 slot17_value1} e
            assert_match {*MOVED*} $e

            # Write the migrated slot to destination server
            assert {[$r1 set $slot17_key1 slot17_value1] == "OK"}
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
            set ret [$r0 clusterx migrate 0 $node1_id]
            assert { $ret == "OK"}

            # Check migration task
            wait_for_condition 50 100 {
                [string match "*0*success*" [$r0 cluster info]]
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
            set slot1_tag [lindex $::CRC16_SLOT_TABLE 1]
            set slot1_key "\x3a\x88{$slot1_tag}\x3d\xaa"
            set count 257
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot1_key "\00\01"
            }
            set ret [$r0 clusterx migrate 1 $node1_id]
            assert {$ret == "OK"}
            set slot1_key_2 "\x49\x1f\x7f{$slot1_tag}\xaf"
            $r0 set $slot1_key_2 "\00\01"
            after 1000
            # Check if finish
            wait_for_condition 50 100 {
                [string match "*1*success*" [$r1 cluster info]]
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
            set ret [$r0 clusterx migrate 2 $node1_id]
            assert { $ret == "OK"}
            wait_for_condition 50 100 {
                [string match "*migrating_slot: 2*migrating_state: success*" [$r0 cluster info]]
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
            set ret [$r0 clusterx migrate 8 $node1_id]
            assert {$ret == "OK"}

            # Check migrating start
            catch {[$r0 cluster info]} e
            assert_match {*migrating_slot: 8*start*} $e

            # Kill destination server itself
            exec kill -9 $node1_pid
            # Wait migration timeout
            after 1000
            # Can't success
            catch {[$r0 cluster info]} e
            assert_match {*migrating_slot: 8*migrating_state: fail*} $e
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
                set ret [$r0 clusterx migrate 10 $node2_id]
                assert {$ret == "OK"}
                catch {[$r0 cluster info]} e
                assert_match {*10*start*} $e
                # Change source server to slave by set topology
                set cluster_nodes "$node0_id 127.0.0.1 $node1_port master - 0-10000"
                set cluster_nodes "$cluster_nodes\n$node0_id 127.0.0.1 $node0_port slave $node1_id"
                set cluster_nodes "$cluster_nodes\n$node2_id 127.0.0.1 $node2_port master - 10001-16383"
                $r0 clusterx SETNODES $cluster_nodes 2
                $r1 clusterx SETNODES $cluster_nodes 2
                $r2 clusterx SETNODES $cluster_nodes 2
                after 1000
                # Check destination importing status
                catch {[$r2 cluster info]} e
                assert_match {*10*error*} $e
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
            set ret [$r0 clusterx migrate 11 $node1_id]
            assert {$ret == "OK"}
            # Ensure migration started
            wait_for_condition 10 50 {
                [string match "*11*start*" [$r0 cluster info]]
            } else {
                fail "Fail to start migrating slot 11"
            }
            # Flush source server
            set ret [$r0 flushdb]
            assert {$ret == "OK"}
            after 1000
            wait_for_condition 10 100 {
                [string match "*11*fail*" [$r0 cluster info]]
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
            set ret [$r0 clusterx migrate 20 $node1_id]
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

start_server {tags {"Source server"} overrides {cluster-enabled yes}} {
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

        set cluster_nodes "$node0_id 127.0.0.1 $node0_port master - 0-16383"
        set cluster_nodes "$cluster_nodes\n$node1_id 127.0.0.1 $node1_port master -"
        $r0 clusterx SETNODES $cluster_nodes 1
        $r1 clusterx SETNODES $cluster_nodes 1

        test {MIGRATE - Migrate slot to newly added node} {
            # Construct key
            set slot21_key [lindex $::CRC16_SLOT_TABLE 21]
            $r0 del $slot21_key

            # Write to newly added node1 will be moved
            catch {$r1 set $slot21_key foobar} ret
            assert_match {*MOVED*} $ret

            # Write data
            set count 100
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot21_key $i
            }
            # Migrate slot 21 from node0 to node1
            set ret [$r0 clusterx migrate 21 $node1_id]
            assert {$ret == "OK"}
            # Check if finished
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 21*success*" [$r0 cluster info]]
            } else {
                fail "Slot 21 importing is not finished"
            }
            # Check data
            assert {[$r1 llen $slot21_key] == $count}

            # Write the migrated slot on source server
            set slot21_key1 "{$slot21_key}_1"
            catch {$r0 set $slot21_key1 slot21_value1} e
            assert_match {*MOVED*} $e

            # Write the migrated slot on destination server
            assert {[$r1 set $slot21_key1 slot21_value1] == "OK"}
        }

        test {MIGRATE - Auth before migrating slot} {
            $r1 config set requirepass password
            set slot22_key [lindex $::CRC16_SLOT_TABLE 22]
            set count 100
            for {set i 0} {$i < $count} {incr i} {
                $r0 lpush $slot22_key $i
            }

            # Migrating slot will fail if no auth
            set ret [$r0 clusterx migrate 22 $node1_id]
            assert {$ret == "OK"}
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 22*fail*" [$r0 cluster info]]
            } else {
                fail "Slot 22 importing is not finished"
            }
            catch {[$r1 exists $slot22_key]} e
            assert_match {*MOVED*} $e

            # Migrating slot will fail if auth with wrong password
            $r0 config set requirepass pass
            set ret [$r0 clusterx migrate 22 $node1_id]
            assert {$ret == "OK"}
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 22*fail*" [$r0 cluster info]]
            } else {
                fail "Slot 22 importing is not finished"
            }
            catch {[$r1 exists $slot22_key]} e
            assert_match {*MOVED*} $e

            # Migrating slot will succeed if auth with right password
            $r0 config set requirepass password
            set ret [$r0 clusterx migrate 22 $node1_id]
            assert {$ret == "OK"}
            wait_for_condition 50 1000 {
                [string match "*migrating_slot: 22*success*" [$r0 cluster info]]
            } else {
                fail "Slot 22 importing is not finished"
            }
            assert {[$r1 exists $slot22_key] == 1}
            assert {[$r1 llen $slot22_key] == $count}
        }
    }
}
