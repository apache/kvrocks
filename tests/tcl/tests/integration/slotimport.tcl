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

start_server {tags {"Imported slave server"} overrides {cluster-enabled yes}} {
    # M is master, S is slave
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

        set cluster_nodes "$node0_id 127.0.0.1 $node0_port master - 0-100"
        set cluster_nodes "$cluster_nodes\n$node1_id 127.0.0.1 $node1_port slave $node0_id"
        $r0 clusterx SETNODES $cluster_nodes 1
        $r1 clusterx SETNODES $cluster_nodes 1

        test {IMPORT - Can't import slot to slave} {
            after 100
            catch {$r1 cluster import 1 0} e
            assert_match {*Slave can't import slot*} $e
        }
    }
}

start_server {tags {"Imported server"} overrides {cluster-enabled yes}} {
    set r0 [srv 0 client]
    set node0_host [srv 0 host]
    set node0_port [srv 0 port]
    set node0_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
    set cluster_nodes "$node0_id 127.0.0.1 $node0_port master - 0-16383"
    $r0 clusterx SETNODEID $node0_id
    $r0 clusterx SETNODES $cluster_nodes 1

    test {IMPORT - error slot} {
        catch {$r0 cluster import -1 0} e
        assert_match {*Slot is out of range*} $e

        catch {$r0 cluster import 16384 0} e
        assert_match {*Slot is out of range*} $e
    }

    test {IMPORT - slot with error state} {
        catch {$r0 cluster import 1 4} e
        assert_match {*Invalid import state*} $e
        catch {$r0 cluster import 1 -3} e
        assert_match {*Invalid import state*} $e
    }

    test {IMPORT - slot states in right order} {
        set slot_key1 [lindex $::CRC16_SLOT_TABLE 1]
        # Import start
        assert {[$r0 cluster import 1 0] == "OK"}
        $r0 set $slot_key1 slot1
        assert {[$r0 get $slot_key1] == {slot1}}
        catch {$r0 cluster info} e
        assert_match {*importing_slot: 1*import_state: start*} $e
        # Import success
        assert {[$r0 cluster import 1 1] == "OK"}
        catch {$r0 cluster info} e
        assert_match {*importing_slot: 1*import_state: success*} $e
        after 50
        assert {[$r0 get $slot_key1] == {slot1}}
    }

    test {IMPORT - slot state 'error'} {
        set slot_key10 [lindex $::CRC16_SLOT_TABLE 10]
        assert {[$r0 cluster import 10 0] == "OK"}
        $r0 set $slot_key10 slot10_again
        assert {[$r0 get $slot_key10] == {slot10_again}}
        # Import error
        assert {[$r0 cluster import 10 2] == "OK"}
        after 50
        catch {$r0 cluster info} e
        assert_match {*importing_slot: 10*import_state: error*} $e
        # Get empty
        assert {[$r0 exists $slot_key10] == 0}
    }

    test {IMPORT - connection broken} {
        set slot_key11 [lindex $::CRC16_SLOT_TABLE 11]
        assert {[$r0 cluster import 11 0] == "OK"}
        $r0 set $slot_key11 slot11
        assert {[$r0 get $slot_key11] == {slot11}}
        # Close connection, server will stop importing
        reconnect
        after 50
        # Reassign 'r0' for old 'r0' is released
        set r0 [srv 0 client]
        catch {$r0 cluster info} e
        assert_match {*11*error*} $e
        # Get empty
        assert {[$r0 exists $slot_key11] == 0}
    }
}