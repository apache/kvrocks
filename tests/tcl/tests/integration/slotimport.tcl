# Import crc16 table
source "tests/helpers/crc16_slottable.tcl"

start_server {tags {"Imported slave server"} overrides {cluster-enabled yes}} {
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
        # Cannot import data to slave server
        test {IMPORT - slot to slave} {
            $S slaveof $M_host $M_port
            after 500
            catch {$S cluster import 1 0} e
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
        catch {$r0 cluster importstatus 1} e
        assert_match {*import_slot: 1*import_state: START*} $e
        # Import success
        assert {[$r0 cluster import 1 1] == "OK"}
        catch {$r0 cluster importstatus 1} e
        assert_match {*import_slot: 1*import_state: SUCCESS*} $e
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
        catch {$r0 cluster importstatus 10} e
        assert_match {*import_slot: 10*import_state: ERROR*} $e
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
        catch {$r0 cluster importstatus 11} e
        assert_match {*11*ERROR*} $e
        # Get empty
        assert {[$r0 exists $slot_key11] == 0}
    }
}