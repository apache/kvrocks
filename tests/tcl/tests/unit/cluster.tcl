start_server {
    tags {"cluster"}
} {
    source "tests/helpers/crc16_slottable.tcl"

    test {CLUSTER KEYSLOT} {

        set slot_table_len [llength $::CRC16_SLOT_TABLE]

        for {set i 0} {$i < $slot_table_len} {incr i} {
            assert_equal $i [r cluster keyslot [lindex $::CRC16_SLOT_TABLE $i]]
        }
    }
}