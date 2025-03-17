#!/usr/bin/env tclsh

# Basic test helper for Kvrocks
set ::test_server "127.0.0.1"
set ::test_port 6666
set ::kvrocks_path "../src/kvrocks"
set ::kvrocks_conf "../kvrocks.conf"

proc start_kvrocks {} {
    global test_server test_port kvrocks_path kvrocks_conf
    puts "Starting Kvrocks on $test_server:$test_port..."
    if {[catch {exec $kvrocks_path -c $kvrocks_conf &} result]} {
        puts "Error starting Kvrocks: $result"
        exit 1
    }
    after 2000  ;# Wait for Kvrocks to start
}

proc stop_kvrocks {} {
    puts "Stopping Kvrocks..."
    if {[catch {exec pkill -f kvrocks} result]} {
        puts "Error stopping Kvrocks: $result"
        exit 1
    }
    after 1000
}

proc run_test {test_script} {
    start_kvrocks
    puts "Running test: $test_script"
    if {[catch {source $test_script} result]} {
        puts "Error running test script: $result"
        stop_kvrocks
        exit 1
    }
    stop_kvrocks
}

if {[llength $argv] == 2 && [lindex $argv 0] eq "--single"} {
    run_test [lindex $argv 1]
} else {
    puts "Usage: ./test_helper.tcl --single tests/<your_test_script>.tcl"
}
