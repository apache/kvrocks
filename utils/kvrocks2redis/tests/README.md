## Populate data and append new data script

For testing the `kvrocks2redis` utility, manually check generate AOF.

## Usage

* Start `kvrocks` and `kvrocks2redis`
    * [ ] TODO automatic create docker env
* Install dependency::
    * pip install redis==4.3.6
* Usage:

```bash
# populate data
python3 populate-kvrocks.py [--host HOST] [--port PORT] [--password PASSWORD] [--flushdb FLUSHDB]
# check generated aof file & user_key.log file

# check consistency
python3 check_consistency.py [--src_host SRC_HOST] [--src_port SRC_PORT] [--src_password SRC_PASSWORD]
                          [--dst_host DST_HOST] [--dst_port DST_PORT] [--dst_password DST_PASSWORD]
                          [--key_file KEY_FILE]
```

On an incremental comparison failure, the comparator stops writing and observes
synchronization for up to 10 seconds before raising the original failure. Recovery
during this diagnostic window does not make the test pass. A separate process
bounds the observation, including unavailable endpoints or stalled file reads;
process cleanup can take up to one additional second.

Snapshots include the source replication sequence, synchronization checkpoint,
AOF size, sender offset, and whether the original source value reaches the target.
The AOF lookup scans only its first MiB and reports a matching RESP bulk string and
surrounding bytes; this is diagnostic context, not proof of successful execution.
The sender offset alone is also not an acknowledgement of every command.

By default, diagnostic files are read from the current directory with namespace
`__namespace`, matching CI. For a different utility configuration, pass
`--diagnostics-output-dir PATH` and `--diagnostics-namespace NAME`. Missing files
are reported without replacing the original assertion. Diagnostic clients use
200 ms connect/read timeouts with retries disabled; normal comparison clients and
the original retry budget are unchanged.

Run the diagnostic regression tests with the same Python Redis dependency:

```bash
cd utils/kvrocks2redis/tests
python3 -m unittest -v test_check_consistency
```
