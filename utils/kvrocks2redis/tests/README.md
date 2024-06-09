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
