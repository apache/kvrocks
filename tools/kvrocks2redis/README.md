# kvrocks2redis

`kvrocks2redis` will first attempt to connect to the kvrocks instance for incremental synchronization.

If the connection or incremental synchronization fails, `kvrocks2redis` will parse the full Kvrocks data from the configured data directory to the AOF file.

If incremental synchronization is possible, `kvrocks2redis` parses the incremental data to the AOF file.
Another thread (named `redis-writer`) reads the AOF continuously and sends the contents of the AOF to Redis.

When the program runs, the following files are generated:
1. xxx_appendonly.aof: parsed data will be saved in this file.
2. xxx_last_next_offset.txt: indicates the position of the AOF file read by the `redis-writer` thread.
3. last_next_seq.txt: indicates the sequence number parsed by `kvrocks2redis` to record the synchronization location and check whether incremental synchronization can be performed.