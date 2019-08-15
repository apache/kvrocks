# Replication of rocksdb data

A instance is turned into a slave role when `SLAVEOF` cmd is received. Slave will
try to do a partial synchronization (AKA. incremental replication) if it is viable,
Otherwise, slave will do a full-sync by copying all the rocksdb's latest backup files.
After the full-sync is finished, the slave's DB will be erased and restored using
the backup files downloaded from master, then partial-sync is triggered again.

If everything go OK, the partial-sync is a ongoing procedure that keep receiving
every batch the master gets.

## Replication State Machine

A state machine is used in the slave's replication thread to accommodate the complexity.

On the slave side, replication is composed of the following steps:

  1. Send Auth
  2. Send db\_name to check if the master has the right DB
  3. Try PSYNC, if succeeds, slave is in the loop of receiving batches; if not, go to `4`
  4. Do FULLSYNC
    4.1. send _fetch_meta to get the latest backup meta data
    4.2. send _fetch_file to get all the backup files listed in the meta
    4.3. restore slave's DB using the backup
  5. goto `1`

## Partial Synchronization (PSYNC)

PSYNC takes advantage of the rocksdb's WAL iterator. If the PSYNC's requesting sequence
number is in the range of the WAL files, PSYNC is considered viable.

PSYNC is a command implemented on master role instance. Unlike other commands (eg. GET),
PSYNC cmd is not a REQ-RESP command, but a REQ-RESP-RESP style. That's the response never
ends once the req is accepted.

so PSYNC has two main parts in the code:
- A: libevent callback for sending the batches when the WAL iterator has new data.
- B: timer callback, when A quited because of the exhaustion of the WAL data, timer cb
  will check if WAL has new data available from time to time, so to awake the A again.

## Full Synchronization

On the master side, to support full synchronization, master must create a rocksdb backup
every time the `_fetch_meta` request is received.

On the slave side, after retrieving the meta data, the slave can fetch every file listed in
the meta data (skip if already existed), and restore the backup. to accelerate a bit, file
fetching is executed in parallel.

