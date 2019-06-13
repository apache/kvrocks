# kvrocks

The kvrocks(kv-rocks) is SSD NoSQL which based on rocksdb, and compatible with the MySQL protocol, intention to decrease the cost of memory and increase the capability.

***Features:***
* Redis API, see the [docs/support-commands](https://gitlab.meitu.com/platform/kvrocks/blob/master/docs/support-commands.md)
* Replication, similar to redis's master-slave
* Compatible with redis-sentinel

## Dependencies

* gcc-g++ (required by c++11, version >= 4.8)
* autoconf (required by jemalloc)

## How to install

```shell
$ git clone --recursive https://gitlab.meitu.com/platform/kvrocks.git
$ cd kvrocks
$ mkdir build; cd build
$ cmake ..
$ make -j2
```

## Docs

* [support commands](https://gitlab.meitu.com/platform/kvrocks/blob/master/docs/support-commands.md)
* [replication design](https://gitlab.meitu.com/platform/kvrocks/blob/master/docs/replication-design.md)
* [metadata design](https://gitlab.meitu.com/platform/kvrocks/blob/master/docs/metadata-design.md)

## Benchmark

## Tools

* kvrocks2redis - migrate key value from kvrocks to redis online

## Performance
