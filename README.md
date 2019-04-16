# kvrocks

Implement SSD kv based on rocksdb with Redis API

## How to install

```shell
$ git clone --recursive https://gitlab.meitu.com/platform/kvrocks.git
$ cd kvrocks
$ mkdir build; cd build
$ cmake ..
$ make -j2
```

## Docs

* [kvrocks support commands](https://gitlab.meitu.com/platform/kvrocks/blob/master/docs/metadata-design.md)
* [replication design](https://gitlab.meitu.com/platform/kvrocks/blob/master/docs/replication-design.md)
* [metadata design](https://gitlab.meitu.com/platform/kvrocks/blob/master/docs/metadata-design.md)

## Tools

[kvrocks2redis]: migrate key value from kvrocks to redis online

## Performance
