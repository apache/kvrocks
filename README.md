# kvrocks  ![image](https://travis-ci.org/meitu/kvrocks.svg?branch=master)

kvrocks is an open-source key-value database. which is based on rocksdb and compatible with Redis protocol.  Intention to decrease the cost of memory and increase the capability while compared to Redis. The design of replication and storage was inspired by `rocksplicator` and `blackwidow`.

kvrocks has the following key features:

- Redis protocol, user can use redis client to visit the kvrocks
- Namespace, similar to redis db but use token per namespace
- Replication, async replication using binlog like MySQL
- High Available, supports redis sentinel to failover when master or slave was failed

## Build and run

#### requirements

* gcc-g++ (required by c++11, version >= 4.8)
* autoconf (required by jemalloc)

```shell
$ git clone --recursive https://github.com/meitu/kvrocks.git
$ cd kvrocks
$ make -j4
```

#### run

```
$ mkdir /data # default data dir is /data/kvrocks
$ ./build/kvrocks -c kvrocks.conf
```

### Supported platforms

* centos 6/7
* ubuntu
* macosx

## Try kvrocks using Docker

```
$ docker run -it hulkdev/kvrocks
$ redis-cli -a foobared -p 6666

127.0.0.1:6666> get a
(nil)
```

##  Namspace

namespace was used to isolate data between users. unlike all the redis databases can be visited by `requirepass`, we use one token per namespace. `requirepass` was regraded as admin token, only admin token allows to access the namespace command, as well as some commands like `config`, `slaveof`, `bgave`, etc… 

```
# add token
127.0.0.1:6666>  namespace add ns1 mytoken
OK

# update token
127.0.0.1:6666> namespace set ns1 new_token
OK

# list namespace
127.0.0.1:6666> namespace get *
1) "ns1"
2) "new_token"
3) "__namespace"
4) "foobared"

# delete namespace
127.0.0.1:6666> namespace del ns1
OK
```

## Performance

#### Hardware

* CPU: 48 cores Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz
* Memory: 32 GiB
* NET:  Intel Corporation I350 Gigabit Network Connection
* DISK: 2TB NVMe SSD

>  Benchmark Client:  multi-thread redis-benchmark(unstable branch)

 #### 1. Commands QPS

> kvorkcs: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/meitu/kvrocks/master/docs/images/chart-commands.png)

#### 2.  QPS on different payload

> kvorkcs: workers = 16, benchmark: 8 threads/ 512 conns

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/meitu/kvrocks/master/docs/images/chart-values.png)

#### 3. QPS on different workers

> kvorkcs: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/meitu/kvrocks/master/docs/images/chart-threads.png)

## License

kvrocks is under the MIT license. See the LICENSE file for details.
