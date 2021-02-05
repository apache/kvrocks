<img src="docs/images/kvrocks_logo.png" alt="kvrocks_logo" width="200" height="200"/>

# kvrocks  ![image](https://github.com/bitleak/kvrocks/workflows/kvrocks%20ci%20actions/badge.svg) [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://github.com/bitleak/kvrocks/blob/master/LICENSE)

- [Google Group](https://groups.google.com/g/kvrocks)
- [Slack Channel](https://join.slack.com/t/kvrockscommunity/shared_invite/zt-lnuch2bb-h5Dyv0d4MIQ_Q3b1pYP4HQ)

kvrocks is an open-source key-value database. which is based on rocksdb and compatible with Redis protocol.  Intention to decrease the cost of memory and increase the capability while compared to Redis. The design of replication and storage was inspired by `rocksplicator` and `blackwidow`.

kvrocks has the following key features:

- Redis protocol, user can use redis client to visit the kvrocks
- Namespace, similar to redis db but use token per namespace
- Replication, async replication using binlog like MySQL
- High Available, supports redis sentinel to failover when master or slave was failed
- Codis Protocol, the user can use the codis proxy and dashboard to manage the kvrocks

> Thanks for @smartlee and the trip.com designer(@范世丽) contributes the kvrocks logo for us

## Who uses kvrocks 

<table>
<tr>
<td height = "128" width = "164"><img src="https://imgur.com/9X1kc2j.png" alt="Meitu"></td>
<td height = "128" width = "164"><img src="https://imgur.com/vqgSmMz.jpeg" alt="Ctrip"></td>
<td height = "128" width = "164"><img src="https://imgur.com/MJsoEN7.png" alt="BaishanCloud"></td>
</tr>
</table>

***Tickets a pull reqeust to let us known that you're using kvrocks and add your logo to README***

## Build and run

#### requirements

* g++ (required by c++11, version >= 4.8)
* autoconf automake libtool

#### Build

***NOTE: You shoud install the snappy first:***

```shell
# Centos/Redhat
sudo yum install -y epel-release && sudo yum install -y git gcc gcc-c++ make snappy snappy-devel autoconf automake libtool which gtest gtest-devel

# Ubuntu
sudo apt-get install gcc g++ make libsnappy-dev autoconf automake libtool which libgtest-dev

# MACOSX
brew install snappy googletest
```

```shell
$ git clone --recursive https://github.com/bitleak/kvrocks.git
$ cd kvrocks
$ make -j4
```

#### run

```shell
$ ./src/kvrocks -c kvrocks.conf
```

### TEST

***NOTE: You shoud install the googletest first***

```shell
make test
```

### Supported platforms

* centos 6/7
* ubuntu
* macosx

## Try kvrocks using Docker

```
$ docker run -it -p 6666:6666 bitleak/kvrocks
$ redis-cli -p 6666

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

## DOCs

* [supported commands](https://github.com/bitleak/kvrocks/blob/master/docs/support-commands.md)
* [design complex kv on rocksdb](https://github.com/bitleak/kvrocks/blob/master/docs/metadata-design.md)
* [replication design](https://github.com/bitleak/kvrocks/blob/master/docs/replication-design.md)

## Migrate Tools

* migrate from redis to kvrocks, use [redis-migrate-tool](https://github.com/vipshop/redis-migrate-tool) which developed by vipshop
* migrate from kvrocks to redis. use `kvrocks2redis` in build dir

## Performance

#### Hardware

* CPU: 48 cores Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz
* Memory: 32 GiB
* NET:  Intel Corporation I350 Gigabit Network Connection
* DISK: 2TB NVMe Intel SSD DC P460

>  Benchmark Client:  multi-thread redis-benchmark(unstable branch)

 #### 1. Commands QPS

> kvorkcs: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/bitleak/kvrocks/master/docs/images/chart-commands.png)

#### 2.  QPS on different payload

> kvorkcs: workers = 16, benchmark: 8 threads/ 512 conns

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/bitleak/kvrocks/master/docs/images/chart-values.png)

#### 3. QPS on different workers

> kvorkcs: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/bitleak/kvrocks/master/docs/images/chart-threads.png)

## License

kvrocks is under the BSD-3-Clause license. See the LICENSE file for details.
