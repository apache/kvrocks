<img src="docs/images/kvrocks_logo.png" alt="kvrocks_logo" width="350"/>

# ![image](https://github.com/apache/incubator-kvrocks/workflows/kvrocks%20ci%20actions/badge.svg) ![image](https://img.shields.io/badge/build-passing-brightgreen) [![GitHub license](https://img.shields.io/github/license/apache/incubator-kvrocks)](https://github.com/apache/incubator-kvrocks/blob/unstable/LICENSE) [![GitHub stars](https://img.shields.io/github/stars/apache/incubator-kvrocks)](https://github.com/apache/incubator-kvrocks/stargazers)

- [Google Group](https://groups.google.com/g/kvrocks)
- [Slack Channel](https://join.slack.com/t/kvrockscommunity/shared_invite/zt-p5928e3r-OUAK8SUgC8GOceGM6dAz6w)

Kvrocks is a distributed key value NoSQL database that uses RocksDB as storage engine and is compatible with Redis protocol. Kvrocks intends to decrease the cost of memory and increase the capability while compared to Redis. The design of replication and storage was inspired by `rocksplicator` and `blackwidow`.

Kvrocks has the following key features:

- Redis protocol, user can use redis client to visit the kvrocks
- Namespace, similar to redis db but use token per namespace
- Replication, async replication using binlog like MySQL
- High Available, supports redis sentinel to failover when master or slave was failed
- Cluster mode, centralized management but compatible with Redis cluster client access

> Thanks for designers @[田凌宇](https://github.com/tianlingyu1997) and @范世丽 contribute the kvrocks logo for us.

## Who uses kvrocks 

<table>
<tr>
<td height = "128" width = "164"><img src="https://imgur.com/9X1kc2j.png" alt="Meitu"></td>
<td height = "128" width = "164"><img src="https://imgur.com/vqgSmMz.jpeg" alt="Ctrip"></td>
<td height = "128" width = "164"><img src="docs/images/jiatou_logo.png" alt="JiaTou"></td>
</tr>
<tr>
<td height = "128" width = "164"><img src="docs/images/baidu_logo.png" alt="Baidu"></td>
<td height = "128" width = "164"><img src="https://imgur.com/MJsoEN7.png" alt="BaishanCloud"></td>
<td height = "128" width = "164"><img src="docs/images/rgyun_logo.png" alt="Rgyun"></td>
</tr>
<tr>
<td height = "128" width = "164"><img src="docs/images/xueqiu_logo.png" alt="Xueqiu"></td>
<td height = "128" width = "164"><img src="docs/images/U-NEXT_logo.png" alt="U-NEXT"></td>
<td height = "128" width = "164"><img src="docs/images/circl-lu.png" alt="circl.lu"></td>
</tr>
</table>

***Tickets a pull reqeust to let us known that you're using kvrocks and add your logo to README***

## Building kvrocks

#### requirements
* g++ (required by c++11, version >= 4.8)
* autoconf automake libtool cmake

#### Build

```shell
# Centos/Redhat
sudo yum install -y epel-release && sudo yum install -y git gcc gcc-c++ make cmake autoconf automake libtool which

# Ubuntu/Debian
sudo apt update
sudo apt-get install gcc g++ make cmake autoconf automake libtool

# MACOSX
brew install autoconf automake libtool cmake
```

It is as simple as:

```shell
$ git clone https://github.com/apache/incubator-kvrocks.git
$ cd kvrocks
$ mkdir build
$ ./build.sh build # manually run CMake if you want to build Debug version or add some build options
```

### Running kvrocks

```shell
$ ./build/kvrocks -c kvrocks.conf
```

### Running test cases

```shell
$ # make sure CMake was executed in ./build
$ cd build
$ make unittest
$ ./unittest
```

### Supported platforms

* centos 6/7
* ubuntu
* macosx

## Try kvrocks using Docker

```
$ docker run -it -p 6666:6666 kvrocks/kvrocks
$ redis-cli -p 6666

127.0.0.1:6666> get a
(nil)
```

##  Namespace

namespace was used to isolate data between users. unlike all the redis databases can be visited by `requirepass`, we use one token per namespace. `requirepass` was regraded as admin token, only admin token allows to access the namespace command, as well as some commands like `config`, `slaveof`, `bgsave`, etc…

```
# add token
127.0.0.1:6666>  namespace add ns1 my_token
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

## Cluster

Kvrocks implements a proxyless centralized cluster solution but its accessing method is completely compatible with the Redis cluster client. You can use Redis cluster SDKs to access the kvrocks cluster. More details, please see: [Kvrocks Cluster Introduction](https://github.com/apache/incubator-kvrocks/wiki/Kvrocks-Cluster-Introduction)

## DOCs

* [supported commands](https://github.com/apache/incubator-kvrocks/wiki/Support-Commands)
* [design complex kv on rocksdb](https://github.com/apache/incubator-kvrocks/blob/master/docs/metadata-design.md)
* [replication design](https://github.com/apache/incubator-kvrocks/blob/master/docs/replication-design.md)

For more informations, can see: [Kvrocks Wiki](https://github.com/apache/incubator-kvrocks/wiki)

## Tools
* Export the Kvrocks monitor metrics, please use [kvrocks_exporter](https://github.com/KvrocksLabs/kvrocks_exporter)
* Migrate from redis to kvrocks, use [redis-migrate-tool](https://github.com/vipshop/redis-migrate-tool) which was developed by vipshop
* Migrate from kvrocks to redis. use `kvrocks2redis` in build dir

## Performance

#### Hardware

* CPU: 48 cores Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz
* Memory: 32 GiB
* NET:  Intel Corporation I350 Gigabit Network Connection
* DISK: 2TB NVMe Intel SSD DC P4600

>  Benchmark Client:  multi-thread redis-benchmark(unstable branch)

 #### 1. Commands QPS

> kvrocks: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/apache/incubator-kvrocks/master/docs/images/chart-commands.png)

#### 2.  QPS on different payloads

> kvrocks: workers = 16, benchmark: 8 threads/ 512 conns

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/apache/incubator-kvrocks/master/docs/images/chart-values.png)

#### 3. QPS on different workers

> kvrocks: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/apache/incubator-kvrocks/master/docs/images/chart-threads.png)

## License

Kvrocks is under the Apache License Version 2.0. See the LICENSE file for details.

## 微信公众号

<img src="docs/images/wechat_account.jpg" alt="微信公众号" />
