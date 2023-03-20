<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

<img src="https://kvrocks.apache.org/img/kvrocks-featured.png" alt="kvrocks_logo" width="350"/>

[![kvrocks ci actions](https://github.com/apache/incubator-kvrocks/actions/workflows/kvrocks.yaml/badge.svg)](https://github.com/apache/incubator-kvrocks/actions/workflows/kvrocks.yaml)
[![GitHub license](https://img.shields.io/github/license/apache/incubator-kvrocks)](https://github.com/apache/incubator-kvrocks/blob/unstable/LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/apache/incubator-kvrocks)](https://github.com/apache/incubator-kvrocks/stargazers)

---

* [Slack Channel](https://join.slack.com/t/kvrockscommunity/shared_invite/zt-p5928e3r-OUAK8SUgC8GOceGM6dAz6w)
* [Mailing List](https://lists.apache.org/list.html?dev@kvrocks.apache.org) ([how to subscribe](https://www.apache.org/foundation/mailinglists.html#subscribing))

**Apache Kvrocks(Incubating)** is a distributed key value NoSQL database that uses RocksDB as storage engine and is compatible with Redis protocol. Kvrocks intends to decrease the cost of memory and increase the capacity while compared to Redis. The design of replication and storage was inspired by [rocksplicator](https://github.com/pinterest/rocksplicator) and [blackwidow](https://github.com/Qihoo360/blackwidow).

Kvrocks has the following key features:

* Redis Compatible: Users can access Apache Kvrocks via any Redis client.
* Namespace: Similar to Redis SELECT but equipped with token per namespace.
* Replication: Async replication using binlog like MySQL.
* High Available: Support Redis sentinel to failover when master or slave was failed.
* Cluster: Centralized management but accessible via any Redis cluster client.

Thanks to designers [Lingyu Tian](https://github.com/tianlingyu1997) and Shili Fan for contributing the logo of Kvrocks.

## Who uses Kvrocks

You can find Kvrocks users at [the Users page](https://kvrocks.apache.org/users/).

Users are encouraged to add themselves to the Users page. Either leave a comment on the ["Who is using Kvrocks"](https://github.com/apache/incubator-kvrocks/issues/414) issue, or directly send a pull request to add company or organization [information](https://github.com/apache/incubator-kvrocks-website/blob/main/src/components/UserLogos/index.tsx) and [logo](https://github.com/apache/incubator-kvrocks-website/tree/main/static/media/users).

## Build and run Kvrocks

### Prerequisite

```shell
# CentOS / RedHat
sudo yum install -y epel-release
sudo yum install -y git gcc gcc-c++ make cmake autoconf automake libtool libstdc++-static python3 which openssl-devel

# Ubuntu / Debian
sudo apt update
sudo apt install -y git gcc g++ make cmake autoconf automake libtool python3 libssl-dev

# macOS
brew install autoconf automake libtool cmake openssl

# Please force linking the openssl if still can't find after installing openssl
brew link --force openssl
```

### Build

It is as simple as:

```shell
$ git clone https://github.com/apache/incubator-kvrocks.git
$ cd incubator-kvrocks
$ ./x.py build # `./x.py build -h` to check more options;
               # especially, `./x.py build --ghproxy` will fetch dependencies via ghproxy.com.
```

To build with TLS support, you'll need OpenSSL development libraries (e.g. libssl-dev on Debian/Ubuntu) and run:

```shell
$ ./x.py build -DENABLE_OPENSSL=ON
```

To build with lua instead of luaJIT, run:

```shell
$ ./x.py build -DUSE_LUAJIT=OFF
```

### Running Kvrocks

```shell
$ ./build/kvrocks -c kvrocks.conf
```

### Running Kvrocks using Docker

```shell
$ docker run -it -p 6666:6666 apache/kvrocks
# or get the nightly image:
$ docker run -it -p 6666:6666 apache/kvrocks:nightly
```

### Connect Kvrocks service

```
$ redis-cli -p 6666

127.0.0.1:6666> get a
(nil)
```

### Running test cases

```shell
$ ./x.py build --unittest
$ ./x.py test cpp # run C++ unit tests
$ ./x.py test go # run Golang (unit and integration) test cases
```

### Supported platforms

* Linux
* macOS

## Namespace

Namespace is used to isolate data between users. Unlike all the Redis databases can be visited by `requirepass`, we use one token per namespace. `requirepass` is regraded as admin token, and only admin token allows to access the namespace command, as well as some commands like `config`, `slaveof`, `bgsave`, etc. See the [Namespace](https://kvrocks.apache.org/docs/namespace) page for more details.

```
# add token
127.0.0.1:6666> namespace add ns1 my_token
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

Kvrocks implements a proxyless centralized cluster solution but its accessing method is completely compatible with the Redis cluster client. You can use Redis cluster SDKs to access the kvrocks cluster. More details, please see: [Kvrocks Cluster Introduction](https://kvrocks.apache.org/docs/cluster/)

## Documents

Documents are hosted at the [official website](https://kvrocks.apache.org/docs/getting-started/).

* [Supported Commands](https://kvrocks.apache.org/docs/supported-commands/)
* [Design Complex Structure on RocksDB](https://kvrocks.apache.org/community/data-structure-on-rocksdb/)
* [Replication Design](https://kvrocks.apache.org/docs/replication)

## Tools

* Export the Kvrocks monitor metrics, please use [kvrocks_exporter](https://github.com/KvrocksLabs/kvrocks_exporter)
* Migrate from redis to kvrocks, use [redis-migrate-tool](https://github.com/vipshop/redis-migrate-tool) which was developed by [vipshop](https://github.com/vipshop)
* Migrate from kvrocks to redis, use `kvrocks2redis` in the build directory

## Contributing

Kvrocks community welcomes all forms of contribution and you can find out how to get involved on the [Community](https://kvrocks.apache.org/community/) and [How to Contribute](https://kvrocks.apache.org/community/contributing) pages.

## Performance

### Hardware

* CPU: 48 cores Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz
* Memory: 32 GiB
* NET:  Intel Corporation I350 Gigabit Network Connection
* DISK: 2TB NVMe Intel SSD DC P4600

> Benchmark Client: multi-thread redis-benchmark(unstable branch)

### 1. Commands QPS

> kvrocks: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](assets/chart-commands.png)

### 2. QPS on different payloads

> kvrocks: workers = 16, benchmark: 8 threads/ 512 conns

latency: 99.9% < 10ms

![image](assets/chart-values.png)

### 3. QPS on different workers

> kvrocks: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](assets/chart-threads.png)

## License

Kvrocks is under the Apache License Version 2.0. See the [LICENSE](LICENSE) file for details.

## Social Media

- [Medium](https://kvrocks.medium.com/)
- [Zhihu](https://www.zhihu.com/people/kvrocks) (in Chinese)
- WeChat Official Account (in Chinese, scan the QR code to follow)

![WeChat official account](assets/wechat_account.jpg)
