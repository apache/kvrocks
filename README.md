# kvrocks

kvrocks is an open-source key-value database. which is based on rocksdb and compatible with Redis protocol.  To replace the function of Redis in some scenes, intention to decrease the cost of memory and increase the capability. The design of replication and storage was inspired by `rocksplicator` and  `blackwisdow`.

kvrocks has the following key features:

-  Redis protocol, user can use redis client to visit kvrocks
-  Namespace,  similar to redis db but use token per namespace
-  Replication,   async replicate the binlog like MySQL
- High Available, supports redis sentinel to failover when master or slave was failed

## Build

#### requirements

* gcc-g++ (required by c++11, version >= 4.8)
* autoconf (required by jemalloc)

```shell
$ git clone --recursive https://github.com/meitu/kvrocks.git
$ cd kvrocks
$ make -j4
```

## Try kvrocks using Docker

```
$ docker run -it hulkdev/kvrocks
$ redis-cli -a foobared -p 6666

127.0.0.1:6666> get a
(nil)
```

##  Namspace

namespace was used to isolate data between users. unlike the redis databases can be visited by `requirepass`, we use one token per namespace and `requirepass` was regraded as admin token. only admin token allows to add or del the namespace, as well as some commands like `config`, `slaveof`, `bgave`, etc… 

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

## License

kvrocks is under the MIT license. See the LICENSE file for details.