Redis 里面通过多个 DB 来实现数据的隔离，业务通过 `select` 指令来选择读写的 DB。 这种方式存在一些问题:

* 多租户实现同一个认证密码，虽然数据是隔离的，但权限没法隔离
* DB 数量固定且编号无法标识业务，固定数量的 DB 并不是太大的问题(16 个也足够)，但 DB 编号无法标识业务在我们看来其实是不太方便的


## kvrocks 的实现

kvrocks 允许通过一个实例在线增加或者减少 token, 每个 token 之间的数据是隔离的。 实现上也比较简单，在分配 token 的时候关联一个业务标识(Namespace), 在业务写入数据的时候会自动在 key 前面增加这个标识，读取数据 key 的时候自动去掉标识。

下面是在线管理 token 的命令，也可以在配置文件里面指定:

```shell
namespace add ${namespace} ${token} # 新增一个 token, token 和 namespace 不能是已经存在

namespace set ${namespace} ${token} # 修改 namespace 对应的 token

namespace del ${namespace} # 删除 namespace 对应的 token， 注意这里不会删除数据

namespace get ${namespace} # 获取 namespace 对应的 token

namespace get * # 获取所有 namespace 和 token
 
```

> 注意: 如果修改了 namespace 之后需要执行 `confing rewrite` 命令将配置持久化到本地，同时如果有多个实例也需要同步修改


假设我们配置了一个 namespace N1 对应 token T1, 当业务使用这个 token 执行 `SET a 1`, 那么在存储里面实际上会变成类似 `T1|a => 1` ，读取数据 key 的时候会自动去掉 T1。

这种实现方式有一个问题就是每个 key 都增加一个 namespace 信息会额外占用一些空间。另外一种方式是通过 rocksdb 的 column family 来实现数据隔离，但从实现的复杂度和必要性层面我们还是选择了前者
