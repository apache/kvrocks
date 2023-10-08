# redis2kvrocks

`redis2kvrocks`  is used to load an RDB file into an empty kvrocks database. Therefore, you must ensure that the database directory does not contain any pre-existing databases.

## Usage

```bash
    ./build/redis2kvrocks -c kvrocks.conf -r /path/dump.rdb -ns namespace
```

The default namespace to load is '__namespace'. If the RDB file contains multiple databases, the namespace will be set to `<ns>_<n>`, where `n` is the database number ranging from 1 to `n`. The password will be the same as the namespace. This setup requires the `requirepass` setting in the kvrocks configuration file and will rewrite the config file with new namespaces. Database 0 will use the namespace `<ns>`.

Current support rdb version is `10`, not support:
1. load stream type;
2. load module type;
