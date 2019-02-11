# Metadata Design

## Column Family

The data in kvrocks was split into three column families:

1. metadata - store the metadata's kv of hash/set/list/zset, string type also in this column family
2. subkey - store the subkey of hash/set/list/zset, e.g. the field and value in hash would store in this column family
3. score family - only for zset, store the zset score member

What's column family see also: [rocksdb column families wiki](https://github.com/facebook/rocksdb/wiki/Column-Families)

## Metadata

### 1) string

```shell
NS|key => flags(1byte)|expire(4byte)|value(bytes)
```

### 2) Hash

```shell
# metadata
NS|key => flags(1byte)|expire(4byte)|version(8bytes)|size(4byte)|
# field-value
NS|key|version|field| => value(bytes)
```

### 3) Set

```shell
# meta
NS|key => flags(1byte)|expire(4byte)|version(8bytes)|size(4byte)|
# member
NS|key|version|member| => NULL
```

### 4) List

```shell
# meta
NS|key => flags(1byte)|expire(4byte)|version(8bytes)|size(4byte)|head(8byte)|tail(8byte)
# element
NS|key|version|index(8byte)| => value(bytes)
```

### 5) ZSet

```shell
# meta
NS|key => flags(1byte)|expire(4byte)|version(8bytes)|size(4byte)
# member
NS|key|version|member => score
NS|key|version|score|member => NULL 
```