# Design Complex Structure On Rocksdb

kvrocks use the rocksdb as storage, it's developed by facebook which built on LevelDB with many extra features supports, like column family, transaction, backup, see the rocksdb wiki: [Features Not In LevelDB](https://github.com/facebook/rocksdb/wiki/Features-Not-in-LevelDB). the basic operations in rocksdb are `Put(key, value)`, `Get(key)`, `Delete(key)`, other complex structures weren't supported. the main goal of this doc would explain how we built the Redis hash/list/set/zset/bitmap on rocksdb. most of the design was derived from Qihoo360 `Blackwidow`, but with little modified, like the bitmap design, it's a really interesting part.

## String

Redis string is key-value with expire time, so it's very easy to translate the Redis string into rocksdb key-value. 

```shell
        +----------+------------+--------------------+
key =>  |  flags   |  expire    |       payload      |
        | (1byte)  | (4byte)    |       (Nbyte)      |
        +----------+------------+--------------------+
```

we prepend 1-byte `flags` and 4-bytes expire before the user's value:

-  `flags`  is used to tell the kvrocks which type of this key-value,  maybe `string`/`hash`/`list`/`zset`/`bitmap`
- `expire` is store the absolute time of key should be expired, if zero means the key-value would never be expired
- `payload` is the user's raw value

## Hash

Redis hashmap(dict) was like the hashmap in many languages, it was used to implement an associative array abstract data type, a structure that can map keys to values.  the direct way to implement the hash in rocksdb is serialized the keys/values into one value and store it like the string, but the drawback is performance impact when the keys/values grew bigger. so we split the hash sub keys/values into single key-value in rocksdb, track it with metadata.

#### hash metadata

```shell
        +----------+------------+-----------+-----------+
key =>  |  flags   |  expire    |  version  |  size     |
        | (1byte)  | (4byte)    |  (8byte)  | (8byte)   |
        +----------+------------+-----------+-----------+
```

the value of key we call it metadata here, it stored the metadata of hash key includes:

- `flags` like the string, the field was used to tell which type of this key
- `expire ` is same as the string type, record the expire time
- `version`  is used to accomplish fast delete when the number of sub keys/values grew bigger
- `size` records the number sub of keys/values in this hash key

#### hash sub keys-values

we use extra keys-values to store the hash keys-values,  the format like this:

```shell
                     +---------------+
key|version|field => |     value     |
                     +---------------+
```

we prepend the hash `key` and `version` before the hash field, the value of  `version`  was from the metadata.  for example, when the request `hget h1 f1` was received, kvrocks would fetch the metadata by hash key(here is `h1`) first, and concat the hash key, version, field as new key, then fetch the value with new key.



***Question1:  why store version in the metadata***

> we store the hash keys/values into single key-value, if the store millions of sub keys-values in one hash key . if user delete this key, the kvrocks must iterator millions of sub keys-values and delete, and it would cause performance problem.  with version we can fast delete the metadata and then recycle the others keys-values in compaction background threads. the cost is those tombstone key would take some disk stroage. you can regard the version as atomic increment number, but it's combined with timestamp.



***Question2:  what can we do if the user key was conflicted with the composed key?***

> we store the metadata key and composed key in different  column families, so it wouldn't happend

## Set

Redis set can be regarded as a hash, with the value of sub-key always be null, the metadata was same with the hash:

```shell
        +----------+------------+-----------+-----------+
key =>  |  flags   |  expire    |  version  |  size     |
        | (1byte)  | (4byte)    |  (8byte)  | (8byte)   |
        +----------+------------+-----------+-----------+
```

and the sub keys-values in rocksdb would be:

```shell
                      +---------------+
key|version|member => |     NULL      |
                      +---------------+
```

## List

#### list metadata

Redis list also organized by metadata and sub keys-values, and sub key is index instead of the user key.  metadata like below:

```shell
        +----------+------------+-----------+-----------+-----------+-----------+
key =>  |  flags   |  expire    |  version  |  size     |  head     |  tail     |
        | (1byte)  | (4byte)    |  (8byte)  | (8byte)   | (8byte)   | (8byte)   |
        +----------+------------+-----------+-----------+-----------+-----------+         
```

- `head` was used to indicate the start position of the list head
- `tail` was used to indicate the stop position of the list tail

the meaning of other fields was the same as other types, just add extra head/tail to record the boundary of the list.

#### list sub keys-values

the subkey in list was composed by list key, version, index, index was calculated from metadata's head or tail. for example, when the user requests the `rpush list elem`, kvrocks would fetch the metadata with list key first, and  generate the subkey with list key, version, and tail, simply increase the tail, then write the metadata and subkey's value back to rocksdb.

```shell
                     +---------------+
key|version|index => |     value     |
                     +---------------+
```

## ZSet

Redis zset was set with sorted property, so it's a little different with other types. it must be able to search with the member, as well as retrieve members with score range.

#### zset metadata

the metadata of zset was still same with set, like below

```shell
        +----------+------------+-----------+-----------+
key =>  |  flags   |  expire    |  version  |  size     |
        | (1byte)  | (4byte)    |  (8byte)  | (8byte)   |
        +----------+------------+-----------+-----------+
```

#### zset sub keys-values

the sub keys-values was different, except the value of sub key isn't null, we should have the way to range the members with the score. so the zset has two types of sub keys-values, one for map the members-scores, and one for score range.

```shell
                            +---------------+
key|version|member       => |     score     |   (1)
                            +---------------+
                            
                            +---------------+
key|version|score|member => |     NULL      |   (2)
                            +---------------+                     

```

if the user wants to get the score of the member or check the member exists or not, it would try first one.

## Bitmap

Redis bitmap is the most interesting part in kvrocks design, while unlike other types, it's not subkey and the value would be very large if the user treats it as a sparse array. it's apparent that the things would break down if store the bitmap into a single value, so we should break the bitmap value into multi fragments. another behavior of bitmap is the position would write always arbitrary, it's very similar to the access model of Linux virtual memory, so the idea of the bitmap design came from that.

#### bitmap metadata

```shell
        +----------+------------+-----------+-----------+
key =>  |  flags   |  expire    |  version  |  size     |
        | (1byte)  | (4byte)    |  (8byte)  | (8byte)   |
        +----------+------------+-----------+-----------+
```

#### bitmap sub keys-values

we break the bitmap values into fragments(1KiB, 8096 bit/fragment), and subkey is the index of the fragment. for example, when the request to set the bit of 1024 would locate in the first fragment with index 0 if set bit of 80970 would locate in 10th fragment with index 10. 

```shell
                     +---------------+
key|version|index => |     fragment  |
                     +---------------+
```

when the user requests to get it of position P, kvrocks would first fetch the metadata with bitmap's key and calculate the index of the fragment with bit position, then fetch the bitmap fragment with composed key and find the bit was set or not in fragment offset. for example, if getbit bitmap 8097, so the index is `1` (8097/8096) and subkey is `bitmap|1|1` (assume the version is 1), then fetch the subkey from rocksdb and check the bit of offset `1`(8097%8096) was set or not.

## Sortint

Sortint is Set whose member is int and sorted in int ascending order:

```shell
        +----------+------------+-----------+-----------+
key =>  |  flags   |  expire    |  version  |  size     |
        | (1byte)  | (4byte)    |  (8byte)  | (8byte)   |
        +----------+------------+-----------+-----------+
```

and the sub keys-values in rocksdb would be:

```shell
                      +---------------+
key|version|id => |     NULL      |
                      +---------------+
```
