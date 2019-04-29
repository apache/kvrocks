import redis
from assert_helper import *
from conn import *

def test_hget_and_hset():
    key = "test_hget_and_hset"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    for i, k in enumerate(keys):
        ret = conn.hset(key, k, kvs[k])
        assert(ret == 1)
        ret = conn.hget(key, k)
        assert(ret == kvs[k])
    ret = conn.delete(key)
    assert(ret == 1)

    for i, k in enumerate(keys):
        ret = conn.hget(key, k)
        assert(ret == None)

def test_hincrby():
    key = "test_hincrby"
    conn = get_redis_conn()
    for i in range(1, 10):
        ret = conn.hincrby(key, "f1", 1)
        assert(ret == i)
    ret = conn.delete(key)
    assert(ret == 1)
    # TODO(linty): not number of overflow case
    assert_raise(redis.RedisError, conn.hincrby, key, "f1", "invalid")

def test_hincrbyfloat():
    key = "test_hincrbyfloat"
    conn = get_redis_conn()
    for i in range(1, 10):
        ret = conn.hincrbyfloat(key, "f1", 1.234)
        assert(ret == i*1.234)
    ret = conn.delete(key)
    assert(ret == 1)
    # TODO(linty): not number of overflow case
    assert_raise(redis.RedisError, conn.hincrbyfloat, key, "f1", "invalid")

def test_hsetnx():
    key = "test_hsetnx"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    for i, k in enumerate(keys):
        ret = conn.hsetnx(key, k, kvs[k])
        assert(ret == 1)
        ret = conn.hget(key, k)
        assert(ret == kvs[k])
    for i, k in enumerate(keys):
        ret = conn.hsetnx(key, k, kvs[k])
        assert(ret == 0)
    ret = conn.delete(key)
    assert(ret == 1)

def test_hstrlen():
    key = "test_hstrlen"
    conn = get_redis_conn()
    ret = conn.hset(key, "f1", "hello")
    assert(ret == True)
    ret = conn.hstrlen(key, "f1")
    assert(ret == 5)
    ret = conn.delete(key)
    assert(ret == 1)

def test_hdel():
    key = "test_hdel"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    conn.hmset(key, kvs)
    ret = conn.hdel(key, *keys)
    assert(ret == len(kvs))

def test_hexists():
    key = "test_hexists"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    for i, k in enumerate(keys):
        ret = conn.hset(key, k, kvs[k])
        assert(ret == True)
        ret = conn.hexists(key, k)
        assert(ret == True)
    ret = conn.delete(key)
    assert(ret == 1)
    for i, k in enumerate(keys):
        ret = conn.hexists(key, k)
        assert(ret == False)

def test_hlen():
    key = "test_hlen"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    ret = conn.hmset(key, kvs)
    assert(ret == True)
    ret = conn.hlen(key)
    assert(ret == len(kvs))
    ret = conn.delete(key)
    assert(ret == 1)

def test_mget_and_mset():
    key = "test_mget_and_mset"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    ret = conn.hmset(key, kvs)
    assert(ret == True)
    ret = conn.hmget(key, kvs.keys())
    assert(ret == kvs.values())
    ret = conn.delete(key)
    assert(ret == 1)

def test_hkeys():
    key = "test_hkeys"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    ret = conn.hmset(key, kvs)
    assert(ret == True)
    ret = conn.hkeys(key)
    assert(sorted(keys) == ret)
    ret = conn.delete(key)
    assert(ret == 1)

def test_hvals():
    key = "test_hvals"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    ret = conn.hmset(key, kvs)
    assert(ret == True)
    ret = conn.hvals(key)
    assert(sorted(kvs.values()) == ret)
    ret = conn.delete(key)
    assert(ret == 1)

def test_hgetall():
    key = "test_hgetall"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    ret = conn.hmset(key, kvs)
    assert(ret == True)
    ret = conn.hgetall(key)
    assert(ret == kvs)
    ret = conn.delete(key)
    assert(ret == 1)

def test_hscan():
    conn = get_redis_conn()
    key = "test_hscan"
    ret = conn.hset(key, 'a', 1.3)
    assert (ret == 1)
    ret = conn.execute_command("HSCAN " + key + " 0")
    assert (ret == ['a', ['a']])

    ret = conn.delete(key)
    assert (ret == 1)
