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
        assert(is_double_eq(ret, i*1.234))
    ret = conn.delete(key)
    assert(ret == 1)
    ret = conn.hincrbyfloat(key, "fi", 1.11)
    assert(is_double_eq(ret, 1.11))
    ret = conn.hincrbyfloat(key, "fi", -1.11)
    assert(is_double_eq(ret, 0.0))
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
        ret = conn.hsetnx(key, k, kvs[k] + "_2")
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
    conn.delete(key)
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    ret = conn.hmset(key, kvs)
    assert(ret == True)
    ret = conn.hmget(key, kvs.keys())
    assert(ret == kvs.values())
    ret = conn.hmget(key, ['kkk-1', 'f-no-exist'])
    assert(ret == ['vvv-1', None])
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
    ret = conn.execute_command("HSCAN " + key + " 0")
    if ret != ["0", []]:
        raise ValueError('ret is not ["0", []]')
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(3)}
    ret = conn.hmset(key, kvs)
    if not ret:
        raise ValueError('ret is not ok')
    ret = conn.execute_command("HSCAN " + key + " 0")
    if ret != ['0', ['kkk-0', 'vvv-0', 'kkk-1', 'vvv-1', 'kkk-2', 'vvv-2']]:
        raise ValueError('ret illegal')

    ret = conn.execute_command("HSCAN " + key + " 0 COUNT 2")
    if ret != ['kkk-1', ['kkk-0', 'vvv-0', 'kkk-1', 'vvv-1']]:
        raise ValueError('ret illegal')

    ret = conn.execute_command("HSCAN " + key + " kkk-1 COUNT 2")
    if ret != ['0', ['kkk-2', 'vvv-2']]:
        raise ValueError('ret illegal')

    ret = conn.delete(key)
    assert (ret == 1)
