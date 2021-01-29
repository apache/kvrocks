import redis
from assert_helper import *
from conn import *

def test_get_and_set():
    key = "test_get_and_set"
    conn = get_redis_conn()
    ret = conn.set(key, "bar")
    assert(ret == True)
    value = conn.get(key)
    assert(value == "bar")
    ret = conn.delete(key)
    assert(ret == 1)

def test_append():
    key = "test_append"
    conn = get_redis_conn()
    ret = conn.append(key, "Hello")
    assert(ret == 5)
    ret = conn.append(key, " World")
    assert(ret == 11)
    ret = conn.get(key)
    assert(ret == "Hello World")
    ret = conn.delete(key)
    assert(ret == 1)

def test_strlen():
    key = "test_strlen"
    conn = get_redis_conn()
    ret = conn.set(key, "Hello World")
    assert(ret)
    ret = conn.strlen(key)
    assert(ret == 11)
    ret = conn.strlen("noexistskey")
    assert(ret == 0)
    ret = conn.delete(key)
    assert(ret == 1)

def test_delete():
    key = "test_delete"
    conn = get_redis_conn()
    ret = conn.delete(key)
    assert(ret == 0)
    ret = conn.set(key, "bar")
    ret = conn.delete(key)
    assert(ret == 1)

def test_getset():
    key = "test_getset"
    conn = get_redis_conn()
    ret = conn.getset(key, "bar")
    assert(ret == None)
    ret = conn.getset(key, "new_bar")
    assert(ret == "bar")
    ret = conn.delete(key)
    assert(ret == 1)

def test_set_with_option():
    key = "test_set_with_option"
    conn = get_redis_conn()
    ret = conn.set(key, "bar", nx=True)
    assert(ret == True)
    ret = conn.set(key, "bar", nx=True)
    assert(ret == None)
    ret = conn.set(key, "bar", xx=True)
    assert(ret == True)
    ret = conn.set(key, "bar", px=1024000, xx=True)
    assert(ret == True)
    ret = conn.ttl(key)
    if not 1023 <= ret <= 1025:
        raise ValueError('ret is not between 1023~1025: ' + ret)
    ret = conn.set(key, "bar", px=900, xx=True)
    assert(ret == True)
    ret = conn.ttl(key)
    if not 0 <= ret <= 1:
        raise ValueError('ret is not between 0~1: ' + ret)
    ret = conn.set(key, "bar", ex=1024, xx=True)
    assert(ret == True)
    ret = conn.ttl(key)
    if not 1023 <= ret <= 1025:
        raise ValueError('ret is not between 1023~1025: ' + ret)
    ret = conn.set(key, "bar", ex=1024)
    assert(ret == True)
    ret = conn.ttl(key)
    if not 1023 <= ret <= 1025:
        raise ValueError('ret is not between 1023~1025: ' + ret)
    ret = conn.delete(key)
    assert(ret == 1)

def test_setex():
    key = "test_setex"
    conn = get_redis_conn()
    ret = conn.setex(key, "bar", 1024)
    assert(ret == True)
    ret = conn.ttl(key)
    assert(ret >= 1023 and ret <= 1025)
    ret = conn.delete(key)
    assert(ret == 1)
    assert_raise(redis.RedisError, conn.setex, "foo", "bar", "invalid")

def test_psetex():
    key = "test_psetex"
    conn = get_redis_conn()
    ret = conn.psetex(key, 1000*1024, "bar")
    assert(ret == True)
    ret = conn.ttl(key)
    assert(ret >= 1023 and ret <= 1025)
    ret = conn.delete(key)
    assert(ret == 1)
    assert_raise(redis.RedisError, conn.psetex, "foo", "invalid", "bar")

def test_setnx():
    key = "test_setnx"
    conn = get_redis_conn()
    ret = conn.setnx(key, "bar")
    assert(ret == 1)
    ret = conn.setnx(key, "bar")
    assert(ret == 0)
    ret = conn.delete(key)
    assert(ret == 1)

def test_getrange():
    key = "test_getrange"
    value = "This is a string"
    conn = get_redis_conn()
    ret = conn.set(key, value)
    assert(ret)
    ret = conn.getrange(key, 0, 3)
    assert(ret == value[0:4])
    ret = conn.getrange(key, -3, -1)
    assert(value[-3:] == ret)
    ret = conn.getrange(key, 0, -1)
    assert(value == ret)
    ret = conn.getrange(key, 10, 100)
    assert(value[10:] == ret)
    ret = conn.delete(key)
    assert(ret == 1)

def test_setrange():
    key = "test_setrange"
    conn = get_redis_conn()
    ret = conn.set(key, "hello world")
    assert(ret == 1)
    ret = conn.setrange(key, 6, "redis")
    assert(ret == 11) 
    ret = conn.get(key)
    assert(ret == "hello redis")
    ret = conn.delete(key)
    assert(ret == 1)
    ret = conn.setrange(key, 6, "redis")
    assert(ret == 11) 
    ret = conn.get(key)
    assert(ret == ("\0"*6+"redis"))
    ret = conn.delete(key)
    assert(ret == 1)

def test_incrby():
    key = "test_incrby"
    conn = get_redis_conn()
    ret = conn.incrby(key, 100)
    assert(ret == 100) 
    ret = conn.incrby(key, -100)
    assert(ret == 0) 
    ret = conn.delete(key)
    assert(ret == 1)
    # TODO: not number of overflow case

def test_mset_and_mget():
    key = "test_mset_and_mget"
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    ret = conn.mset(**kvs)
    assert(ret == True)
    keys = kvs.keys()
    vals = conn.mget(keys)
    for i, k in enumerate(keys):
        assert(kvs[k] == vals[i])
    for i, k in enumerate(keys):
        ret = conn.delete(k)
        assert(ret == True)

def test_incr_by_float():
    key = "test_incr_by_float"
    conn = get_redis_conn()
    ret = conn.incrbyfloat(key, 1.11)
    assert(is_double_eq(ret, 1.11))
    ret = conn.incrbyfloat(key, -1.11)
    assert(ret == 0) 
    ret = conn.delete(key)
    assert(ret == 1)

