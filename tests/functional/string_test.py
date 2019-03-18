import redis
from assert_helper import *
from conn import *

def test_get_and_set():
    conn = get_redis_conn()
    ret = conn.set("foo", "bar")
    assert(ret == True)
    value = conn.get("foo")
    assert(value == "bar")
    ret = conn.delete("foo")
    assert(ret == 1)

def test_delete():
    conn = get_redis_conn()
    ret = conn.delete("foo")
    assert(ret == 0)
    ret = conn.set("foo", "bar")
    ret = conn.delete("foo")
    assert(ret == 1)

def test_getset():
    conn = get_redis_conn()
    ret = conn.getset("foo", "bar")
    assert(ret == None)
    ret = conn.getset("foo", "new_bar")
    assert(ret == "bar")
    ret = conn.delete("foo")
    assert(ret == 1)

def test_setex():
    conn = get_redis_conn()
    ret = conn.setex("foo", "bar", 1024)
    assert(ret == True)
    ret = conn.ttl("foo")
    assert(ret >= 1023 and ret <= 1025)
    ret = conn.delete("foo")
    assert(ret == 1)
    assert_raise(redis.RedisError, conn.setex, "foo", "bar", "invalid")

def test_setnx():
    conn = get_redis_conn()
    ret = conn.setnx("foo", "bar")
    assert(ret == 1)
    ret = conn.setnx("foo", "bar")
    assert(ret == 0)
    ret = conn.delete("foo")
    assert(ret == 1)

def test_setrange():
    conn = get_redis_conn()
    ret = conn.set("foo", "hello world")
    assert(ret == 1)
    ret = conn.setrange("foo", 6, "redis")
    assert(ret == 11) 
    ret = conn.get("foo")
    assert(ret == "hello redis")
    ret = conn.delete("foo")
    assert(ret == 1)
    ret = conn.setrange("foo", 6, "redis")
    assert(ret == 11) 
    ret = conn.get("foo")
    assert(ret == ("\0"*6+"redis"))
    ret = conn.delete("foo")
    assert(ret == 1)

def test_incrby():
    conn = get_redis_conn()
    ret = conn.incrby("foo", 100)
    assert(ret == 100) 
    ret = conn.incrby("foo", -100)
    assert(ret == 0) 
    ret = conn.delete("foo")
    assert(ret == 1)
    # TODO: not number of overflow case

def test_mset_and_mget():
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
    conn = get_redis_conn()
    ret = conn.incrbyfloat("foo", 1.11)
    assert(ret == 1.11) 
    ret = conn.incrbyfloat("foo", -1.11)
    assert(ret == 0) 
    ret = conn.delete("foo")
    assert(ret == 1)

