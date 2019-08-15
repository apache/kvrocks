import redis
import time
from assert_helper import *
from conn import *

def test_psync_string_set_and_del():
    key = "test_psync_string_set_and_del"
    conn = get_redis_conn()
    conn_slave = get_redis_conn(False)
    ret = conn.set(key, "bar")
    assert (ret == True)
    time.sleep(0.01)
    value = conn_slave.get(key)
    assert (value == "bar")
    ret = conn.delete(key)
    assert (ret == 1)
    time.sleep(0.01)
    ret = conn_slave.get(key)
    assert(ret == None)

def test_psync_string_setex():
    key = "test_psync_string_setex"
    conn = get_redis_conn()
    conn_slave = get_redis_conn(False)
    ret = conn.setex(key, "bar", 1024)
    assert(ret == True)
    time.sleep(0.01)
    ret = conn_slave.ttl(key)
    assert(ret >= 1023 and ret <= 1025)

    ret = conn.delete(key)
    assert(ret == 1)


def test_psync_expire():
    key = "test_psync_expire"
    conn = get_redis_conn()
    conn_slave = get_redis_conn(False)
    ret = conn.set(key, "bar")
    assert(ret == True)
    ret = conn.expire(key, 1024)
    time.sleep(0.01)
    ret = conn_slave.ttl(key)
    assert(ret >= 1023 and ret <= 1025)

    ret = conn.delete(key)
    assert(ret == 1)

def test_psync_zset():
    key = "test_psync_zset"
    conn = get_redis_conn()
    conn_slave = get_redis_conn(False)
    rst = conn.zadd(key, 'a', 1.3)
    assert(rst == 1)
    time.sleep(0.01)
    ret = conn_slave.zscore(key, 'a')
    assert(ret == 1.3)
    rst = conn.zrem(key, 'a')
    assert(rst == 1)
    time.sleep(0.01)
    ret = conn_slave.zscore(key, 'a')
    assert(None == ret)

def test_psync_list():
    key = "test_psync_list"
    conn = get_redis_conn()
    conn_slave = get_redis_conn(False)
    ret = conn.lpush(key, 'a')
    assert (ret == 1)
    time.sleep(0.01)
    ret = conn_slave.lindex(key, 0)
    assert (ret == 'a')
    ret = conn.lset(key, 0, 'b')
    assert(ret == 1)
    time.sleep(0.01)
    ret = conn_slave.lindex(key, 0)
    assert(ret == 'b')

    ret = conn.rpop(key)
    assert (ret == 'b')
    time.sleep(0.01)
    ret = conn_slave.lindex(key, 0)
    assert (None == ret)

    ret = conn.rpush(key, 'a')
    assert (ret == 1)
    time.sleep(0.01)
    ret = conn_slave.lindex(key, 0)
    assert (ret == 'a')

    ret = conn.lpop(key)
    assert (ret == 'a')
    time.sleep(0.01)
    ret = conn_slave.lrange(key, 0, 1)
    assert (ret == [])

    elems = ["one", "two", "three"]
    ret = conn.rpush(key, *elems)
    assert (ret == len(elems))
    ret = conn.ltrim(key, 1, -1)
    assert (ret)
    time.sleep(0.01)
    ret = conn_slave.lrange(key, 0, -1)
    assert (ret == elems[1:])

    ret = conn.delete(key)
    assert(ret == 1)


def test_psync_set():
    key = "test_psync_set"
    conn = get_redis_conn()
    conn_slave = get_redis_conn(False)
    rst = conn.sadd(key, 'a')
    assert(rst == 1)
    time.sleep(0.01)
    ret = conn_slave.sismember(key, 'a')
    assert(ret == 1)
    ret = conn.srem(key, 'a')
    assert(ret == 1)
    time.sleep(0.01)
    ret = conn_slave.sismember(key, 'a')
    assert(ret == 0)


def test_psync_hash():
    key = "test_psync_hash"
    conn = get_redis_conn()
    conn_slave = get_redis_conn(False)
    ret = conn.hset(key, 'a', '1.3')
    assert(ret == 1)
    time.sleep(0.01)
    ret = conn_slave.hget(key, 'a')
    assert(ret == '1.3')
    ret = conn.hdel(key, 'a')
    assert(ret == 1)
    time.sleep(0.01)
    ret = conn_slave.hget(key, 'a')
    assert(None == ret)

def test_psync_bitmap():
    key = "test_psync_bitmap"
    conn = get_redis_conn()
    conn_slave = get_redis_conn(False)
    ret = conn.setbit(key, 1, 1)
    assert(ret == 0)
    time.sleep(0.01)
    ret = conn_slave.getbit(key, 1)
    assert(ret == 1)
    ret = conn.setbit(key, 1, 0)
    assert(ret == 1)
    time.sleep(0.01)
    ret = conn_slave.getbit(key, 1)
    assert(ret == 0)

    ret = conn.delete(key)
    assert(ret == 1)
