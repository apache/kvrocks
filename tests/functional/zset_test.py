import redis
from assert_helper import *
from conn import *


def test_zadd_and_zscore():
    conn = get_redis_conn()
    key = "test_sadd_and_srem"
    ret = conn.zadd(key, 'a', 1.3)
    assert(ret == 1)
    ret = conn.zscore(key, 'a')
    assert(ret == 1.3)

    ret = conn.delete(key)
    assert(ret == 1)


def test_zcard():
    conn = get_redis_conn()
    key = "test_zcard"
    ret = conn.zadd(key, 'a', 1.3)
    ret = conn.zcard(key)
    assert(ret == 1)

    ret = conn.delete(key)
    assert(ret == 1)


def test_zcount():
    conn = get_redis_conn()
    key = "test_zcount"
    ret = conn.zadd(key, 'a', 1.3, 'b', 5.3)
    ret = conn.zcount(key, 1, 100)
    assert(ret == 2)

    ret = conn.delete(key)
    assert(ret == 1)


def test_zincrby():
    conn = get_redis_conn()
    key = "test_zincrby"
    ret = conn.zincrby(key, 'a', 1.3)
    assert (ret == 1.3)
    ret = conn.zscore(key, 'a')
    assert(ret == 1.3)

    ret = conn.delete(key)
    assert(ret == 1)


def test_zpopmax():
    conn = get_redis_conn()
    key = "test_zpopmax"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.execute_command("ZPOPMAX " + key)
    assert (ret == ['b', '1.800000'])

    ret = conn.delete(key)
    assert(ret == 1)


def test_zpopmin():
    conn = get_redis_conn()
    key = "test_zpopmin"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.execute_command("ZPOPMIN " + key)
    assert (ret == ['a', '1.300000'])

    ret = conn.delete(key)
    assert(ret == 1)


def test_zrange():
    conn = get_redis_conn()
    key = "test_zrange"
    ret = conn.delete(key)
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.zrange(key, 0, 1)
    assert(ret == ['a', 'b'])

    ret = conn.delete(key)
    assert(ret == 1)


def test_zrangebyscore():
    conn = get_redis_conn()
    key = "test_zrangebyscore"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.zrangebyscore(key, 1, 3)
    assert(ret == ['a', 'b'])

    ret = conn.delete(key)
    assert(ret == 1)


def test_zrank():
    conn = get_redis_conn()
    key = "test_zrank"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.zrank(key, 'b')
    assert(ret == 1)

    ret = conn.delete(key)
    assert(ret == 1)


def test_zrevrange():
    conn = get_redis_conn()
    key = "test_zrange"
    ret = conn.delete(key)
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.zrevrange(key, 0, 1)
    assert (ret == ['b', 'a'])

    ret = conn.delete(key)
    assert (ret == 1)


def test_zrevrank():
    conn = get_redis_conn()
    key = "test_zrevrank"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.zrevrank(key, 'a')
    assert (ret == 1)

    ret = conn.delete(key)
    assert (ret == 1)


def test_zrem():
    conn = get_redis_conn()
    key = "test_zrem"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.zrem(key, 'a')
    assert(ret == 1)

    ret = conn.delete(key)
    assert(ret == 1)


def test_zremrangebyrank():
    conn = get_redis_conn()
    key = "test_zremrangebyrank"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.zremrangebyrank(key, 0, 1)
    assert (ret == 2)


def test_zremrangebyscore():
    conn = get_redis_conn()
    key = "test_zremrangebyscore"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.zremrangebyscore(key, 0, 3)
    assert (ret == 2)

def test_zscan():
    conn = get_redis_conn()
    key = "test_zscan"
    ret = conn.zadd(key, 'a', 1.3)
    assert (ret == 1)
    ret = conn.execute_command("ZSCAN " + key + " 0")
    assert (ret == ['a', ['a']])

    ret = conn.delete(key)
    assert (ret == 1)




