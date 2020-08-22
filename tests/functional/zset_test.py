import redis
import sys
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
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8, 'c', sys.float_info.max)
    assert (ret == 3)
    ret = conn.execute_command("ZPOPMAX", key)
    assert (ret[0] == 'c')
    assert (float(ret[1]) == sys.float_info.max)

    ret = conn.delete(key)
    assert(ret == 1)


def test_zpopmin():
    conn = get_redis_conn()
    key = "test_zpopmin"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8)
    assert (ret == 2)
    ret = conn.execute_command("ZPOPMIN", key)
    assert (ret[0] == 'a')
    assert (is_double_eq(float(ret[1]),1.3))
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
    ret = conn.zrange(key, 0, 1, False, True)
    assert(ret == [('a', 1.3), ('b', 1.8)])

    ret = conn.delete(key)
    assert(ret == 1)


def test_zrangebyscore():
    conn = get_redis_conn()
    key = "test_zrangebyscore"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8, 'c', 2.5)
    assert (ret == 3)
    ret = conn.zrangebyscore(key, 1, 3)
    assert(ret == ['a', 'b', 'c'])

    ret = conn.zrangebyscore(key, 1, 3, None, None, True)
    assert (ret == [('a', 1.3), ('b', 1.8), ('c', 2.5)])

    ret = conn.zrangebyscore(key, 1, 3, 0, 2, True)
    assert (ret == [('a', 1.3), ('b', 1.8)])

    ret = conn.zrangebyscore(key, 1, 3, 1, 2)
    assert (ret == ['b', 'c'])

    ret = conn.delete(key)
    assert(ret == 1)


def test_zlexcount():
    conn = get_redis_conn()
    key = "test_zlexcount"
    ret = conn.zadd(key, 'a', 0, 'b', 0, 'c', 0)
    assert (ret == 3)
    ret = conn.zlexcount(key, '-', '+')
    assert(ret == 3)

    ret = conn.zlexcount(key, '(a', '(c')
    assert (ret == 1)

    ret = conn.zlexcount(key, '(a', '[c')
    assert (ret == 2)

    ret = conn.zlexcount(key, '[a', '[c')
    assert (ret == 3)

    ret = conn.delete(key)
    assert(ret == 1)


def test_zrangebylex():
    conn = get_redis_conn()
    key = "test_zrangebylex"
    ret = conn.zadd(key, 'a', 0, 'b', 0, 'c', 0)
    assert (ret == 3)
    ret = conn.zrangebylex(key, '-', '+')
    assert(ret == ['a', 'b', 'c'])

    ret = conn.zrangebylex(key, '(a', '(c')
    assert (ret == ['b'])

    ret = conn.zrangebylex(key, '(a', '[c')
    assert (ret == ['b', 'c'])

    ret = conn.zrangebylex(key, '[a', '[c')
    assert (ret == ['a', 'b', 'c'])

    ret = conn.zrangebylex(key, '[a', '[c', 0, 2)
    assert (ret == ['a', 'b'])

    ret = conn.zrangebylex(key, '[a', '[c', 1, 2)
    assert (ret == ['b', 'c'])

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
    key = "test_zrevrange"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8, 'c', sys.float_info.max)
    assert (ret == 3)
    ret = conn.zrevrange(key, 0, 1)
    assert (ret == ['c', 'b'])

    ret = conn.zrevrange(key, 1, 2, True)
    assert (ret == [('b', 1.8), ('a', 1.3)])

    ret = conn.delete(key)
    assert (ret == 1)


def test_zrevrangebyscore():
    conn = get_redis_conn()
    key = "test_zrevrangebyscore"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8, 'c', 2.5, 'd', sys.float_info.max)
    assert (ret == 4)
    ret = conn.zrevrangebyscore(key, sys.float_info.max, 1)
    assert (ret == ['d', 'c', 'b', 'a'])
    ret = conn.zrevrangebyscore(key, 3, 1)
    assert(ret == ['c', 'b', 'a'])

    ret = conn.zrevrangebyscore(key, "+inf", "-inf")
    assert(ret == ['d', 'c', 'b', 'a'])

    ret = conn.zrevrangebyscore(key, 1.8, 1.3)
    assert(ret == ['b', 'a'])

    ret = conn.zrevrangebyscore(key, 1.8, "(1.3")
    assert(ret == ['b'])

    ret = conn.zrevrangebyscore(key, "(1.8", "(1.3")
    assert(ret == [])

    ret = conn.zrevrangebyscore(key, 3, 1, None, None, True)
    assert (ret == [('c', 2.5), ('b', 1.8), ('a', 1.3)])

    ret = conn.zrevrangebyscore(key, 3, 1, 0, 2, True)
    assert (ret == [('c', 2.5), ('b', 1.8)])

    ret = conn.zrevrangebyscore(key, 3, 1, 1, 2, True)
    assert (ret == [('b', 1.8), ('a', 1.3)])


    ret = conn.delete(key)
    assert(ret == 1)
    

def test_zrevrank():
    conn = get_redis_conn()
    key = "test_zrevrank"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.8, 'c', sys.float_info.max)
    assert (ret == 3)
    ret = conn.zrevrank(key, 'a')
    assert (ret == 2)

    ret = conn.zrevrank(key, 'c')
    assert (ret == 0)

    ret = conn.zrevrank(key, 'd')
    assert (ret == None)

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


def test_zremrangebylex():
    conn = get_redis_conn()
    key = "test_zremrangebylex"
    ret = conn.zadd(key, 'aaaa', 0, 'b', 0,
                    'c', 0, 'd', 0, 'e', 0, 'foo', 0, 'zap', 0, 'zip', 0, 'ALPHA', 0, 'alpha', 0)
    assert (ret == 10)
    ret = conn.zremrangebylex(key, '[alpha', '[omega')
    assert(ret == 6)

    ret = conn.zrangebylex(key, '-', '+')
    assert (ret == ['ALPHA', 'aaaa', 'zap', 'zip'])

    ret = conn.zrange(key, 0, -1)
    assert (ret == ['ALPHA', 'aaaa', 'zap', 'zip'])

    ret = conn.zremrangebylex(key, '-', '+')
    assert (ret == 4)

    ret = conn.zrangebylex(key, '-', '+')
    assert (ret == [])

    
def test_zscan():
    conn = get_redis_conn()
    key = "test_zscan"
    ret = conn.execute_command("ZSCAN " + key + " 0")
    if ret != ["0", []]:
        raise ValueError('ret is not ["0", []]')
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.3, 'c', 1.3)
    if ret != 3:
        raise ValueError('ret is not 3')
    ret = conn.execute_command("ZSCAN " + key + " 0")
    if ret != ['0', ['a', '1.3', 'b', '1.3', 'c', '1.3']]:
        raise ValueError('ret illegal')

    ret = conn.execute_command("ZSCAN " + key + " 0 COUNT 2")
    if ret != ['b', ['a', '1.3', 'b', '1.3']]:
        raise ValueError('ret illegal')

    ret = conn.execute_command("ZSCAN " + key + " b COUNT 2")
    if ret != ['0', ['c', '1.3']]:
        raise ValueError('ret illegal')

    ret = conn.delete(key)
    assert (ret == 1)


def test_zunionstore():
    conn = get_redis_conn()
    key = "test_zunionstore"
    key1 = key + "_1"
    key2 = key + "_2"
    ret = conn.zadd(key1, 'one', 1, 'two', 2)
    assert (ret == 2)
    ret = conn.zadd(key2, 'one', 1, 'two', 2, 'three', 3)
    assert (ret == 3)

    ret = conn.zunionstore(key, [key1, key2])
    assert (ret == 3)
    ret = conn.zrange(key, 0, -1, False, True)
    assert (ret == [('one', 2.0), ('three', 3), ('two', 4.0)])

    ret = conn.zunionstore(key, [key1, key2], "MIN")
    assert (ret == 3)
    ret = conn.zrange(key, 0, -1, False, True)
    assert (ret == [('one', 1.0), ('two', 2.0), ('three', 3)])

    ret = conn.zunionstore(key, [key1, key2], "MAX")
    assert (ret == 3)
    ret = conn.zrange(key, 0, -1, False, True)
    assert (ret == [('one', 1.0), ('two', 2.0), ('three', 3)])

    ret = conn.zunionstore(key, {key1: 10, key2: 30})
    assert (ret == 3)
    ret = conn.zrange(key, 0, -1, False, True)
    assert (ret == [('one', 40.0), ('two', 80.0), ('three', 90.0)])

    ret = conn.delete(key)
    assert (ret == 1)
    ret = conn.delete(key1)
    assert (ret == 1)
    ret = conn.delete(key2)
    assert (ret == 1)


def test_zinterstore():
    conn = get_redis_conn()
    key = "test_zinterstore"
    key1 = key + "_1"
    key2 = key + "_2"
    conn.delete(key1);
    conn.delete(key2);
    conn.delete(key);
    ret = conn.zadd(key1, 'one', 1, 'two', 2)
    assert (ret == 2)
    ret = conn.zadd(key2, 'one', 1, 'two', 2, 'three', 3)
    assert (ret == 3)

    ret = conn.zinterstore(key, [key1, key2])
    assert (ret == 2)
    ret = conn.zrange(key, 0, -1, False, True)
    assert (ret == [('one', 2.0), ('two', 4.0)])

    ret = conn.zinterstore(key, [key1, key2], "MIN")
    assert (ret == 2)
    ret = conn.zrange(key, 0, -1, False, True)
    assert (ret == [('one', 1.0), ('two', 2.0)])

    ret = conn.zinterstore(key, [key1, key2], "MAX")
    assert (ret == 2)
    ret = conn.zrange(key, 0, -1, False, True)
    assert (ret == [('one', 1.0), ('two', 2.0)])

    ret = conn.zinterstore(key, {key1: 10, key2: 30})
    assert (ret == 2)
    ret = conn.zrange(key, 0, -1, False, True)
    assert (ret == [('one', 40.0), ('two', 80.0)])

    ret = conn.delete(key)
    assert (ret == 1)
    ret = conn.delete(key1)
    assert (ret == 1)
    ret = conn.delete(key2)
    assert (ret == 1)


def test_zmscore():
    conn = get_redis_conn()
    key = "test_zmscore"
    ret = conn.zadd(key, 'a', 1.3, 'b', 1.4)
    if not ret == 2:
        raise ValueError('ret illegal')
    ret = conn.execute_command("zmscore", key, "a", "b", "c")
    if not ret == ['1.3', '1.3999999999999999', None]:
        raise ValueError('ret illegal')

    ret = conn.delete(key)
    if not ret == 1:
        raise ValueError('ret illegal')