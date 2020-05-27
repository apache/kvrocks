import redis
from assert_helper import *
from conn import *


def test_sadd_and_sismember():
    conn = get_redis_conn()
    key = "test_sadd_and_srem"
    ret = conn.sadd(key, 'a')
    assert(ret == 1)
    value = conn.sismember(key, 'a')
    assert(value == 1)

    ret = conn.delete(key)
    assert(ret == 1)


def test_srem():
    conn = get_redis_conn()
    key = "test_srem"
    ret = conn.sadd(key, 'a')
    ret = conn.srem(key, 'a')
    assert(ret == 1)


def test_spop():
    conn = get_redis_conn()
    key = "test_spop"
    ret = conn.sadd(key, 'a')
    ret = conn.spop(key)
    assert(ret[0] == 'a')

def test_smove():
    conn = get_redis_conn()
    key = "test_smove"
    key_o = 'test_smove_o'
    ret = conn.sadd(key, 'a', 'b')
    ret = conn.sadd(key_o, 'c')
    ret = conn.smove(key, key_o, 'a')
    assert(ret == 1)
    ret = conn.smembers(key_o)
    assert (ret == {'c', 'a'})


    ret = conn.delete(key)
    assert (ret == 1)
    ret = conn.delete(key_o)
    assert (ret == 1)


def test_scard():
    conn = get_redis_conn()
    key = "test_scard"
    ret = conn.sadd(key, 'a')
    ret = conn.scard(key)
    assert(ret == 1)

    ret = conn.delete(key)
    assert (ret == 1)


def test_srandmember():
    conn = get_redis_conn()
    key = "test_srandmember"
    ret = conn.sadd(key, 'a')
    ret = conn.srandmember(key)
    assert(ret[0] == 'a')

    ret = conn.delete(key)
    assert (ret == 1)


def test_smembers():
    conn = get_redis_conn()
    key = "test_smembers"
    ret = conn.sadd(key, 'a')
    ret = conn.smembers(key)
    assert(ret == {'a'})

    ret = conn.delete(key)
    assert (ret == 1)


def test_sdiff():
    conn = get_redis_conn()
    key = "test_sdiff"
    key_o = 'test_sdiff_o'
    ret = conn.sadd(key, 'a', 'b')
    ret = conn.sadd(key_o, 'b')
    ret = conn.sdiff(key, key_o)
    assert(ret == {'a'})

    ret = conn.delete(key)
    assert (ret == 1)
    ret = conn.delete(key_o)
    assert (ret == 1)


def test_sdiffstore():
    conn = get_redis_conn()
    key = 'test_sdiffstore'
    key_main = "test_sdiff"
    key_o = 'test_sdiff_o'
    ret = conn.sadd(key_main, 'a', 'b')
    ret = conn.sadd(key_o, 'b')
    ret = conn.sdiffstore(key, key_main, key_o)
    assert(ret == 1)
    ret = conn.smembers(key)
    assert (ret == {'a'})

    ret = conn.delete(key)
    assert (ret == 1)
    ret = conn.delete(key_main)
    assert (ret == 1)
    ret = conn.delete(key_o)
    assert (ret == 1)


def test_sinter():
    conn = get_redis_conn()
    key = "test_sinter"
    key_o = 'test_sinter_o'
    ret = conn.sadd(key, 'a', 'b')
    ret = conn.sadd(key_o, 'a')
    ret = conn.sinter(key, key_o)
    assert(ret == {'a'})

    ret = conn.delete(key)
    assert (ret == 1)
    ret = conn.delete(key_o)
    assert (ret == 1)


def test_sinterstore():
    conn = get_redis_conn()
    key = 'test_sinterstore'
    key_main = "test_sinter"
    key_o = 'test_sinter_o'
    ret = conn.sadd(key_main, 'a', 'b')
    ret = conn.sadd(key_o, 'a')
    ret = conn.sinterstore(key, key_main, key_o)
    assert(ret == 1)
    ret = conn.smembers(key)
    assert (ret == {'a'})

    ret = conn.delete(key)
    assert (ret == 1)
    ret = conn.delete(key_main)
    assert (ret == 1)
    ret = conn.delete(key_o)
    assert (ret == 1)


def test_sunion():
    conn = get_redis_conn()
    key = "test_sunion"
    key_o = 'test_sunion_o'
    ret = conn.sadd(key, 'a', 'b')
    ret = conn.sadd(key_o, 'a')
    ret = conn.sunion(key, key_o)
    assert(ret == {'a', 'b'})

    ret = conn.delete(key)
    assert (ret == 1)
    ret = conn.delete(key_o)
    assert (ret == 1)


def test_sunionstore():
    conn = get_redis_conn()
    key = 'test_sunionstore'
    key_main = "test_sunion"
    key_o = 'test_sunion_o'
    ret = conn.sadd(key_main, 'a', 'b')
    ret = conn.sadd(key_o, 'a')
    ret = conn.sunionstore(key, key_main, key_o)
    assert(ret == 2)
    ret = conn.smembers(key)
    assert (ret == {'a', 'b'})

    ret = conn.delete(key)
    assert (ret == 1)
    ret = conn.delete(key_main)
    assert (ret == 1)
    ret = conn.delete(key_o)
    assert (ret == 1)


def test_sscan():
    conn = get_redis_conn()
    key = "test_sscan"
    members = ['a', 'b', 'c']
    ret = conn.execute_command("SSCAN " + key + " 0")
    if ret != ["0", []]:
        raise ValueError('ret is not ["0", []]: ')
    ret = conn.sadd(key, members[0], members[1], members[2])
    ret = conn.execute_command("SSCAN " + key + " 0")
    if ret != ['0', members]:
        raise ValueError('ret illegal')

    ret = conn.execute_command("SSCAN " + key + " 0 COUNT 2")
    if ret != ['b', [members[0], members[1]]]:
        raise ValueError('ret illegal')

    ret = conn.execute_command("SSCAN " + key + " b COUNT 2")
    if ret != ['0', [members[2]]]:
        raise ValueError('ret illegal')

    ret = conn.delete(key)
    assert(ret == 1)
