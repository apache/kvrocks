import redis
from assert_helper import *
from conn import *

def test_getbit_and_setbit():
    key = "test_getbit_and_setbit"
    conn = get_redis_conn()
    bits = [0, 1, 2, 3, 1024, 1024*8, 1024*8+1, 1024*8+2, 1024*8+3, 4*1024*8, 4*1024*8+1]
    for pos in bits:
        ret = conn.getbit(key, pos)
        assert(ret == 0)
        ret = conn.setbit(key, pos, 1)
        assert(ret == 0)
        ret = conn.getbit(key, pos)
        assert(ret == 1)
        ret = conn.setbit(key, pos, 0)
        assert(ret == 1)
        ret = conn.getbit(key, pos)
        assert(ret == 0)
    ret = conn.delete(key)
    assert(ret == 1)

def test_bitcount():
    key = "test_bitcount"
    conn = get_redis_conn()
    bits = [0, 1, 2, 3, 1024, 1024*8, 1024*8+1, 1024*8+2, 1024*8+3, 4*1024*8, 4*1024*8+1]
    for pos in bits:
        ret = conn.getbit(key, pos)
        assert(ret == 0)
        ret = conn.setbit(key, pos, 1)
        assert(ret == 0)
    ret = conn.bitcount(key)
    assert(ret == len(bits))
    for pos in bits:
        ret = conn.setbit(key, pos, 0)
        assert(ret == 1)
    ret = conn.bitcount(key)
    assert(ret == 0)
    ret = conn.delete(key)
    assert(ret == 1)

def test_bitpos():
    key = "test_bitpos"
    conn = get_redis_conn()
    bits = [0, 1, 2, 3, 1024, 1024*8, 1024*8+1, 1024*8+2, 1024*8+3, 4*1024*8, 4*1024*8+1]
    for pos in bits:
        ret = conn.getbit(key, pos)
        assert(ret == 0)
        ret = conn.setbit(key, pos, 1)
        assert(ret == 0)
    ret = conn.bitpos(key, 0, 0, 3)
    assert(ret == 4)
    ret = conn.bitpos(key, 1, 0, 3)
    assert(ret == 0)
    ret = conn.bitpos(key, 0, 1024)
    assert(ret == 1024*8+4)
    ret = conn.bitpos(key, 1, 1024)
    assert(ret == 1024*8)
    ret = conn.delete(key)
    assert(ret == 1)
