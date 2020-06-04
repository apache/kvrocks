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


def test_getbit_and_setbit_redis_string_bitmap():
    key = "test_getbit_and_setbit_redis_string_bitmap"
    conn = get_redis_conn()
    ret = conn.set(key, "\xe0")
    if not ret:
        raise ValueError('ret is not true')
    bits = [0, 1, 2]
    for pos in bits:
        ret = conn.getbit(key, pos)
        if not ret == 1:
            raise ValueError('ret is not 1' + str(ret))
        ret = conn.setbit(key, pos, 0)
        if not ret == 1:
            raise ValueError('ret is not 1: ' + str(ret))
        ret = conn.getbit(key, pos)
        if not ret == 0:
            raise ValueError('ret is not 0: ' + str(ret))
        ret = conn.setbit(key, pos, 1)
        if not ret == 0:
            raise ValueError('ret is not 0: ' + str(ret))
        ret = conn.getbit(key, pos)
        if not ret == 1:
            raise ValueError('ret is not 1: ' + str(ret))
    new_bits = [3, 1024, 1024*8, 1024*8+1, 1024*8+2, 1024*8+3, 4*1024*8, 4*1024*8+1]
    for pos in new_bits:
        ret = conn.getbit(key, pos)
        if not ret == 0:
            raise ValueError('ret is not 0: ' + str(ret))
        ret = conn.setbit(key, pos, 1)
        if not ret == 0:
            raise ValueError('ret is not 0: ' + str(ret))
        ret = conn.getbit(key, pos)
        if not ret == 1:
            print(pos)
            raise ValueError('ret is not 1: ' + str(ret))
        ret = conn.setbit(key, pos, 0)
        if not ret == 1:
            raise ValueError('ret is not 1: ' + str(ret))
        ret = conn.getbit(key, pos)
        if not ret == 0:
            raise ValueError('ret is not 0: ' + str(ret))
    ret = conn.delete(key)
    if not ret == 1:
        raise ValueError('ret is not 1: ' + str(ret))


def test_bitcount_redis_string_bitmap():
    key = "test_bitcount_redis_string_bitmap"
    conn = get_redis_conn()
    ret = conn.set(key, "\xe0")
    if not ret:
        raise ValueError('ret is not true: ' + str(ret))
    bits = [0, 1, 2]
    ret = conn.bitcount(key)
    if not ret == len(bits):
        raise ValueError('ret is not ' + len(bits) + ': ' + str(ret))
    new_bits = [3, 1024, 1024*8, 1024*8+1, 1024*8+2, 1024*8+3, 4*1024*8, 4*1024*8+1]
    for pos in new_bits:
        ret = conn.getbit(key, pos)
        if not ret == 0:
            raise ValueError('ret is not 0: ' + str(ret))
        ret = conn.setbit(key, pos, 1)
        if not ret == 0:
            raise ValueError('ret is not 0: ' + str(ret))
    ret = conn.bitcount(key)
    assert(ret == (len(bits) + len(new_bits)))
    for pos in new_bits:
        ret = conn.setbit(key, pos, 0)
        if not ret == 1:
            raise ValueError('ret is not 1: ' + str(ret))
    ret = conn.bitcount(key)
    assert(ret == len(bits))

    for pos in bits:
        ret = conn.setbit(key, pos, 0)
        if not ret == 1:
            raise ValueError('ret is not 1: ' + str(ret))
    ret = conn.bitcount(key)
    if not ret == 0:
        raise ValueError('ret is not 0: ' + str(ret))

    ret = conn.delete(key)
    if not ret == 1:
        raise ValueError('ret is not 1: ' + str(ret))


def test_bitpos_redis_string_bitmap():
    key = "test_bitpos_redis_string_bitmap"
    conn = get_redis_conn()
    ret = conn.set(key, "\xe0")
    if not ret:
        raise ValueError('ret is not true: ' + str(ret))
    new_bits = [3, 1024, 1024*8, 1024*8+1, 1024*8+2, 1024*8+3, 4*1024*8, 4*1024*8+1]
    for pos in new_bits:
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