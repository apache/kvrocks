import redis
from assert_helper import *
from conn import *


def test_slotsinfo():
    conn = get_redis_conn_codis(1)
    key = "test_slotsinfo"
    ret = conn.set(key, "bar")
    assert(ret == True)
    ret = conn.execute_command("slotsinfo", 0, 1024)
    assert(ret == [[527L, 1L]])

    ret = conn.delete(key)
    assert(ret == 1)


def test_slotsscan():
    conn = get_redis_conn_codis(1)
    key = "test_slotsscan"
    ret = conn.zadd(key, 'a', 1.3)
    assert (ret == 1)
    hash_num = conn.execute_command("slotshashkey", key)
    ret = conn.execute_command("slotsscan", hash_num[0], 0)
    if ret != ["0", [key]]:
        raise ValueError('ret illegal')

    ret = conn.delete(key)
    assert(ret == 1)


def test_slotsdel():
    conn = get_redis_conn_codis(1)
    key = "test_slotsdel"
    ret = conn.lpush(key, "bar")
    assert(ret == 1)
    hash_num = conn.execute_command("slotshashkey", key)
    ret = conn.execute_command("slotsscan", hash_num[0], 0)
    if ret != ["0", [key]]:
        raise ValueError('ret illegal')

    ret = conn.execute_command("slotsdel", hash_num[0])
    assert (ret == [[hash_num[0], 0]])
    ret = conn.execute_command("slotsscan", hash_num[0], 0)
    assert (ret == ['0', []])

    ret = conn.delete(key)
    assert (ret == 1)


def test_slotsmgrtslot():
    conn = get_redis_conn_codis(1)
    conn_group2 = get_redis_conn_codis(2)
    key = "test_slotsmgrtslot"
    ret = conn.sadd(key, 'a')
    assert (ret == 1)
    hash_num = conn.execute_command("slotshashkey", key)
    ret = conn.execute_command("slotsmgrtslot", "127.0.0.1", 6672, 3000, hash_num[0])
    assert (ret == [1L, 0L])

    value = conn_group2.sismember(key, 'a')
    assert (value == 1)
    value = conn.sismember(key, 'a')
    assert (value == 0)

    ret = conn_group2.delete(key)
    assert (ret == 1)


def test_slotsmgrtone():
    conn = get_redis_conn_codis(1)
    conn_group2 = get_redis_conn_codis(2)
    key = "test_slotsmgrtone"
    ret = conn.set(key, "bar")
    assert (ret == True)
    ret = conn.execute_command("slotsmgrtone", "127.0.0.1", 6672, 3000, key)
    assert (ret == 1L)

    value = conn_group2.get(key)
    assert (value == "bar")
    value = conn.get(key)
    assert (value == None)

    ret = conn_group2.delete(key)
    assert (ret == 1)


def test_slotsmgrttagslot():
    conn = get_redis_conn_codis(1)
    conn_group2 = get_redis_conn_codis(2)
    key = "test_slots{mgrt}tagslot"
    key2 = "test_slots{mgrt}tagslot_3333"
    ret = conn.execute_command("siadd", key, 1, 2, 3, 4)
    assert (ret == 4)
    ret = conn.hset(key2, "f1", 'v1')
    assert (ret == 1)
    hash_num = conn.execute_command("slotshashkey", key)
    ret = conn.execute_command("slotsmgrttagslot", "127.0.0.1", 6672, 3000, hash_num[0])
    assert (ret == [2L, 0L])

    ret = conn_group2.execute_command("sirange", key, 0, 20)
    assert (ret == ['1', '2', '3', '4'])
    value = conn_group2.hget(key2, "f1")
    assert (value == "v1")
    value = conn.execute_command("sirange", key, 0, 20)
    assert (value == [])
    value = conn.hget(key2, "f1")
    assert (value == None)

    ret = conn_group2.delete(key, key2)
    assert (ret == 2)


def test_slotsmgrttagone():
    conn = get_redis_conn_codis(1)
    conn_group2 = get_redis_conn_codis(2)
    key = "test_slots{mgrt}tagone"
    key2 = "test_slots{mgrt}tagone_3333"
    ret = conn.set(key, "bar")
    assert (ret == True)
    ret = conn.setbit(key2, 0, 1)
    assert (ret == 0)
    ret = conn.execute_command("slotsmgrttagone", "127.0.0.1", 6672, 3000, key)
    assert (ret == 2L)

    value = conn_group2.get(key)
    assert (value == "bar")
    value = conn_group2.getbit(key2, 0)
    assert (value == 1)
    value = conn.get(key)
    assert (value == None)
    value = conn.getbit(key2, 0)
    assert (value == 0)

    ret = conn_group2.delete(key, key2)
    assert (ret == 2)


def test_slotshashkey():
    conn = get_redis_conn_codis(1)
    key = "test_slotshashkey"
    key2 = "test_slots{hash}key"
    ret = conn.execute_command("slotshashkey", key, key2)
    assert (ret == [966L, 696L])


def test_slotscheck():
    conn = get_redis_conn_codis(1)
    key = "test_slotshashkey"
    ret = conn.set(key, "bar")
    assert (ret == True)
    ret = conn.execute_command("slotscheck")
    assert (ret == "OK")

    ret = conn.delete(key)
    assert (ret == 1)


def test_slotsmgrtslot_async():
    conn = get_redis_conn_codis(1)
    conn_group2 = get_redis_conn_codis(2)
    key = "test_slotsmgrtslot_async"
    ret = conn.sadd(key, 'a')
    assert (ret == 1)
    hash_num = conn.execute_command("slotshashkey", key)
    ret = conn.execute_command("slotsmgrtslot-async", "127.0.0.1", 6672, 3000, 20000, 2000, hash_num[0], 500)
    assert (ret == [1L, 0L])

    value = conn_group2.sismember(key, 'a')
    assert (value == 1)
    value = conn.sismember(key, 'a')
    assert (value == 0)

    ret = conn_group2.delete(key)
    assert (ret == 1)



def test_slotsmgrttagslot_async():
    conn = get_redis_conn_codis(1)
    conn_group2 = get_redis_conn_codis(2)
    key = "test_slots{mgrt}tagslot"
    key2 = "test_slots{mgrt}tagslot_3333"
    ret = conn.execute_command("siadd", key, 1, 2, 3, 4)
    assert (ret == 4)
    ret = conn.hset(key2, "f1", 'v1')
    assert (ret == 1)
    hash_num = conn.execute_command("slotshashkey", key)
    ret = conn.execute_command("slotsmgrttagslot-async", "127.0.0.1", 6672, 3000, 20000, 2000, hash_num[0], 500)
    assert (ret == [2L, 0L])

    ret = conn_group2.execute_command("sirange", key, 0, 20)
    assert (ret == ['1', '2', '3', '4'])
    value = conn_group2.hget(key2, "f1")
    assert (value == "v1")
    value = conn.execute_command("sirange", key, 0, 20)
    assert (value == [])
    value = conn.hget(key2, "f1")
    assert (value == None)

    ret = conn_group2.delete(key, key2)
    assert (ret == 2)