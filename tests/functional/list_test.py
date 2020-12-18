import redis
import threading
import time
from assert_helper import *
from conn import *

def test_lpush_and_rpop():
    key = "test_lpush_and_rpop"
    conn = get_redis_conn()
    for i in range (10):
        ret = conn.lpush(key, "val-"+str(i))
        assert((i+1 == ret))
    for i in range (10):
        ret = conn.rpop(key)
        assert(ret == "val-"+str(i))

def test_lpush_multi_elems():
    key = "test_lpush_multi_elems"
    conn = get_redis_conn()
    elems = ["a", "b", "c"]
    ret = conn.lpush(key, *elems)
    assert(ret == len(elems))
    ret = conn.delete(key)
    assert(ret == 1)
    
def test_rpush_and_lpop():
    key = "test_rpush_and_lpop"
    conn = get_redis_conn()
    for i in range (10):
        ret = conn.rpush(key, "val-"+str(i))
        assert((i+1 == ret))
    for i in range (10):
        ret = conn.lpop(key)
        assert(ret == "val-"+str(i))

def test_lpushx():
    key = "test_lpushx"
    conn = get_redis_conn()
    ret = conn.lpushx(key, "noop")
    assert(ret == 0)
    ret = conn.lpush(key, "val-0")
    assert(ret == 1)
    for i in range (10):
        ret = conn.lpushx(key, "val-"+str(i))
        assert(i+2 == ret)
    ret = conn.rpop(key)
    assert(ret == "val-0")
    for i in range (10):
        ret = conn.rpop(key)
        assert(ret == "val-"+str(i))

def test_rpushx():
    key = "test_rpushx"
    conn = get_redis_conn()
    ret = conn.rpushx(key, "noop")
    assert(ret == 0)
    ret = conn.rpush(key, "val-0")
    assert(ret == 1)
    for i in range (10):
        ret = conn.rpushx(key, "val-"+str(i))
        assert(i+2 == ret)
    ret = conn.lpop(key)
    assert(ret == "val-0")
    for i in range (10):
        ret = conn.lpop(key)
        assert(ret == "val-"+str(i))

def test_lindex():
    key = "test_lindex"
    conn = get_redis_conn()
    elems = ["a", "b", "c", "d", "e"]
    ret = conn.rpush(key, *elems)
    assert(ret == len(elems))
    for i in range(len(elems)):
        ret = conn.lindex(key, i)
        assert(ret == elems[i])
    for i in range(-1*len(elems), 0):
        ret = conn.lindex(key, i)
        assert(ret == elems[i])
    ret = conn.lindex(key, len(elems))
    assert(None == ret)
    ret = conn.delete(key)
    assert(ret == 1)

def test_lset():
    key = "test_lset"
    conn = get_redis_conn()
    elems = ["a", "b", "c", "d", "e"]
    ret = conn.rpush(key, *elems)
    assert(ret == len(elems))
    for i in range (len(elems)): 
        assert(conn.lset(key, i, str(i)))
    for i in range(len(elems)):
        ret = conn.lindex(key, i)
        assert(ret == str(i))
    ret = conn.delete(key)
    assert(ret == 1)

def test_llen():
    key = "test_lset"
    conn = get_redis_conn()
    ret = conn.llen(key)
    assert(ret == 0)
    elems = ["a", "b", "c", "d", "e"]
    ret = conn.rpush(key, *elems)
    assert(ret == len(elems))
    ret = conn.llen(key)
    assert(ret == len(elems))
    ret = conn.delete(key)
    assert(ret == 1)

def test_lrange():
    key = "test_lrange"
    conn = get_redis_conn()
    elems = ["one", "two", "three"]
    ret = conn.rpush(key, *elems)
    assert(ret == len(elems))
    ret = conn.lrange(key, 0, 0)
    assert(ret == [elems[0]])
    ret = conn.lrange(key, -3, 2)
    assert(ret == elems)
    ret = conn.lrange(key, -100, 100)
    assert(ret == elems)
    ret = conn.lrange(key, 5, 10)
    assert(ret == [])
    ret = conn.delete(key)
    assert(ret == 1)

def test_ltrim():
    key = "test_ltrim"
    conn = get_redis_conn()
    elems = ["one", "two", "three", "four", "five"]
    ret = conn.rpush(key, *elems)
    assert(ret == len(elems))
    ret = conn.ltrim(key, 0, 2000)
    assert(ret)
    ret = conn.llen(key)
    if not ret == len(elems):
        raise ValueError('ret illegal')
    ret = conn.ltrim(key, 1, 3)
    if not ret == 1:
        raise ValueError('ret illegal')
    ret = conn.lrange(key, 0, -1)
    if not ret == elems[1:4]:
        raise ValueError('ret illegal')
    ret = conn.ltrim(key, 1, -1)
    if not ret == 1:
        raise ValueError('ret illegal')
    ret = conn.lrange(key, 0, -1)
    if not ret == elems[2:4]:
        raise ValueError('ret illegal')
    ret = conn.ltrim(key, -100, 0)
    if not ret == 1:
        raise ValueError('ret illegal')
    ret = conn.lrange(key, 0, -1)
    if not ret == [elems[2]]:
        raise ValueError('ret illegal')
    ret = conn.ltrim(key, 100, 0)
    if not ret == 1:
        raise ValueError('ret illegal')
    ret = conn.lrange(key, 0, -1)
    assert(ret == [])

def test_lrem():
    key = "test_lrem"
    conn = get_redis_conn()
    elems = ["E1", "E2", "E3", "hello", "E4", "E5", "hello", "E6"]
    elems_without_hello =  ["E1", "E2", "E3", "E4", "E5", "E6"]
    ret = conn.rpush(key, *elems)
    assert(ret == len(elems))
    ret = conn.execute_command("LREM", key, 0, "hello")
    assert (ret == 2)
    ret = conn.lrange(key, 0, -1)
    assert(ret == elems_without_hello)
    ret = conn.execute_command("LREM", key, 1, elems_without_hello[0])
    assert (ret == 1)
    ret = conn.lrange(key, 0, -1)
    assert(ret == elems_without_hello[1:])
    ret = conn.execute_command("LREM", key, -1, elems_without_hello[5])
    assert (ret == 1)
    ret = conn.lrange(key, 0, -1)
    assert(ret == elems_without_hello[1:5])

    ret = conn.delete(key)
    assert(ret == 1)

def test_linsert():
    key = "test_linsert"
    conn = get_redis_conn()
    ret = conn.delete(key)
    elems = ["E1", "E2", "E3", "E3"]
    ret = conn.rpush(key, *elems)
    assert(ret == len(elems))

    ret = conn.linsert(key, "after", "E3", "E4")
    assert (ret == 5)
    ret = conn.lrange(key, 0, -1)
    assert(ret == ["E1", "E2", "E3", "E4", "E3"])
    ret = conn.linsert(key, "before", "E3", "E5")
    assert (ret == 6)
    ret = conn.lrange(key, 0, -1)
    print ret
    assert(ret == ["E1", "E2", "E5", "E3", "E4", "E3"])

    ret = conn.delete(key)
    assert(ret == 1)

def test_rpoplpush():
    key = "test_rpoplpush"
    new_key = "new_test_rpoplpush"
    conn = get_redis_conn()
    elems = ["one", "two", "three"]
    ret = conn.rpush(key, *elems)
    assert(ret == len(elems))
    ret = conn.rpoplpush(key, new_key)
    assert(ret == elems[-1])
    ret = conn.lrange(key, 0, -1)
    assert(ret == elems[0:-1])
    ret = conn.lrange(new_key, 0, -1)
    assert(ret == elems[-1:])
    ret = conn.delete(key, new_key)
    assert(ret == 2)


def bpop(key):
    conn = get_redis_conn()
    ret = conn.execute_command("brpop", key, 1)
    assert (ret == None)
    ret = conn.execute_command("brpop", key, 0)
    assert (ret == [key, 'a'])

    ret = conn.execute_command("blpop", key, 0)
    assert (ret == [key, 'b'])


def test_bpop():
    key = "test_bpop"
    conn = get_redis_conn()
    x = threading.Thread(target=bpop, args=(key,))
    x.start()

    time.sleep(3)
    ret = conn.rpush(key, "a")
    assert(ret == 1)
    time.sleep(3)
    ret = conn.lpush(key, "b")
    assert(ret == 1)

