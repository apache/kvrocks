import redis
import time
from assert_helper import *
from conn import *

def test_type():
    conn = get_redis_conn()
    string_key = "test_string_type"
    ret = conn.set(string_key, "bar")
    assert(ret == True)
    ret = conn.type(string_key)
    assert(ret == "string")
    hash_key = "test_hash_type"
    ret = conn.hset(hash_key, "f1", "v1")
    assert(ret == 1)
    ret = conn.type(hash_key)
    assert(ret == "hash")
    list_key = "test_list_type"
    ret = conn.lpush(list_key, "v1")
    assert(ret == 1)
    ret = conn.type(list_key)
    assert(ret == "list")
    set_key = "test_set_type"
    ret = conn.sadd(set_key, "s1")
    assert(ret == 1)
    ret = conn.type(set_key)
    assert(ret == "set")
    zset_key = "test_zset_type"
    ret = conn.zadd(zset_key, "s1", 0.1)
    assert(ret == 1)
    ret = conn.type(zset_key)
    assert(ret == "zset")
    bitmap_key = "test_bitmap_type"
    ret = conn.setbit(bitmap_key, 0, 1)
    assert(ret == 0)
    ret = conn.type(bitmap_key)
    assert(ret == "bitmap")
    sortedint_key = "test_sortedint_type"
    ret = conn.execute_command("siadd", sortedint_key, 1)
    assert(ret == 1)
    ret = conn.type(sortedint_key)
    assert(ret == "sortedint")
    ret = conn.delete(string_key, hash_key, list_key, set_key, zset_key, bitmap_key, sortedint_key)
    if ret != 7:
        raise ValueError('ret is not 7: ' + ret)

def test_expire():
    key = "test_expire"
    conn = get_redis_conn()
    ret = conn.lpush(key, "v1")
    assert(ret == 1)
    ret = conn.expire(key, 2)
    assert(ret == 1)
    ret = conn.ttl(key)
    assert(ret >= 1 and ret <= 2)
    time.sleep(3)
    ret = conn.exists(key)
    assert(ret == False)
    
def test_exists():
    key = "test_exists"
    conn = get_redis_conn()
    ret = conn.set(key, "bar")
    assert(ret)
    ret = conn.exists(key)
    assert(ret)
    ret = conn.delete(key)
    assert(ret == 1)
    ret = conn.exists(key)
    assert(not ret)

def test_ttl():
    key = "test_ttl"
    conn = get_redis_conn()
    ret = conn.set(key, "bar")
    assert(ret)
    ret = conn.ttl(key)
    assert(ret == None)
    ret = conn.ttl("notexistskey")
    assert(ret == None)
    ret = conn.expire(key, 2)
    assert(ret == 1)
    ret = conn.ttl(key)
    assert(ret >= 1 and ret <= 2)


def test_object_dump():
    default_namespace = "__namespace"
    conn = get_redis_conn()

    string_key = "test_string_dump"
    ret = conn.set(string_key, "bar")
    assert(ret == True)
    ret = conn.object("dump", string_key)
    assert(ret[1] == default_namespace)
    assert(ret[3] == "string")
    assert(ret[7] == "0")
    assert(ret[9] == "0")
    ret = conn.expire(string_key, 2)
    assert (ret == True)
    ret = conn.object("dump", string_key)
    ttl = int(ret[7]) - int(time.time())
    assert (1 <= ttl <= 2)

    hash_key = "test_hash_dump"
    ret = conn.hset(hash_key, "f1", "v1")
    assert(ret == 1)
    ret = conn.object("dump", hash_key)
    assert(ret[1] == default_namespace)
    assert(ret[3] == "hash")
    assert(ret[7] == "0")
    assert(ret[9] == "1")
    ret = conn.hset(hash_key, "f2", "v2")
    assert (ret == 1)
    ret = conn.object("dump", hash_key)
    assert (ret[1] == default_namespace)
    assert (ret[3] == "hash")
    assert (ret[7] == "0")
    assert (ret[9] == "2")
    ret = conn.hdel(hash_key, "f2")
    assert (ret == 1)
    ret = conn.object("dump", hash_key)
    assert (ret[1] == default_namespace)
    assert (ret[3] == "hash")
    assert (ret[7] == "0")
    assert (ret[9] == "1")

    list_key = "test_list_dump"
    ret = conn.lpush(list_key, "v1")
    assert(ret == 1)
    ret = conn.object("dump", list_key)
    assert(ret[1] == default_namespace)
    assert(ret[3] == "list")
    assert(ret[7] == "0")
    assert(ret[9] == "1")
    ret = conn.lpush(list_key, "v2")
    assert (ret == 2)
    ret = conn.object("dump", list_key)
    assert (ret[1] == default_namespace)
    assert (ret[3] == "list")
    assert (ret[7] == "0")
    assert (ret[9] == "2")
    ret = conn.lpop(list_key)
    assert (ret == "v2")
    ret = conn.object("dump", list_key)
    assert (ret[1] == default_namespace)
    assert (ret[3] == "list")
    assert (ret[7] == "0")
    assert (ret[9] == "1")

    set_key = "test_set_dump"
    ret = conn.sadd(set_key, "s1")
    assert(ret == 1)
    ret = conn.object("dump", set_key)
    assert(ret[1] == default_namespace)
    assert(ret[3] == "set")
    assert(ret[7] == "0")
    assert(ret[9] == "1")
    ret = conn.sadd(set_key, "s2")
    assert (ret == 1)
    ret = conn.object("dump", set_key)
    assert (ret[1] == default_namespace)
    assert (ret[3] == "set")
    assert (ret[7] == "0")
    assert (ret[9] == "2")
    ret = conn.spop(set_key)
    assert (ret == ['s1'])
    ret = conn.object("dump", set_key)
    assert (ret[1] == default_namespace)
    assert (ret[3] == "set")
    assert (ret[7] == "0")
    assert (ret[9] == "1")

    zset_key = "test_zset_dump"
    ret = conn.zadd(zset_key, "s1", 0.1)
    assert(ret == 1)
    ret = conn.object("dump", zset_key)
    assert(ret[1] == default_namespace)
    assert(ret[3] == "zset")
    assert(ret[7] == "0")
    assert(ret[9] == "1")
    ret = conn.zadd(zset_key, "s2", 0.2)
    assert (ret == 1)
    ret = conn.object("dump", zset_key)
    assert (ret[1] == default_namespace)
    assert (ret[3] == "zset")
    assert (ret[7] == "0")
    assert (ret[9] == "2")
    ret = conn.zrem(zset_key, "s2")
    assert (ret == 1)
    ret = conn.object("dump", zset_key)
    assert (ret[1] == default_namespace)
    assert (ret[3] == "zset")
    assert (ret[7] == "0")
    assert (ret[9] == "1")
    ret = conn.expire(zset_key, 2)
    assert (ret == True)
    ret = conn.object("dump", zset_key)
    ttl = int(ret[7]) - int(time.time())
    assert (1 <= ttl <= 2)

    ret = conn.delete(string_key, hash_key, list_key, set_key, zset_key)
    assert(ret == 5)


def test_persist():
    key = "test_persist"
    conn = get_redis_conn()
    ret = conn.persist(key)
    assert(not ret)
    ret = conn.set(key, "bar")
    assert(ret)
    ret = conn.persist(key)
    assert(not ret)
    ret = conn.expire(key, 100)
    assert(ret == 1)
    ret = conn.persist(key)
    assert(ret)
    ret = conn.delete(key)
    assert(ret == 1)

def test_expireat():
    key = "test_expireat"
    conn = get_redis_conn()
    ret = conn.sadd(key, "s1")
    assert(ret == 1)
    ret = conn.expireat(key, time.time()+2)
    assert(ret == 1)
    ret = conn.ttl(key)
    assert(ret >= 1 and ret <= 2)
    time.sleep(3)
    ret = conn.exists(key)
    assert(ret == False)

def test_pexpire():
    key = "test_pexpire"
    conn = get_redis_conn()
    ret = conn.hset(key, "f1", "v1")
    assert(ret == 1)
    ret = conn.pexpire(key, 2000)
    assert(ret == 1)
    ret = conn.pttl(key)
    if not 1000 <= ret <= 2000:
        raise ValueError('ret is not between 1000~2000: ' + ret)
    time.sleep(3)
    ret = conn.exists(key)
    assert(ret == False)

    ret = conn.hset(key, "f1", "v1")
    assert(ret == 1)
    ret = conn.pexpire(key, 900)
    assert(ret == 1)
    ret = conn.pttl(key)
    if not 0 <= ret <= 1000:
        raise ValueError('ret is not between 0~1000: ' + ret)

def test_pexpireat():
    key = "test_pexpireat"
    conn = get_redis_conn()
    ret = conn.sadd(key, "s1")
    assert(ret == 1)
    ret = conn.pexpireat(key, (time.time()+2)*1000)
    assert(ret == 1)
    ret = conn.pttl(key)
    assert(ret >= 1000 and ret <= 2000)
    time.sleep(3)
    ret = conn.exists(key)
    assert(ret == False)
    
def test_pttl():
    key = "test_ttl"
    conn = get_redis_conn()
    ret = conn.set(key, "bar")
    assert(ret)
    ret = conn.ttl(key)
    assert(ret == None)
    ret = conn.ttl("notexistskey")
    assert(ret == None)
    ret = conn.pexpire(key, 2000)
    assert(ret == 1)
    ret = conn.ttl(key)
    assert(ret >= 1 and ret <= 2)

    ret = conn.delete(key)
    assert(ret == 1)

def test_randomkey():
    keys = ["test_randomkey", "test_randomkey_1", "test_randomkey_2"]
    conn = get_redis_conn()
    for key in keys:
        ret = conn.set(key, "bar")
        assert(ret)

    ret = conn.execute_command("RANDOMKEY")
    assert(ret in keys)

    for key in keys:
        ret = conn.delete(key)
        assert(ret == 1)

def test_scan():
    keys = ["test_scankey", "test_scankey_1", "test_scankey_2"]
    conn = get_redis_conn()
    ret = conn.execute_command("SCAN 0")
    if ret != ["0", []]:
        raise ValueError('ret is not ["0", []]')

    for key in keys:
        ret = conn.set(key, "bar")
        if not ret:
            raise ValueError('ret is not OK')

    ret = conn.execute_command("SCAN 0 count 10")
    if ret != ["0", keys]:
        raise ValueError('ret illegal')

    ret = conn.execute_command("SCAN 0 count 2")
    if ret != [keys[1], [keys[0], keys[1]]]:
        raise ValueError('ret illegal')

    ret = conn.execute_command("SCAN " + keys[1] + " count 2")
    if ret != ['0', [keys[2]]]:
        raise ValueError('ret illegal')

    ret = conn.delete(key)
    assert (ret == 1)


def test_flushdb():
    key = "test_flushdb"
    key_zset = key + "_zset"
    conn = get_redis_conn()
    ret = conn.set(key, "bar")
    assert ret
    ret = conn.zadd(key_zset, 'a', 1.3)
    assert (ret == 1)

    ret = conn.flushdb()
    assert ret

    ret = conn.exists(key)
    assert (ret == False)
    ret = conn.exists(key_zset)
    assert (ret == False)

    ret = conn.set(key, "bar")
    assert ret
    ret = conn.get(key)
    assert (ret == "bar")
    ret = conn.zadd(key_zset, 'a', 1.3)
    assert (ret == 1)
    ret = conn.zscore(key_zset, 'a')
    assert (ret == 1.3)

    ret = conn.flushdb()
    assert ret

    ret = conn.exists(key)
    assert (ret == False)
    ret = conn.exists(key_zset)
    assert (ret == False)


def test_flushall():
    key = "test_flushall"
    key_zset = key + "_zset"
    conn = get_redis_conn()
    ret = conn.set(key, "bar")
    assert (ret)
    ret = conn.zadd(key_zset, 'a', 1.3)
    assert (ret == 1)

    ret = conn.flushall()
    assert (ret)

    ret = conn.exists(key)
    assert (ret == False)
    ret = conn.exists(key_zset)
    assert (ret == False)

    ret = conn.set(key, "bar")
    assert ret
    ret = conn.get(key)
    assert (ret == "bar")
    ret = conn.zadd(key_zset, 'a', 1.3)
    assert (ret == 1)
    ret = conn.zscore(key_zset, 'a')
    assert (ret == 1.3)

    ret = conn.flushdb()
    assert ret

    ret = conn.exists(key)
    assert (ret == False)
    ret = conn.exists(key_zset)
    assert (ret == False)
