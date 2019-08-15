import redis
from assert_helper import *
from conn import *

def test_namespace_get():
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    for i, k in enumerate(keys):
        ret = conn.execute_command("namespace", "get", k)
        assert(ret == None)
        ret = conn.execute_command("namespace", "add", k, kvs[k])
        assert(ret == "OK")
        ret = conn.execute_command("namespace", "get", k)
        assert(ret == kvs[k])
    for i, k in enumerate(keys):
        ret = conn.execute_command("namespace", "del", k)
        assert(ret)

def test_namespace_add():
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    for i, k in enumerate(keys):
        ret = conn.execute_command("namespace", "get", k)
        assert(ret == None)
        ret = conn.execute_command("namespace", "add", k, kvs[k])
        assert(ret == "OK")
    for i, k in enumerate(keys):
        assert_raise(redis.RedisError, conn.execute_command, "namespace", "add", k, kvs[k]+"-new")
    for i, k in enumerate(keys):
        ret = conn.execute_command("namespace", "del", k)
        assert(ret)

def test_namespace_set():
    conn = get_redis_conn()
    kvs = {'kkk-%s' % i :'vvv-%s' % i for i in range(10)}
    keys = kvs.keys()
    for i, k in enumerate(keys):
        ret = conn.execute_command("namespace", "get", k)
        assert(ret == None)
        assert_raise(redis.RedisError, conn.execute_command, "namespace", "set", k, kvs[k]+"-new")
        ret = conn.execute_command("namespace", "add", k, kvs[k])
        assert(ret == "OK")
        ret = conn.execute_command("namespace", "set", k, kvs[k]+"-new")
        assert(ret == "OK")
    for i, k in enumerate(keys):
        ret = conn.execute_command("namespace", "del", k)
        assert(ret)
