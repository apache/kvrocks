import redis
from assert_helper import *
from conn import *

def test_pipeline_without_transaction():
    key = "test_pipeline_without_transaction"
    conn = get_redis_conn()
    pipe = conn.pipeline(False)
    pipe.hset(key, "f1", "v1")
    pipe.hset(key, "f2", "v2")
    pipe.hset(key, "f3", "v3")
    pipe.hset(key, "f4", "v4")
    pipe.hlen(key)
    ret = pipe.execute()
    assert(ret == [1, 1, 1, 1, 4])
    ret = conn.delete(key)
    assert(ret == 1)
