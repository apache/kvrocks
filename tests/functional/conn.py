import os
import sys
import redis

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,'../')

def get_redis_conn(master=True):
    if master:
        r = redis.Redis("127.0.0.1", 6666, 0, "foobared")
    else:
        r = redis.Redis("127.0.0.1", 6668, 0, "foobared")
    return r

def get_redis_conn_codis(group=1):
    if group == 1:
        r = redis.Redis("127.0.0.1", 6670, 0)
    else:
        r = redis.Redis("127.0.0.1", 6672, 0)
    return r