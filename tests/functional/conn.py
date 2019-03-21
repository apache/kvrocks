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

