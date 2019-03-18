import os
import sys
import redis

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,'../')

def get_redis_conn():
    r = redis.Redis("127.0.0.1", 6666, 0, "foobared")
    return r

