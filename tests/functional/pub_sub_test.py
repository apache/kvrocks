import redis
from assert_helper import *
import time
import threading
from conn import *


def subscribe(channel, master=True):
    conn = get_redis_conn(master)
    p = conn.pubsub()
    p.subscribe(channel)

    for item in p.listen():
        if item['type'] == "message":
            assert (item['data'] == "a")
            p.unsubscribe()
            break


def test_publish():
    channel = "test_publish"

    x = threading.Thread(target=subscribe, args=(channel,))
    x.start()

    y = threading.Thread(target=subscribe, args=(channel, False))
    y.start()

    time.sleep(2)

    conn = get_redis_conn()
    ret = conn.publish(channel, "a")
    assert (ret == 1)
