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


def psubscribe(pattern, master=True):
    conn = get_redis_conn(master)
    p = conn.pubsub()
    p.psubscribe(pattern)

    for item in p.listen():
        if item['type'] == "message":
            assert (item['data'] == "a")
            p.punsubscribe()
            break


def test_publish():
    channel = "test_publish"
    pattern = "test*"

    x = threading.Thread(target=subscribe, args=(channel,))
    x.start()

    y = threading.Thread(target=subscribe, args=(channel, False))
    y.start()

    a = threading.Thread(target=psubscribe, args=(pattern,))
    a.start()

    a = threading.Thread(target=psubscribe, args=(pattern, False))
    a.start()

    time.sleep(2)

    conn = get_redis_conn()
    ret = conn.execute_command("pubsub", "channels")
    assert (ret == [channel])
    ret = conn.execute_command("pubsub", "channels", pattern)
    assert (ret == [channel])
    ret = conn.execute_command("pubsub", "numsub", channel)
    assert (ret == [channel, 1L])
    ret = conn.execute_command("pubsub", "numpat")
    assert (ret == 1)
    ret = conn.publish(channel, "a")
    assert (ret == 2)
