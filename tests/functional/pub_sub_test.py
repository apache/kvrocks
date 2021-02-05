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
        if item['type'] == "pmessage":
            assert (item['data'] == "a")
            p.punsubscribe()
            break


def test_replication():
    channel = "test_publish"

    x = threading.Thread(target=subscribe, args=(channel,))
    x.start()

    y = threading.Thread(target=subscribe, args=(channel, False))
    y.start()

    time.sleep(1)

    conn = get_redis_conn()
    ret = conn.publish(channel, "a")
    assert (ret == 1)

    time.sleep(0.01)

    ret = conn.execute_command("pubsub", "channels")
    assert (ret == [])


def test_pubsub_channels():
    channel = "test_pubsub_channels"
    channel_two = "two_test_pubsub_channels"
    pattern_match_all = "test*"
    pattern_unmatch_all = "a*"
    pattern_match_question_mark = "test?pubsub_channels"
    pattern_unmatch_question_mark = "tes?pubsub_channels"
    pattern_match_or = "tes[ta]_pubsub_channels"
    pattern_unmatch_or = "tes[sa]_pubsub_channels"

    x = threading.Thread(target=subscribe, args=(channel,))
    x.start()

    time.sleep(1)

    conn = get_redis_conn()
    ret = conn.execute_command("pubsub", "channels")
    assert (ret == [channel])
    ret = conn.execute_command("pubsub", "channels", pattern_match_all)
    assert (ret == [channel])
    ret = conn.execute_command("pubsub", "channels", pattern_unmatch_all)
    assert (ret == [])
    ret = conn.execute_command("pubsub", "channels", pattern_match_question_mark)
    assert (ret == [channel])
    ret = conn.execute_command("pubsub", "channels", pattern_unmatch_question_mark)
    assert (ret == [])
    ret = conn.execute_command("pubsub", "channels", pattern_match_or)
    assert (ret == [channel])
    ret = conn.execute_command("pubsub", "channels", pattern_unmatch_or)
    assert (ret == [])

    y = threading.Thread(target=subscribe, args=(channel_two,))
    y.start()

    time.sleep(1)

    ret = conn.execute_command("pubsub", "channels")
    assert (ret == [channel, channel_two])

    ret = conn.publish(channel, "a")
    assert (ret == 1)
    ret = conn.publish(channel_two, "a")
    assert (ret == 1)

    time.sleep(0.01)

    ret = conn.execute_command("pubsub", "channels")
    assert (ret == [])


def test_pubsub_numsub():
    channel = "test_pubsub_numsub"

    x = threading.Thread(target=subscribe, args=(channel,))
    x.start()

    time.sleep(1)

    conn = get_redis_conn()

    ret = conn.execute_command("pubsub", "numsub", channel)
    assert (ret == [channel, 1L])

    y = threading.Thread(target=subscribe, args=(channel,))
    y.start()

    time.sleep(1)

    ret = conn.execute_command("pubsub", "numsub", channel)
    assert (ret == [channel, 2L])

    ret = conn.publish(channel, "a")
    assert (ret == 2)

    time.sleep(0.01)

    ret = conn.execute_command("pubsub", "numsub", channel)
    assert (ret == [channel, 0L])


def test_pubsub_numpat():
    channel = "test_publish"
    channel_two = "2_test_publish"
    pattern_match_all = "test*"
    pattern_match_all_two = "2*"

    conn = get_redis_conn()

    x = threading.Thread(target=psubscribe, args=(pattern_match_all,))
    x.start()

    time.sleep(1)

    ret = conn.execute_command("pubsub", "numpat")
    assert (ret == 1)

    y = threading.Thread(target=psubscribe, args=(pattern_match_all_two,))
    y.start()

    time.sleep(1)

    ret = conn.execute_command("pubsub", "numpat")
    assert (ret == 2)

    ret = conn.publish(channel, "a")
    assert (ret == 1)
    ret = conn.publish(channel_two, "a")
    assert (ret == 1)

    time.sleep(0.01)

    ret = conn.execute_command("pubsub", "numpat")
    assert (ret == 0)