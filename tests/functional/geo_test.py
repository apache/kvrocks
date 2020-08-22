from assert_helper import *
from conn import *


def test_geoadd_and_geohash():
    conn = get_redis_conn()
    key = "test_geoadd_and_geohash"
    field1 = "f1"
    field2 = "f2"
    field1_long = 13.361389
    field1_lat = 38.115556
    field2_long = 15.087269
    field2_lat = 37.502669
    ret = conn.execute_command("geoadd", key, field1_long, field1_lat, field1, field2_long, field2_lat, field2)
    if not ret == 2:
        raise ValueError('ret is not 2: ' + ret)
    ret = conn.execute_command("geohash", key, field1, field2)
    if not ret == ["sqc8b49rny0", "sqdtr74hyu0"]:
        raise ValueError('ret is not ["sqc8b49rny0", "sqdtr74hyu0"]: ' + ret)

    ret = conn.delete(key)
    if not ret == 1:
        raise ValueError('ret is not 1: ' + ret)


def test_geodist():
    conn = get_redis_conn()
    key = "test_geodist"
    field1 = "f1"
    field2 = "f2"
    field1_long = 13.361389
    field1_lat = 38.115556
    field2_long = 15.087269
    field2_lat = 37.502669
    ret = conn.execute_command("geoadd", key, field1_long, field1_lat, field1, field2_long, field2_lat, field2)
    if not ret == 2:
        raise ValueError('ret is not 2: ' + ret)
    ret = conn.execute_command("geodist", key, field1, field2, 'mi')
    if not is_double_eq(float(ret), 103.318225) :
        raise ValueError('ret is not 103.318225: ' + ret)

    ret = conn.delete(key)
    if not ret == 1:
        raise ValueError('ret is not 1: ' + ret)


def test_geopos():
    conn = get_redis_conn()
    key = "test_geopos"
    field1 = "f1"
    field2 = "f2"
    field1_long = 13.361389
    field1_lat = 38.115556
    field2_long = 15.087269
    field2_lat = 37.502669
    ret = conn.execute_command("geoadd", key, field1_long, field1_lat, field1, field2_long, field2_lat, field2)
    if not ret == 2:
        raise ValueError('ret is not 2: ' + ret)
    ret = conn.execute_command("geopos", key, field1, field2)

    if not ret == [['13.361389338970184', '38.115556395496299'], ['15.087267458438873', '37.50266842333162']]:
        raise ValueError('ret is not correct: ' + ret)

    ret = conn.delete(key)
    if not ret == 1:
        raise ValueError('ret is not 1: ' + ret)


def test_georadius():
    conn = get_redis_conn()
    key = "test_georadius"
    field1 = "f1"
    field2 = "f2"
    field1_long = 13.361389
    field1_lat = 38.115556
    field2_long = 15.087269
    field2_lat = 37.502669
    ret = conn.execute_command("geoadd", key, field1_long, field1_lat, field1, field2_long, field2_lat, field2)
    if not ret == 2:
        raise ValueError('ret is not 2: ' + ret)
    ret = conn.execute_command("georadius", key, field2_long, field2_lat, 200, "km", "WITHDIST", "WITHCOORD")
    print(ret)
    if not ret == [[field1, '166.27424828631862', ['13.361389338970184', '38.115556395496299']],
                   [field2, '0.00015038910532546101', ['15.087267458438873', '37.50266842333162']]]:
        raise ValueError('ret is not correct: ' + ret)

    ret = conn.delete(key)
    if not ret == 1:
        raise ValueError('ret is not 1: ' + ret)


def test_georadiusbymember():
    conn = get_redis_conn()
    key = "test_georadiusbymember"
    field1 = "f1"
    field2 = "f2"
    field1_long = 13.361389
    field1_lat = 38.115556
    field2_long = 15.087269
    field2_lat = 37.502669
    ret = conn.execute_command("geoadd", key, field1_long, field1_lat, field1, field2_long, field2_lat, field2)
    if not ret == 2:
        raise ValueError('ret is not 2: ' + ret)
    ret = conn.execute_command("georadiusbymember", key, field2, 200, "km", "WITHDIST", "WITHCOORD")
    if ret != [[field1, '166.27415156960032', ['13.361389338970184', '38.115556395496299']],
               [field2, '0', ['15.087267458438873', '37.50266842333162']]]:
        raise ValueError('ret is not correct: ' + ' '.join(ret))

    ret = conn.delete(key)
    if not ret == 1:
        raise ValueError('ret is not 1: ' + ret)