import redis
import codecs
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_host', default='127.0.0.1', type=str)
    parser.add_argument('--src_port', default=6666, type=int)
    parser.add_argument('--src_password', default='foobared')
    parser.add_argument('--dst_host', default='127.0.0.1',type=str)
    parser.add_argument('--dst_port', default=6379, type=int)
    parser.add_argument('--dst_password', default='')

    return parser.parse_args()

def get_redis_client(host, port, password):
    return redis.Redis(host=host, port=port, decode_responses=True, password=password)

def compare_string(src_cli, dst_cli, key):
    src_data = src_cli.get(key)
    dst_data = dst_cli.get(key)

    if src_data != dst_data:
        print(f"Data mismatch for key '{key}': source data: '{src_data}' destination data: '{dst_data}'")

def compare_hash(src_cli, dst_cli, key):
    src_data = src_cli.hgetall(key)
    dst_data = dst_cli.hgetall(key)

    if src_data != dst_data:
        print(f"Data mismatch for key '{key}': source data: '{src_data}' destination data: '{dst_data}'")

def compare_list(src_cli, dst_cli, key):
    src_data = src_cli.lrange(key, 0, -1)
    dst_data = dst_cli.lrange(key, 0, -1)

    if src_data != dst_data:
        print(f"Data mismatch for key '{key}': source data: '{src_data}' destination data: '{dst_data}'")

def compare_set(src_cli, dst_cli, key):
    src_data = src_cli.smembers(key)
    dst_data = dst_cli.smembers(key)

    if src_data != dst_data:
        print(f"Data mismatch for key '{key}': source data: '{src_data}' destination data: '{dst_data}'")

def compare_zset(src_cli, dst_cli, key):
    src_data = src_cli.zrange(key, 0, -1, withscores=True)
    dst_data = dst_cli.zrange(key, 0, -1, withscores=True)

    if src_data != dst_data:
        print(f"Data mismatch for key '{key}': source data: '{src_data}' destination data: '{dst_data}'")

def compare_bitmap(src_cli, dst_cli, key, pos):
    src_data = src_cli.getbit(key, pos)
    dst_data = dst_cli.getbit(key, pos)

    if src_data != dst_data:
        print(f"Data mismatch for key '{key}': source data: '{src_data}' destination data: '{dst_data}'")

def compare_none(src_cli, dst_cli, key):
    return dst_cli.type(key) == 'none'

def compare_data(src_cli, dst_cli, key, data_type):
    if data_type == "string":
        compare_string(src_cli, dst_cli, key[0])
    elif data_type == "hash":
        compare_hash(src_cli, dst_cli, key[0])
    elif data_type == "list":
        compare_list(src_cli, dst_cli, key[0])
    elif data_type == "set":
        compare_set(src_cli, dst_cli, key[0])
    elif data_type == "zset":
        compare_zset(src_cli, dst_cli, key[0])
    elif data_type == 'bitmap':
        compare_bitmap(src_cli, dst_cli, key[0], key[1])
    # deleted
    elif data_type == 'none':
        compare_none(src_cli, dst_cli, key[0])
    else:
        print(f"Unsupported data type '{data_type}' for key '{key}'")


if __name__ == "__main__":
    args = parse_args()
    src_cli = get_redis_client(args.src_host, args.src_port, args.src_password)
    dst_cli = get_redis_client(args.dst_host, args.dst_port, args.dst_password)

    with open('user_key.log', 'rb') as f:
        for line in f:
            key = codecs.decode(line.strip()).split('-')
            data_type = src_cli.type(key[0])
            compare_data(src_cli, dst_cli, key, data_type)