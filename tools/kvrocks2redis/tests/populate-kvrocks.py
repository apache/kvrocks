import redis

range=100
factor=32
port=6666

r = redis.StrictRedis(host='localhost', port=port, db=0, password='foobared')

# flushall ?
# rst = r.flushall()
# assert rst

# string
rst = r.set('foo', 1)
assert rst

rst = r.setex('foo_ex', 3600, 1)
assert rst

# zset
rst = r.zadd('zfoo', 1, 'a', 2, 'b', 3, 'c')
assert(rst == 3)

# list
rst = r.rpush('lfoo', 1, 2, 3, 4)
assert(rst == 4)

# set
rst = r.sadd('sfoo', 'a', 'b', 'c', 'd')
assert(rst == 4)

# hash
rst = r.hset('hfoo', 'a', 1)
assert(rst == 1)

# bitmap
rst = r.setbit('bfoo', 0, 1)
assert(rst == 0)
rst = r.setbit('bfoo', 1, 1)
assert(rst == 0)
rst = r.setbit('bfoo', 800000, 1)
assert(rst == 0)

# expire cmd
rst = r.expire('foo', 3600)
assert rst
rst = r.expire('zfoo', 3600)
assert rst






