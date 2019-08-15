import redis

range=100
factor=32
port=6666

r = redis.StrictRedis(host='localhost', port=port, db=0, password='foobared')

# string
rst = r.set('foo', 2)  # update old
assert rst
rst = r.set('foo2', 2)  # add new
assert rst

rst = r.setex('foo_ex', 7200, 2)
assert rst

# zset
rst = r.zadd('zfoo', 4, 'd')
assert(rst == 1)
rst = r.zrem('zfoo', 'd')
assert(rst == 1)

# list
rst = r.lset('lfoo', 0, 'a')
assert(rst == 1)
rst = r.rpush('lfoo', 'a')
assert(rst == 5)
rst = r.lpush('lfoo', 'b')
assert(rst == 6)
rst = r.lpop('lfoo')
assert(rst == 'b')
rst = r.rpop('lfoo')
assert(rst == 'a')
rst = r.ltrim('lfoo', 0, 2)
assert rst

# set
rst = r.sadd('sfoo', 'f')
assert(rst == 1)
rst = r.srem('sfoo', 'f')
assert(rst == 1)

# hash
rst = r.hset('hfoo', 'b', 2)
assert(rst == 1)
rst = r.hdel('hfoo', 'b')
assert(rst == 1)

# bitmap
rst = r.setbit('bfoo', 0, 0)  # update old
assert(rst == 1)
rst = r.setbit('bfoo', 900000, 1)  # add new
assert(rst == 0)

# expire cmd
rst = r.expire('foo', 7200)
assert rst
rst = r.expire('zfoo', 7200)
assert rst

# del cmd
rst = r.delete('foo')
assert rst
rst = r.delete('zfoo')
assert rst

