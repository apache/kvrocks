# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Copyright (c) 2006-2020, Salvatore Sanfilippo
# See bundled license file licenses/LICENSE.redis for details.

# This file is copied and modified from the Redis project,
# which started out as: https://github.com/redis/redis/blob/dbcc0a8/tests/unit/scripting.tcl

start_server {tags {"scripting"}} {
    test {EVAL - Does Lua interpreter replies to our requests?} {
        r eval {return 'hello'} 0
    } {hello}
    test {EVAL - Lua integer -> Redis protocol type conversion} {
        r eval {return 100.5} 0
    } {100}

    test {EVAL - Lua string -> Redis protocol type conversion} {
        r eval {return 'hello world'} 0
    } {hello world}

    test {EVAL - Lua true boolean -> Redis protocol type conversion} {
        r eval {return true} 0
    } {1}

    test {EVAL - Lua false boolean -> Redis protocol type conversion} {
        r eval {return false} 0
    } {}

    test {EVAL - Lua status code reply -> Redis protocol type conversion} {
        r eval {return {ok='fine'}} 0
    } {fine}

    test {EVAL - Lua error reply -> Redis protocol type conversion} {
        catch {
            r eval {return {err='this is an error'}} 0
        } e
        set _ $e
    } {this is an error}

    test {Script return recursive object} {
        r readraw 1
        set bulk_len [r eval {return "hello"} 0]
        set bulk [r read]
        r readraw 0
        assert_equal $bulk_len {$5}
        assert_equal $bulk hello
    } {}

    test {EVAL - Lua table -> Redis protocol type conversion} {
        r eval {return {1,2,3,'ciao',{1,2}}} 0
    } {1 2 3 ciao {1 2}}

    test {EVAL - Are the KEYS and ARGV arrays populated correctly?} {
        r eval {return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}} 2 a b c d
    } {a b c d}

    test {EVAL - is Lua able to call Redis API?} {
        r set mykey myval
        r eval {return redis.call('get',KEYS[1])} 1 mykey
    } {myval}

    test {EVALSHA - Can we call a SHA1 if already defined?} {
        r evalsha fd758d1589d044dd850a6f05d52f2eefd27f033f 1 mykey
    } {myval}

    test {EVALSHA - Can we call a SHA1 in uppercase?} {
        r evalsha FD758D1589D044DD850A6F05D52F2EEFD27F033F 1 mykey
    } {myval}

    test {EVALSHA - Do we get an error on invalid SHA1?} {
        catch {r evalsha NotValidShaSUM 0} e
        set _ $e
    } {ERR NOSCRIPT*}

    test {EVALSHA - Do we get an error on non defined SHA1?} {
        catch {r evalsha ffd632c7d33e571e9f24556ebed26c3479a87130 0} e
        set _ $e
    } {ERR NOSCRIPT*}

    test {EVAL - Redis integer -> Lua type conversion} {
        r set x 0
        r eval {
            local foo = redis.pcall('incr',KEYS[1])
            return {type(foo),foo}
        } 1 x
    } {number 1}

    test {EVAL - Redis bulk -> Lua type conversion} {
        r set mykey myval
        r eval {
            local foo = redis.pcall('get',KEYS[1])
            return {type(foo),foo}
        } 1 mykey
    } {string myval}

    test {EVAL - Redis integer -> Lua type conversion} {
        r set x 0
        r eval {
            local foo = redis.pcall('incr',KEYS[1])
            return {type(foo),foo}
        } 1 x
    } {number 1}

    test {EVAL - Redis bulk -> Lua type conversion} {
        r set mykey myval
        r eval {
            local foo = redis.pcall('get',KEYS[1])
            return {type(foo),foo}
        } 1 mykey
    } {string myval}
    test {EVAL - Redis multi bulk -> Lua type conversion} {
        r del mylist
        r rpush mylist a
        r rpush mylist b
        r rpush mylist c
        r eval {
            local foo = redis.pcall('lrange',KEYS[1],0,-1)
            return {type(foo),foo[1],foo[2],foo[3],# foo}
        } 1 mylist
    } {table a b c 3}

    test {EVAL - Redis status reply -> Lua type conversion} {
        r eval {
            local foo = redis.pcall('set',KEYS[1],'myval')
            return {type(foo),foo['ok']}
        } 1 mykey
    } {table OK}

    test {EVAL - Redis error reply -> Lua type conversion} {
        r set mykey myval
        r eval {
            local foo = redis.pcall('incr',KEYS[1])
            return {type(foo),foo['err']}
        } 1 mykey
    } {table {Invalid argument: value is not an integer or out of range}}
    test {EVAL - Redis nil bulk reply -> Lua type conversion} {
        r del mykey
        r eval {
            local foo = redis.pcall('get',KEYS[1])
            return {type(foo),foo == false}
        } 1 mykey
    } {boolean 1}
    test {EVAL - Scripts can't run certain commands} {
        set e {}
        catch {r eval {return redis.pcall('blpop','x',0)} 0} e
        set e
    } {*not allowed*}

    test {EVAL - Scripts can't run certain commands} {
        set e {}
        catch {
            r eval "redis.pcall('randomkey'); return redis.pcall('set','x','ciao')" 0
        } e
        set e
    } {OK}

    test {EVAL - No arguments to redis.call/pcall is considered an error} {
        set e {}
        catch {r eval {return redis.call()} 0} e
        set e
    } {*one argument*}

    test {EVAL - redis.call variant raises a Lua error on Redis cmd error (1)} {
        set e {}
        catch {
            r eval "redis.call('nosuchcommand')" 0
        } e
        set e
    } {*Unknown Redis*}
    test {EVAL - redis.call variant raises a Lua error on Redis cmd error (1)} {
        set e {}
        catch {
            r eval "redis.call('get','a','b','c')" 0
        } e
        set e
    } {*number of args*}

    test {EVAL - redis.call variant raises a Lua error on Redis cmd error (1)} {
        set e {}
        r set foo bar
        catch {
            r eval {redis.call('lpush',KEYS[1],'val')} 1 foo
        } e
        set e
    } {*against a key*}

    test {EVAL - JSON numeric decoding} {
        # We must return the table as a string because otherwise
        # Redis converts floats to ints and we get 0 and 1023 instead
        # of 0.0003 and 1023.2 as the parsed output.
        r eval {return
                 table.concat(
                   cjson.decode(
                    "[0.0, -5e3, -1, 0.3e-3, 1023.2, 0e10]"), " ")
        } 0
    } {0 -5000 -1 0.0003 1023.2 0}

    test {EVAL - JSON string decoding} {
        r eval {local decoded = cjson.decode('{"keya": "a", "keyb": "b"}')
                return {decoded.keya, decoded.keyb}
        } 0
    } {a b}

    test {EVAL - cmsgpack can pack double?} {
        r eval {local encoded = cmsgpack.pack(0.1)
                local h = ""
                for i = 1, #encoded do
                    h = h .. string.format("%02x",string.byte(encoded,i))
                end
                return h
        } 0
    } {cb3fb999999999999a}

    test {EVAL - cmsgpack can pack negative int64?} {
        r eval {local encoded = cmsgpack.pack(-1099511627776)
                local h = ""
                for i = 1, #encoded do
                    h = h .. string.format("%02x",string.byte(encoded,i))
                end
                return h
        } 0
    } {d3ffffff0000000000}
    test {EVAL - cmsgpack can pack and unpack circular references?} {
        r eval {local a = {x=nil,y=5}
                local b = {x=a}
                a['x'] = b
                local encoded = cmsgpack.pack(a)
                local h = ""
                -- cmsgpack encodes to a depth of 16, but can't encode
                -- references, so the encoded object has a deep copy recursive
                -- depth of 16.
                for i = 1, #encoded do
                    h = h .. string.format("%02x",string.byte(encoded,i))
                end
                -- when unpacked, re.x.x != re because the unpack creates
                -- individual tables down to a depth of 16.
                -- (that's why the encoded output is so large)
                local re = cmsgpack.unpack(encoded)
                assert(re)
                assert(re.x)
                assert(re.x.x.y == re.y)
                assert(re.x.x.x.x.y == re.y)
                assert(re.x.x.x.x.x.x.y == re.y)
                assert(re.x.x.x.x.x.x.x.x.x.x.y == re.y)
                -- maximum working depth:
                assert(re.x.x.x.x.x.x.x.x.x.x.x.x.x.x.y == re.y)
                -- now the last x would be b above and has no y
                assert(re.x.x.x.x.x.x.x.x.x.x.x.x.x.x.x)
                -- so, the final x.x is at the depth limit and was assigned nil
                assert(re.x.x.x.x.x.x.x.x.x.x.x.x.x.x.x.x == nil)
                assert(h == "82a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a178c0a17905a17905a17905a17905a17905a17905a17905a17905" or h == "82a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a178c0")
                return {re.x.x.x.x.x.x.x.x.y == re.y, re.y == 5}
        } 0
    } {1 1}
    test {EVAL - Numerical sanity check from bitop} {
        r eval {assert(0x7fffffff == 2147483647, "broken hex literals");
                assert(0xffffffff == -1 or 0xffffffff == 2^32-1,
                    "broken hex literals");
                assert(tostring(-1) == "-1", "broken tostring()");
                assert(tostring(0xffffffff) == "-1" or
                    tostring(0xffffffff) == "4294967295",
                    "broken tostring()")
        } 0
    } {}

    test {EVAL - Verify minimal bitop functionality} {
        r eval {assert(bit.tobit(1) == 1);
                assert(bit.band(1) == 1);
                assert(bit.bxor(1,2) == 3);
                assert(bit.bor(1,2,4,8,16,32,64,128) == 255)
        } 0
    } {}

    test {EVAL - Able to parse trailing comments} {
        r eval {return 'hello' --trailing comment} 0
    } {hello}

    test {EVAL does not leak in the Lua stack} {
        r set x 0
        # Use a non blocking client to speedup the loop.
        set rd [redis_deferring_client]
        for {set j 0} {$j < 10000} {incr j} {
            $rd eval {return redis.call("incr",KEYS[1])} 1 x
        }
        for {set j 0} {$j < 10000} {incr j} {
            $rd read
        }
        assert {[s used_memory_lua] < 1024*100}
        $rd close
        r get x
    } {10000}

    test {SCRIPTING FLUSH - is able to clear the scripts cache?} {
        r set mykey myval
        set v [r evalsha fd758d1589d044dd850a6f05d52f2eefd27f033f 1 mykey]
        assert_equal $v myval
        set e ""
        r script flush
        catch {r evalsha fd758d1589d044dd850a6f05d52f2eefd27f033f 1 mykey} e
        set e
    } {ERR NOSCRIPT*}

    test {SCRIPT EXISTS - can detect already defined scripts?} {
        r eval "return 1+1" 0
        r script exists a27e7e8a43702b7046d4f6a7ccf5b60cef6b9bd9 a27e7e8a43702b7046d4f6a7ccf5b60cef6b9bda
    } {1 0}

    test {SCRIPT LOAD - is able to register scripts in the scripting cache} {
        list \
            [r script load "return 'loaded'"] \
            [r evalsha b534286061d4b9e4026607613b95c06c06015ae8 0]
    } {b534286061d4b9e4026607613b95c06c06015ae8 loaded}

    test {Globals protection reading an undeclared global variable} {
        catch {r eval {return a} 0} e
        set e
    } {*ERR*attempted to access * global*}

    test {Globals protection setting an undeclared global*} {
        catch {r eval {a=10} 0} e
        set e
    } {*ERR*attempted to create global*}

    test {Test an example script DECR_IF_GT} {
        set decr_if_gt {
            local current

            current = redis.call('get',KEYS[1])
            if not current then return nil end
            if current > ARGV[1] then
                return redis.call('decr',KEYS[1])
            else
                return redis.call('get',KEYS[1])
            end
        }
        r set foo 5
        set res {}
        lappend res [r eval $decr_if_gt 1 foo 2]
        lappend res [r eval $decr_if_gt 1 foo 2]
        lappend res [r eval $decr_if_gt 1 foo 2]
        lappend res [r eval $decr_if_gt 1 foo 2]
        lappend res [r eval $decr_if_gt 1 foo 2]
        set res
    } {4 3 2 2 2}

    test {Scripting engine PRNG can be seeded correctly} {
        set rand1 [r eval {
            math.randomseed(ARGV[1]); return tostring(math.random())
        } 0 10]
        set rand2 [r eval {
            math.randomseed(ARGV[1]); return tostring(math.random())
        } 0 10]
        set rand3 [r eval {
            math.randomseed(ARGV[1]); return tostring(math.random())
        } 0 20]
        assert_equal $rand1 $rand2
        assert {$rand2 ne $rand3}
    }
    test "In the context of Lua the output of random commands gets ordered" {
        r del myset
        r sadd myset a b c d e f g h i l m n o p q r s t u v z aa aaa azz
        r eval {return redis.call('smembers',KEYS[1])} 1 myset
    } {a aa aaa azz b c d e f g h i l m n o p q r s t u v z}

    test "Make sure redis.log() works" {
        set v [r eval { return redis.log(redis.LOG_DEBUG, 'debug level'); } 0]
        assert_equal "" $v
        set v [r eval { return redis.log(redis.LOG_VERBOSE, 'verbose level'); } 0]
        assert_equal "" $v
        set v [r eval { return redis.log(redis.LOG_NOTICE, 'notice level'); } 0]
        assert_equal "" $v
        set v [r eval { return redis.log(redis.LOG_WARNING, 'warning level'); } 0]
        assert_equal "" $v
    } {}
}

start_server {tags {"repl"}} {
    start_server {} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        $slave slaveof $master_host $master_port
        wait_for_sync $slave

        test {SCRIPTING: script load on master, read on slave} {
            set sha [$master script load "return 'script loaded'"]
            assert_equal 4167ea82ed9c381c7659f7cf93f394219147e8c4 $sha
            wait_for_ofs_sync $master $slave
            assert_equal 1 [$master script exists $sha]
            assert_equal 1 [$slave script exists $sha]

            $master script flush
            wait_for_ofs_sync $master $slave
            assert_equal 0 [$slave script exists $sha]
        }
    }
}
