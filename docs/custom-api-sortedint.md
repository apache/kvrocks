# Custom api sortedint
* The following example demostrate how to use sortedint to paginate id list
```
redis> SIADD mysi 1
(integer) 1
redis> SIADD mysi 2
(integer) 1
redis> SIADD mysi 2
(integer) 0
redis> SIADD mysi 3 4 5 123 245
(integer) 5
redis> SICARD mysi
(integer) 7
redis> SIREVRANGE mysi 0 3
1) 245
2) 123
3) 5
redis> SIREVRANGE mysi 0 3 cursor 5
1) 4
2) 3
3) 2
redis> SIRANGE mysi 0 3 cursor 123
1) 245
redis> SIRANGEBYVALUE mysi 1 (5
1) "1"
2) "2"
3) "3"
4) "4"
redis> SIREVRANGEBYVALUE mysi 5 (1
1) "5"
2) "4"
3) "3"
4) "2"
redis> SIEXISTS mysi 1 88 2
1) 1
2) 0
3) 1
redis> SIREM mysi 2
(integer) 1
redis>
```
* siexists key member1 (member2 ...)
```
Return value
Array reply: list of integers at the specified keys, specifically:
    1 if the key exists.
    0 if the key does not exist.
```
```
* sirangebyvalue key min max (LIMIT offset count)
```
like zrangebyscore.
```
```
* sirevrangebyvalue key max min (LIMIT offset count)
```
like zrevrangebyscore.
```