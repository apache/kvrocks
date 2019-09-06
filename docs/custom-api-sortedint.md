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
redis> SIREM mysi 2
(integer) 1
redis>
```