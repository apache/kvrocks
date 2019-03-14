## Populate data and append new data script 
* for testing the kvrocks2redis, manually check generate aof )

## Usage

* Start kvrocks and kvrocks2redis
    * TODO automatic create docker env
* install dependency::
    * pip install git+https://github.com/andymccurdy/redis-py.git@2.10.3   
* Usage 
```
# populate data
python populate-kvrocks.py  
# check generated aof file 
# append new data 
python append-data-to-kvrocks.py
# check appended new aof data  
