## Populate data and append new data script

For testing the `kvrocks2redis` utility, manually check generate AOF.

## Usage

* Start `kvrocks` and `kvrocks2redis`
    * [ ] TODO automatic create docker env
* Install dependency::
    * pip install git+https://github.com/andymccurdy/redis-py.git@2.10.3
* Usage:

```bash
# populate data
python populate-kvrocks.py  
# check generated aof file 
# append new data 
python append-data-to-kvrocks.py
# check appended new aof data  
```
