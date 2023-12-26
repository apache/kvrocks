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
python3 populate-kvrocks.py
# check generated aof file & user_key.log file

# check consistency
python3 check_consistency.py
```
