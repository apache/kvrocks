import os
import sys

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,'../')

def assert_raise(exception_cls, callable, *args, **kwargs):
    try:
        callable(*args, **kwargs)
    except exception_cls as e:
        return e
    except Exception as e:
        assert False, 'assert_raises %s but raised: %s' % (exception_cls, e)
    assert False, 'assert_raises %s but nothing raise' % (exception_cls)
