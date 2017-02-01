import os
import functools

import dill


def _hash(thing):
    try:
        hsh = hash(dill.dumps(thing))
    except dill.PicklingError:
        raise RuntimeError
    return hsh

# Recipe for _make_key taken from
# http://code.activestate.com/recipes/578078-py26-and-py30-backport-of-python-33s-lru-cache/
# and modified

def _make_key(f, args, kwds, typed=False, kwd_mark=(object(),),
              fasttypes={int, str, frozenset, type(None)},
              sorted=sorted, tuple=tuple, type=type, len=len):
    """Make a cache key from the name of the function and
    optionally typed positional and keyword arguments"""
    key = [f.__name__]
    key += args
    if kwds:
        sorted_items = sorted(kwds.items())
        key += kwd_mark
        for item in sorted_items:
            key += item
    if typed:
        key += tuple(type(v) for v in args)
        if kwds:
            key += tuple(type(v) for k, v in sorted_items)
    elif len(key) == 1 and type(key[0]) in fasttypes:
        return key[0]
    return key


class CacheMixin(object):
    """To use this mixin, the following methods must be implemented:

    __getitem__
    __setitem__
    """

    def key(self, f, args, kwargs):
        return _hash(_make_key(f, args, kwargs))

    def __call__(self, f, ignore=None):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            key = self.key(f, args, kwargs)
            if key not in self:
                res = f(*args, **kwargs)
                self[key] = res
            return self[key]

        return wrapped

    def __contains__(self, key):
        try:
            self[key]
            return True
        except KeyError:
            return False


class FileCache(CacheMixin):


    def __init__(self, path):
        self.path = path
        self.fname = lambda key: os.path.join(self.path, str(hash(dill.dumps(key))) + ".dill")

    def __getitem__(self, key):
        file_ = self.fname(key)
        if not os.path.exists(file_):
            raise KeyError
        with open(file_, 'rb') as f:
            result = dill.load(f)
        return result

    def __setitem__(self, key, value):
        with open(self.fname(key), 'wb') as f:
            dill.dump(value, f)


class _Memoize(CacheMixin, dict):
    pass


def memoize(f):
    m =_Memoize()(f)
    return m
