import os
import functools
import inspect
import logging
import collections

import sqlitedict
import dill
from joblib import hash


def _hash(x):
    try:
        if callable(x):
            return _hash(x.__name__)
        else:
            return hash(dill.dumps(x))
    except dill.PicklingError:
        raise RuntimeError


def deep_hash(x):
    if isinstance(x, dict):
        k = _hash([(k, deep_hash(v)) for v, k in sorted(x.items())])
        return k
    elif isinstance(x, (tuple, list, set)):
        return _hash([deep_hash(xi) for xi in x])
    else:
        return _hash(x)


def _make_key(f, args, kwargs):
    kwargs.update(dict(zip(inspect.getargspec(f).args, args)))
    key = tuple(kwargs.get(k, None) for k in inspect.getargspec(f).args) + (f.__name__, )
    return deep_hash(key)


class CacheMixin(object):
    """To use this mixin, the following methods must be implemented:

    __getitem__
    __setitem__
    """

    @staticmethod
    def key(f, args, kwargs):
        return _make_key(f, args, kwargs)

    def __call__(self, f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            key = self.key(f, args, kwargs.copy())
            if key not in self or getattr(self, "overwrite", False):
                res = f(*args, **kwargs)
                self[key] = res
                return res
            return self[key]
        return wrapped

    def __contains__(self, key):
        try:
            self[key]
            return True
        except KeyError:
            return False


class _Memoize(CacheMixin, dict):
    pass


def memoize(f):
    m =_Memoize()(f)
    return m


class DBCache(CacheMixin):
    """A dictionary based cache that periodically syncs with a sqlite data base."""
    def __init__(self, fname="dbcache", path="./tmp", tabname="mytable", buffer_size=5,
                 silence=True, overwrite=False):
        self.path = "{}.sqlite".format(os.path.join(path, fname))
        self.tabname = tabname
        self.overwrite = overwrite
        self._buffer = {}
        self.buffer_size = buffer_size

        if silence:
            logging.getLogger("sqlitedict").setLevel(logging.WARNING)

        if not self.overwrite:
            with self.db as db:
                self._d = {k:v for k,v in db.items()}
        else:
            self._d = {}
        self._view = collections.ChainMap(self._buffer, self._d)

    @property
    def db(self):
        return sqlitedict.SqliteDict(filename=self.path, tablename=self.tabname)

    def __getitem__(self, key):
        return self._view[key]

    def __setitem__(self, key, value):
        self._buffer[key] = value
        if len(self._buffer) >= self.buffer_size:
            self.flush()

    def flush(self):
        """Save current buffer to database"""
        with self.db as db:
            db.update(self._buffer)
            db.commit()
        self._d.update(self._buffer)
        self._buffer.clear()

    def __del__(self):
        """Flush current buffer before carbage collection"""
        try:
            self.flush()
        except RuntimeError:
            pass # DB not available anymore
