import mock
import random
import contextlib
import shutil
import tempfile

import numpy as np
import delegator
import sqlitedict

import pytest
from hypothesis import given, example, settings
from hypothesis.strategies import composite, integers, text, floats, tuples, lists, sampled_from, one_of, dictionaries

from cache import *


txt = text(alphabet='abcdefgh_', min_size=1)
anything = one_of(txt, floats(allow_nan=False), integers(), lists(integers()), dictionaries(txt, floats(allow_nan=True)), dictionaries(txt, dictionaries(txt, txt)), )


class C(CacheMixin):
    d = {}
    m = mock.MagicMock(spec=dict)

    def __getitem__(self, key):
        self.m.__getitem__(key)
        return self.d.__getitem__(key)

    def __setitem__(self, key, val):
        self.m.__setitem__(key, val)
        self.d.__setitem__(key, val)

    def __contains__(self, key):
        self.m.__contains__(key)
        return self.d.__contains__(key)


@composite
def gen_fgakc(draw):
    f = mock.MagicMock()
    g = mock.MagicMock()
    f.__name__, g.__name__ = draw(lists(txt, min_size=2, max_size=2, unique=True))
    f.__repr__ = lambda x: "f: '{}'".format(f.__name__)
    g.__repr__ = lambda x: "g: '{}'".format(g.__name__)
    f.side_effect = lambda *args, **kwargs: f.call_count
    g.side_effect = lambda *args, **kwargs: g.call_count * -1
    a = tuple(draw(lists(anything, min_size=0, max_size=2)))
    k = draw(dictionaries(txt, anything, min_size=0, max_size=2))
    c = C()
    c.d = {}
    print(f, g, a, k, c.d)
    return f, g, a, k, c


@given(fgakc=gen_fgakc())
def test_call(fgakc):
    f, g, a, k, cache = fgakc
    cf, cg = cache(f), cache(g)
    r1, r3 = cf(*a, **k), cg(*a, **k)
    r2, r4 = cf(*a, **k), cg(*a, **k)
    assert f.call_count == 1
    assert g.call_count == 1
    assert f.call_args == (a, k)
    assert g.call_args == (a, k)
    assert r1 == r2
    assert r3 == r4
    assert r2 != r3


@pytest.mark.skip
def test_callable():
    g = np.random.random
    f = lambda g: g()

    cache = C()
    cf = cache(f)
    _ = cf(g)
    _ = cf(g)
    keys = cache.d.keys()
    assert len(keys) == 1


objs = [
    "np.sin",
    "(np.sin, np.cos)",
    "[np.sin, np.cos]",
    "{'sin': np.sin}",
]

@pytest.mark.parametrize("obj", objs)
def test_callable_different_processes(obj):
    cmd = "python -c 'import numpy as np; from cache import *; print(deep_hash({}))'".format(obj)

    c = delegator.run(cmd)
    d = delegator.run(cmd)

    assert c.out == d.out


@contextlib.contextmanager
def cd(newdir, cleanup=lambda: True):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)
        cleanup()

@contextlib.contextmanager
def tempdir():
    dirpath = tempfile.mkdtemp()
    def cleanup():
        shutil.rmtree(dirpath)
    with cd(dirpath, cleanup):
        yield dirpath


class Test_DBCache():
    def test_no_pure(self):
        with tempdir() as temp_dir:
            c = DBCache(path=temp_dir)
            f = lambda x: random.random()
            f = c(f)
            x = f(10)
            for i in range(100):
                assert x == f(10)


    def test_flush(self):
        with tempdir() as temp_dir:
            c = DBCache(path=temp_dir)
            path = c.path
            tabname = c.tabname
            f = c(lambda x: x**2)
            f(1)
            # need to use the original address for the key
            key = c.key(f.__closure__[0].cell_contents, (1,), {})
            del f
            del c
            try:
                c
            except NameError:
                pass
            else:
                assert False
            db = sqlitedict.SqliteDict(filename=path, tablename=tabname)
            assert db[key] == 1

    def test_overwrite(self):
        with tempdir() as temp_dir:
            c = DBCache(path=temp_dir, overwrite=True, buffer_size=1)

            a = iter([1, 2])
            @c
            def f():
                return next(a)

            f()
            key = c.key(f.__closure__[0].cell_contents, (), {})
            assert c.db[key] == 1
            f()
            assert c.db[key] == 2
