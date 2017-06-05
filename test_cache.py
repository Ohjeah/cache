import mock
import random

import numpy as np
import delegator

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


caches = [
            lambda: DBCache("tmp", "mytable", buffer_size=1),
            lambda: FileCache("/tmp")
            ]

@pytest.mark.parametrize("cache", caches)
def test_Cache(cache):
    c = cache()

    f = lambda x: random.random()
    f = c(f)
    x = f(10)
    for i in range(100):
        assert x == f(10)

    delegator.run("rm {}".format(c.fname))
