import mock

from hypothesis import given, example, settings
from hypothesis.strategies import composite, integers, text, floats, tuples, lists, sampled_from, one_of, dictionaries

from cache import *

txt = text(alphabet='abcdefgh_', min_size=1)
anything = one_of(txt, floats(allow_nan=False), integers(), lists(integers()), dictionaries(txt, floats(allow_nan=True)))


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
