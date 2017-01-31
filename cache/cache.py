import inspect


class CacheMixin:
    def get(self, f, *args, **kwargs):
        kwargs.update(dict(zip(inspect.getargspec(f).args, args)))
        key = _hash(tuple(kwargs.get(k, None) for k in inspect.getargspec(f).args))
        if key not in self:
            res = f(**kwargs)
            self[key] = res
        return self[key]

    def __call__(self, f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return self.get(f, *args, **kwargs)
        return wrapped


class _Memoize(Cache, dict):
    pass


def memoize(f):
    m =_Memoize()(f)
    return m
