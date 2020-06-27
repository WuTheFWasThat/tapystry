import pytest

import tapystry as tap


def test_parallel():
    def ret(value):
        yield tap.Send('key', value)
        return value

    def fn():
        t = (yield tap.Parallel([
            tap.Call(ret, 5),
            tap.Call(ret, 6),
        ]))
        results = yield tap.Join(t)
        return results

    assert tap.run(fn) == [5, 6]
