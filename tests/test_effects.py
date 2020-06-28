import pytest

import tapystry as tap


def test_join():
    def ret(value):
        yield tap.Send('key', value)
        return value

    def fn():
        t = yield tap.CallFork(ret, 5)
        results = yield tap.Join(t)
        return results

    assert tap.run(fn) == 5


def test_join_dict():
    def ret(value):
        yield tap.Send('key', value)
        return value

    def fn():
        t = yield tap.CallFork(ret, 5)
        t2 = yield tap.CallFork(ret, 6)
        t3 = yield tap.CallFork(ret, 7)
        results = yield tap.Join(dict(
            a=t,
            bs=[t2, t3],
        ))
        return results

    assert tap.run(fn) == dict(
        a=5,
        bs=[6, 7]
    )


def test_fork():
    def ret(value):
        yield tap.Send('key', value)
        return value

    def fn():
        t = (yield tap.Fork([
            tap.Call(ret, 5),
            tap.Call(ret, 6),
        ]))
        results = yield tap.Join(t)
        return results

    assert tap.run(fn) == [5, 6]


def test_race():
    def recv1():
        yield tap.Receive('key1')
        return 1

    def recv2():
        yield tap.Receive('key2')
        return 2

    def send():
        yield tap.Send("key2")

    def fn():
        t = yield tap.Fork(tap.Race([
            tap.Call(recv1),
            tap.Call(recv2),
        ]))
        yield tap.Call(send)
        results = yield tap.Join(t)
        return results

    assert tap.run(fn) == (1, 2)


# TODO:
# test nested cancel
