import pytest

import tapystry as tap


def test_join():
    def ret(value):
        yield tap.Send('key', value)
        return value

    def fn():
        t = yield tap.CallFork(ret, (5,))
        results = yield tap.Join(t)
        return results

    assert tap.run(fn) == 5


def test_join_dict():
    def ret(value):
        yield tap.Send('key', value)
        return value

    def fn():
        t = yield tap.CallFork(ret, (5,))
        t2 = yield tap.CallFork(ret, (6,))
        t3 = yield tap.CallFork(ret, (7,))
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
            tap.Call(ret, (5,)),
            tap.Call(ret, (6,)),
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


def test_race_dict():
    def sender(value):
        yield tap.Send('key', value)
        return "sent"

    def incrementer():
        value = 0
        while True:
            winner, received_val = yield tap.Race(dict(
                receive=tap.Receive('key'),
                exit=tap.Receive('exit'),
            ))
            if winner == 'exit':
                break
            else:
                assert winner == 'receive'
                value += received_val
        return value

    def fn():
        # fork off a strand that increments
        recv_strand = yield tap.CallFork(incrementer)
        # send a value to add
        sent = yield tap.Call(sender, (5,))
        assert sent == "sent"
        # equivalent syntax using yield from
        sent = yield from sender(8)
        assert sent == "sent"
        # forked process is not yet done
        assert not recv_strand.is_done()
        yield tap.Send("exit")
        # this value won't get received
        sent = yield tap.Call(sender, (1,))
        assert sent == "sent"
        value = yield tap.Join(recv_strand)
        return value

    assert tap.run(fn) == 13


def test_nested_cancel():
    a = 0
    b = 0

    def recv_inner():
        nonlocal a
        while True:
            yield tap.Receive('key')
            yield tap.Receive('key')
            a += 1

    def recv_outer():
        nonlocal b
        yield tap.CallFork(recv_inner)
        while True:
            yield tap.Receive('key')
            b += 1

    def fn():
        t = yield tap.CallFork(recv_outer)
        for _ in range(4):
            yield tap.Send("key")
        # this should also cancel recv_inner
        t.cancel()
        for _ in range(4):
            yield tap.Send("key")
        return a, b

    assert tap.run(fn) == (2, 4)


def test_subscribe():
    a = 0
    b = 0

    def recv_all(v):
        nonlocal a
        a += v
        yield tap.Receive("unlock")

    def recv_latest(v):
        nonlocal b
        b += v
        yield tap.Receive("unlock")

    def fn():
        ta = yield tap.Subscribe("key", recv_all, predicate=lambda x: x % 2 == 1)
        tb = yield tap.Subscribe("key", recv_latest, predicate=lambda x: x % 2 == 1, leading_only=True)

        for i in range(4):
            yield tap.Send("key", i)
        yield tap.Sleep(0)
        assert a == 1 + 3
        assert b == 1
        yield tap.Send("unlock")
        for i in range(4):
            yield tap.Send("key", i)
        yield tap.Sleep(0)
        assert a == (1 + 3) * 2
        assert b == 2

        ta.cancel()
        tb.cancel()
        yield tap.Send("unlock")
        for i in range(4):
            yield tap.Send("key", i)
        yield tap.Sleep(0)
        assert a == (1 + 3) * 2
        assert b == 2

    tap.run(fn)


def test_subscribes_all():
    a = 0

    def recv_send():
        yield tap.Receive("key")
        yield tap.Send("key")

    def increment(_x):
        nonlocal a
        a += 1
        yield tap.Receive("unlock")

    def fn():
        yield tap.CallFork(recv_send)
        ta = yield tap.Subscribe("key", increment)
        yield tap.CallFork(recv_send)

        yield tap.Sleep(0)
        yield tap.Send("key", "main")
        yield tap.Sleep(0)
        assert a == 3
        yield tap.Sleep(0)
        yield tap.Send("key", "main2")
        yield tap.Sleep(0)
        assert a == 4
        ta.cancel()

    tap.run(fn)
