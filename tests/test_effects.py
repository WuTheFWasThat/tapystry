import pytest

import tapystry as tap


def test_join():
    def ret(value):
        yield tap.Broadcast('key', value)
        return value

    def fn():
        t = yield tap.CallFork(ret, (5,))
        results = yield tap.Join(t)
        return results

    assert tap.run(fn) == 5


def test_join_dict():
    def ret(value):
        yield tap.Broadcast('key', value)
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
        yield tap.Broadcast('key', value)
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

    def broadcast():
        yield tap.Broadcast("key2")

    def fn():
        t = yield tap.Fork(tap.Race([
            tap.Call(recv1),
            tap.Call(recv2),
        ]))
        yield tap.Call(broadcast)
        results = yield tap.Join(t)
        return results

    assert tap.run(fn) == (1, 2)


def test_race_dict():
    def broadcaster(value):
        yield tap.Broadcast('key', value)
        return "success"

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
        # broadcast a value to add
        success = yield tap.Call(broadcaster, (5,))
        assert success == "success"
        # equivalent syntax using yield from
        success = yield from broadcaster(8)
        assert success == "success"
        # forked process is not yet done
        assert not recv_strand.is_done()
        yield tap.Broadcast("exit")
        # this value won't get received
        success = yield tap.Call(broadcaster, (1,))
        assert success == "success"
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
            yield tap.Broadcast("key")
        # this should also cancel recv_inner
        yield tap.Cancel(t)
        for _ in range(4):
            yield tap.Broadcast("key")
        return a, b

    assert tap.run(fn) == (2, 4)


def test_subscribe():
    a = 0
    b = 0

    def recv_all(v):
        nonlocal a
        a += v
        yield tap.Receive("finish")

    def recv_leading(v):
        nonlocal b
        b += v
        yield tap.Receive("finish")

    def fn():
        ta = yield tap.Subscribe("key", recv_all, predicate=lambda x: x % 2 == 1)
        tb = yield tap.Subscribe("key", recv_leading, predicate=lambda x: x % 2 == 1, leading_only=True)

        for i in range(4):
            yield tap.Broadcast("key", i)
        yield tap.Sleep(0.001)  # TODO: fix this to join on the original broadcast?
        assert a == 1 + 3
        assert b == 1
        yield tap.Broadcast("finish")
        for i in range(4):
            yield tap.Broadcast("key", i)
        yield tap.Sleep(0.001)  # TODO: fix this to join on the original broadcast?
        assert a == (1 + 3) * 2
        assert b == 2

        ta.cancel()
        tb.cancel()
        yield tap.Broadcast("finish")
        for i in range(4):
            yield tap.Broadcast("key", i)
        yield tap.Sleep(0.001)  # TODO: fix this to join on the original broadcast?
        assert a == (1 + 3) * 2
        assert b == 2

    tap.run(fn)


def test_subscribe_latest():
    a = 0

    def recv_latest(v):
        yield tap.Receive("start")
        nonlocal a
        a += v

    def fn():
        t = yield tap.Subscribe("key", recv_latest, latest_only=True)

        yield tap.Broadcast("key", 3)
        yield tap.Broadcast("key", 5)
        yield tap.Broadcast("start")
        yield tap.Broadcast("start")
        yield tap.Sleep(0)
        assert a == 5

        yield tap.Broadcast("key", 3)
        yield tap.Broadcast("key", 5)
        yield tap.Broadcast("key", 1)
        yield tap.Broadcast("start")
        yield tap.Broadcast("start")
        yield tap.Sleep(0)
        assert a == 6
        t.cancel()

    tap.run(fn)


def test_subscribes_all():
    a = 0

    def recv_broadcast():
        yield tap.Receive("key")
        yield tap.Broadcast("key")

    def increment(_x):
        nonlocal a
        a += 1
        yield tap.Receive("finish")

    def fn():
        t1 = yield tap.CallFork(recv_broadcast)
        ta = yield tap.Subscribe("key", increment)
        t2 = yield tap.CallFork(recv_broadcast)

        yield tap.Broadcast("key", "main")
        yield tap.Join([t1, t2])
        yield tap.Sleep(0.001)  # TODO: fix this to join on the original broadcast?
        assert a == 3
        yield tap.Broadcast("key", "main2")
        yield tap.Sleep(0.001)  # TODO: fix this to join on the original broadcast?
        assert a == 4
        yield tap.Cancel(ta)

    tap.run(fn)
