import time
import pytest

import tapystry as tap


def test_simple():
    def fn():
        yield tap.Broadcast('key')
        return 5

    assert tap.run(fn) == 5


def test_receive():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver():
        value = yield tap.Receive('key')
        return value

    def fn():
        recv_strand = yield tap.CallFork(receiver)
        # even though this is forked, it doesn't end up hanging
        yield tap.CallFork(broadcaster, (5,))
        value = yield tap.Join(recv_strand)
        # join again should give the same thing, it's already done
        value1 = yield tap.Join(recv_strand)
        assert value1 == value
        return value

    assert tap.run(fn) == 5


def test_broadcast_receive_order():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver():
        value = yield tap.Receive('key')
        return value

    def fn():
        # fork in apparently wrong order!
        broadcast_strand = yield tap.CallFork(broadcaster, (5,))
        recv_strand = yield tap.CallFork(receiver)
        yield tap.Join(broadcast_strand)
        value = yield tap.Join(recv_strand)
        return value

    assert tap.run(fn) == 5



def test_never_receive():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver():
        value = yield tap.Receive('key2')
        return value

    def fn():
        recv_strand = yield tap.CallFork(receiver)
        broadcast_strand = yield tap.CallFork(broadcaster, (5,))
        yield tap.Join(broadcast_strand)
        value = yield tap.Join(recv_strand)
        return value

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    assert str(x.value).startswith("Hanging strands")


def test_bad_yield():
    def fn():
        yield 3

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    assert str(x.value).startswith("Strand yielded non-effect")


def test_immediate_return():
    def fn():
        if False:
            yield
        return 3

    assert tap.run(fn) == 3


def test_never_join():
    def broadcaster(value):
        yield tap.Broadcast('key', value)
        yield tap.Broadcast('key2', value)

    def fn():
        yield tap.CallFork(broadcaster, (5,))
        return

    assert tap.run(fn) is None


def test_no_arg():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def fn():
        yield tap.CallFork(broadcaster)
        return

    with pytest.raises(TypeError):
        tap.run(fn)


def test_call():
    def random(value):
        yield tap.Broadcast('key', value)
        return 10

    def fn():
        x = yield tap.Call(random, (5,))
        return x

    assert tap.run(fn) == 10


def test_call_trivial():
    def random(value):
        return 10

    def fn():
        x = yield tap.Call(random, (5,))
        return x

    def fork_fn():
        strand = yield tap.CallFork(random, (5,))
        x = yield tap.Join(strand)
        return x

    assert tap.run(fn) == 10
    assert tap.run(fork_fn) == 10
    assert tap.run(random, args=(5,)) == 10


def test_cancel():
    a = 0
    def add_three(value):
        nonlocal a
        yield tap.Receive('key')
        a += 5
        yield tap.Receive('key')
        a += 5
        yield tap.Receive('key')
        a += 5
        return 10

    def fn():
        strand = yield tap.CallFork(add_three, (5,))
        yield tap.Broadcast('key')
        yield tap.Broadcast('key')
        yield tap.Cancel(strand)

    tap.run(fn)
    assert a == 10


def test_multifirst():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver(wait_value):
        value = yield tap.Receive('key', lambda x: x == wait_value)
        return value

    def fn():
        strand_1 = yield tap.CallFork(receiver, (1,))
        strand_2 = yield tap.CallFork(receiver, (2,))
        strand_3 = yield tap.CallFork(receiver, (3,))
        results = yield tap.Fork([
            tap.First([strand_1, strand_2, strand_3]),
            tap.First([strand_2, strand_1]),
        ])
        yield tap.Call(broadcaster, (5,))
        yield tap.Call(broadcaster, (3,))
        yield tap.Call(broadcaster, (1,))
        value = yield tap.Join(results)
        return value

    # the first race resolves first, thus cancelling strands 1 and 2, preventing the second from ever finishing
    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    assert str(x.value).startswith("Hanging strands")


def test_multifirst_again():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver(wait_value):
        value = yield tap.Receive('key', lambda x: x == wait_value)
        return value

    def fn():
        strand_1 = yield tap.CallFork(receiver, (1,))
        strand_2 = yield tap.CallFork(receiver, (2,))
        strand_3 = yield tap.CallFork(receiver, (3,))
        results = yield tap.Fork([
            tap.First([strand_1, strand_2], name="1v2"),
            tap.First([strand_2, strand_3], name="2v3"),
        ])
        yield tap.Call(broadcaster, (5,))
        yield tap.Call(broadcaster, (1,))
        yield tap.Call(broadcaster, (3,))
        value = yield tap.Join(results, name="joinfork")
        # yield tap.Join([strand_1, strand_2, strand_3], name="joinstrands")
        return value

    assert tap.run(fn) == [
        (0, 1),
        (1, 3),
    ]


def test_multifirst_canceled():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver(wait_value):
        value = yield tap.Receive('key', lambda x: x == wait_value)
        return value

    def fn():
        strand_1 = yield tap.CallFork(receiver, (1,))
        strand_2 = yield tap.CallFork(receiver, (2,))
        strand_3 = yield tap.CallFork(receiver, (3,))
        results = yield tap.Fork([
            tap.First([strand_1, strand_2], name="1v2"),
            tap.First([strand_2, strand_3], name="2v3"),
        ])
        yield tap.Call(broadcaster, (5,))
        yield tap.Call(broadcaster, (1,))
        yield tap.Call(broadcaster, (3,))
        value = yield tap.Join(results, name="joinfork")
        yield tap.Join(strand_2, name="joincanceled")
        return value

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    # TODO: always do the child strand
    assert str(x.value).startswith("Hanging strands detected waiting for Race(Join(joincanceled))") or str(x.value).startswith("Hanging strands detected waiting for Join")


def test_multifirst_no_cancel():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver(wait_value):
        value = yield tap.Receive('key', lambda x: x == wait_value)
        return value

    def fn():
        strand_1 = yield tap.CallFork(receiver, (1,))
        strand_2 = yield tap.CallFork(receiver, (2,))
        strand_3 = yield tap.CallFork(receiver, (3,))
        results = yield tap.Fork([
            tap.First([strand_1, strand_2], name="1v2", cancel_losers=False),
            tap.First([strand_2, strand_3], name="2v3", cancel_losers=False),
        ])
        yield tap.Call(broadcaster, (5,))
        yield tap.Call(broadcaster, (1,))
        yield tap.Call(broadcaster, (3,))
        value = yield tap.Join(results, name="joinfork")
        yield tap.Call(broadcaster, (2,))
        yield tap.Join(strand_2, name="joincanceled")
        return value

    assert tap.run(fn) == [
        (0, 1),
        (1, 3),
    ]


def test_yield_from():
    def fn1(value):
        yield tap.Broadcast('key1', value)
        yield tap.Broadcast('key2', value)
        return 1

    def fn2(value):
        yield tap.Broadcast('key3', value)
        yield tap.Broadcast('key4', value)
        return 2

    def fn():
        v1 = yield from fn1(3)
        v2 = yield from fn2(4)
        assert v1 == 1
        assert v2 == 2

    tap.run(fn)


def test_sleep():
    def fn():
        t = time.time()
        yield tap.Sleep(0.01)
        assert time.time() - t > 0.01
        t = time.time()
        yield tap.Sleep(0)
        assert time.time() - t < 0.01

    tap.run(fn)


def test_intercept_nontest():
    def fn():
        yield tap.Intercept()

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    assert str(x.value).startswith("Cannot intercept outside of test mode")


def test_intercept():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver():
        value = yield tap.Receive('key')
        return value

    def intercepter():
        (effect, inject) = yield tap.Intercept(lambda e: isinstance(e, tap.Receive))
        assert effect.key == "key"
        yield inject("real")

    def fn():
        intercept_strand = yield tap.CallFork(intercepter)
        recv_strand = yield tap.CallFork(receiver)
        # even though this is forked, it doesn't end up hanging
        broadcast_strand = yield tap.CallFork(broadcaster, (5,))
        value = yield tap.Join(recv_strand)
        assert value == "real"  # got intercepted
        yield tap.Join(intercept_strand)
        yield tap.Cancel(broadcast_strand)
        return value

    assert tap.run(fn, test_mode=True) == "real"  # got intercepted


def test_intercept_cancel_too_late():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver():
        value = yield tap.Receive('key')
        return value

    def intercepter():
        (effect, inject) = yield tap.Intercept(lambda e: isinstance(e, tap.Receive))
        assert effect.key == "key"
        yield inject("real")

    def fn():
        intercept_strand = yield tap.CallFork(intercepter)
        yield tap.Cancel(intercept_strand)
        recv_strand = yield tap.CallFork(receiver)
        # even though this is forked, it doesn't end up hanging
        yield tap.Call(broadcaster, (5,))
        # this gets intercepted and never gets injected
        yield tap.Join(recv_strand)

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn, test_mode=True)
    # print(x.value)
    assert str(x.value).startswith("Hanging strands detected waiting for Race(Join") or str(x.value).startswith("Hanging strands detected waiting for Join") or str(x.value).startswith("Hanging strands detected waiting for Receive")


def test_intercept_cancel():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver():
        value = yield tap.Receive('key')
        return value

    def intercepter():
        (effect, inject) = yield tap.Intercept(lambda e: False)
        (effect, inject) = yield tap.Intercept(lambda e: isinstance(e, tap.Receive))
        assert effect.key == "key"
        yield inject("real")

    def fn():
        intercept_strand = yield tap.CallFork(intercepter)
        yield tap.Cancel(intercept_strand)
        recv_strand = yield tap.CallFork(receiver)
        # even though this is forked, it doesn't end up hanging
        yield tap.Call(broadcaster, (5,))
        value = yield tap.Join(recv_strand)
        assert value == 5  # did *not* get intercepted
        return value

    assert tap.run(fn, test_mode=True) == 5  # got intercepted


def test_error_stack():
    def broadcaster(value):
        yield tap.Broadcast('key', value)

    def receiver():
        value = yield tap.Receive('key')
        if value < 10:
            broadcast_strand = yield tap.CallFork(broadcaster, (value + 1,))
            receive_strand = yield tap.CallFork(receiver)
            yield tap.Join([broadcast_strand, receive_strand])
        raise Exception("too large")
        return value

    def fn():
        # fork in apparently wrong order!
        broadcast_strand = yield tap.CallFork(broadcaster, (5,))
        recv_strand = yield tap.CallFork(receiver)
        yield tap.Join(broadcast_strand)
        value = yield tap.Join(recv_strand)
        return value

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    # print(x.value)
    assert str(x.value).startswith("Exception caught at")
    assert str(x.value).count(", in receiver\n") == 6


def test_tricky_cancel():
    def wake_and_fork(
    ):
        yield tap.Fork(tap.Broadcast("wake"))
        yield tap.Fork(tap.Receive("hmm"))

    def fn():
        task = yield tap.CallFork(wake_and_fork)
        yield tap.Receive("wake")
        yield tap.Cancel(task)

    tap.run(fn)


