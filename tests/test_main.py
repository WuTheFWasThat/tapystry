import time
import pytest

import tapystry as tap


def test_simple():
    def fn():
        yield tap.Send('key')
        return 5

    assert tap.run(fn) == 5


def test_receive():
    def sender(value):
        yield tap.Send('key', value)

    def receiver():
        value = yield tap.Receive('key')
        return value

    def fn():
        recv_strand = yield tap.CallFork(receiver)
        # even though this is forked, it doesn't end up hanging
        yield tap.CallFork(sender, (5,))
        value = yield tap.Join(recv_strand)
        # join again should give the same thing, it's already done
        value1 = yield tap.Join(recv_strand)
        assert value1 == value
        return value

    assert tap.run(fn) == 5


def test_send_receive_order():
    def sender(value):
        yield tap.Send('key', value)

    def receiver():
        value = yield tap.Receive('key')
        return value

    def fn():
        # fork in apparently wrong order!
        send_strand = yield tap.CallFork(sender, (5,))
        recv_strand = yield tap.CallFork(receiver)
        yield tap.Join(send_strand)
        value = yield tap.Join(recv_strand)
        return value

    assert tap.run(fn) == 5



def test_never_receive():
    def sender(value):
        yield tap.Send('key', value)

    def receiver():
        value = yield tap.Receive('key2')
        return value

    def fn():
        recv_strand = yield tap.CallFork(receiver)
        send_strand = yield tap.CallFork(sender, (5,))
        yield tap.Join(send_strand)
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
    def sender(value):
        yield tap.Send('key', value)
        yield tap.Send('key2', value)

    def fn():
        yield tap.CallFork(sender, (5,))
        return

    assert tap.run(fn) is None


def test_no_arg():
    def sender(value):
        yield tap.Send('key', value)

    def fn():
        yield tap.CallFork(sender)
        return

    with pytest.raises(TypeError):
        tap.run(fn)


def test_call():
    def random(value):
        yield tap.Send('key', value)
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
        yield tap.Send('key')
        yield tap.Send('key')
        yield tap.Cancel(strand)

    tap.run(fn)
    assert a == 10


def test_multifirst():
    def sender(value):
        yield tap.Send('key', value)

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
        yield tap.Call(sender, (5,))
        yield tap.Call(sender, (3,))
        yield tap.Call(sender, (1,))
        value = yield tap.Join(results)
        return value

    # the first race resolves first, thus cancelling strands 1 and 2, preventing the second from ever finishing
    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    assert str(x.value).startswith("Hanging strands")


def test_multifirst_again():
    def sender(value):
        yield tap.Send('key', value)

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
        yield tap.Call(sender, (5,))
        yield tap.Call(sender, (1,))
        yield tap.Call(sender, (3,))
        value = yield tap.Join(results, name="joinfork")
        # yield tap.Join([strand_1, strand_2, strand_3], name="joinstrands")
        return value

    assert tap.run(fn) == [
        (0, 1),
        (1, 3),
    ]


def test_multifirst_canceled():
    def sender(value):
        yield tap.Send('key', value)

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
        yield tap.Call(sender, (5,))
        yield tap.Call(sender, (1,))
        yield tap.Call(sender, (3,))
        value = yield tap.Join(results, name="joinfork")
        yield tap.Join(strand_2, name="joincanceled")
        return value

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    # TODO: always do the child strand
    assert str(x.value).startswith("Hanging strands detected waiting for Race(Join(joincanceled))") or str(x.value).startswith("Hanging strands detected waiting for Call(join)")


def test_multifirst_no_cancel():
    def sender(value):
        yield tap.Send('key', value)

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
        yield tap.Call(sender, (5,))
        yield tap.Call(sender, (1,))
        yield tap.Call(sender, (3,))
        value = yield tap.Join(results, name="joinfork")
        yield tap.Call(sender, (2,))
        yield tap.Join(strand_2, name="joincanceled")
        return value

    assert tap.run(fn) == [
        (0, 1),
        (1, 3),
    ]


def test_yield_from():
    def fn1(value):
        yield tap.Send('key1', value)
        yield tap.Send('key2', value)
        return 1

    def fn2(value):
        yield tap.Send('key3', value)
        yield tap.Send('key4', value)
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
