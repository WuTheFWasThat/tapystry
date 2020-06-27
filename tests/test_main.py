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
        recv_task = yield tap.Fork(receiver)
        send_task = yield tap.Fork(sender, 5)
        yield tap.Join(send_task)
        value = yield tap.Join(recv_task)
        return value

    assert tap.run(fn) == 5


def test_never_receive():
    def sender(value):
        yield tap.Send('key', value)

    def receiver():
        value = yield tap.Receive('key')
        return value

    def fn():
        # fork in wrong order!
        send_task = yield tap.Fork(sender, 5)
        recv_task = yield tap.Fork(receiver)
        yield tap.Join(send_task)
        value = yield tap.Join(recv_task)
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
        yield tap.Fork(sender, 5)
        return

    assert tap.run(fn) is None


def test_no_arg():
    def sender(value):
        yield tap.Send('key', value)

    def fn():
        yield tap.Fork(sender)
        return

    with pytest.raises(TypeError):
        tap.run(fn)


def test_call():
    def random(value):
        yield tap.Send('key', value)
        return 10

    def fn():
        x = yield tap.Call(random, 5)
        return x

    assert tap.run(fn) == 10
