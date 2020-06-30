import pytest

import tapystry as tap


def test_lock():
    a = 0

    lock = tap.Lock()

    def waits():
        yield tap.Receive("msg")
        nonlocal a
        a += 1
        release = yield lock.Acquire()
        a += 2
        yield release

    def nowaits():
        release = yield lock.Acquire()
        nonlocal a
        a += 5
        yield tap.Receive("unlock")
        yield release


    def fn():
        yield tap.CallFork(waits)
        yield tap.CallFork(nowaits)
        yield tap.CallFork(nowaits)
        yield tap.Sleep(0)
        assert a == 5

        # the waiting strand finally gets to acquire lock, but it is the latest
        yield tap.Send("msg")
        yield tap.Sleep(0)
        assert a == 6

        yield tap.Send("unlock")
        yield tap.Sleep(0.001)
        assert a == 11

        yield tap.Send("unlock")
        yield tap.Sleep(0.001)
        assert a == 13

    tap.run(fn)


def test_lock_hang():
    lock = tap.Lock()

    def fn():
        yield lock.Acquire()
        yield lock.Acquire()

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    # TODO fix
    assert str(x.value).startswith("Hanging strands detected waiting for Receive(lock") or \
        str(x.value).startswith("Hanging strands detected waiting for Call(acquire")


def test_release_twice():
    lock = tap.Lock()

    def dummy(eff):
        yield eff

    def fn():
        release = yield lock.Acquire()
        yield tap.Call(dummy, (release,))
        yield release

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    assert str(x.value).startswith("Yielded same lock release multiple times?")


def test_create_acquires_out_of_order():
    lock = tap.Lock()

    def fn():
        a1 = lock.Acquire()
        a2 = lock.Acquire()
        r2 = yield a2
        yield r2
        r1 = yield a1
        yield r1

    tap.run(fn)


def test_lock_cancel_mid_acquire():
    a = 0

    lock = tap.Lock()

    def acquire():
        release = yield lock.Acquire()
        nonlocal a
        a += 5
        yield tap.Receive("unlock")
        yield release


    def fn():
        yield tap.CallFork(acquire)
        t = yield tap.CallFork(acquire)
        yield tap.CallFork(acquire)
        yield tap.Sleep(0)
        assert a == 5

        yield tap.Cancel(t)
        yield tap.Send("unlock")
        yield tap.Sleep(0.001)
        assert a == 10

        yield tap.Send("unlock")
        yield tap.Sleep(0.001)
        assert a == 10

    tap.run(fn)

