import threading
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
        yield tap.Broadcast("msg")
        yield tap.Sleep(0)
        assert a == 6

        yield tap.Broadcast("unlock")
        yield tap.Sleep(0.01)
        assert a == 11

        yield tap.Broadcast("unlock")
        yield tap.Sleep(0.01)
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
        str(x.value).startswith("Hanging strands detected waiting for Call(Acquire")


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
    # print(x.value)
    assert str(x.value).startswith("Exception caught at")
    assert str(x.value).count("in test_release_twice\n") == 1
    assert str(x.value).count("in Acquire\n") == 1
    assert str(x.value).count("Yielded same lock release multiple times?") == 1


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
        yield tap.Broadcast("unlock")
        yield tap.Sleep(0.01)
        assert a == 10

        yield tap.Broadcast("unlock")
        yield tap.Sleep(0.001)
        assert a == 10

    tap.run(fn)


def test_lock_cancel_after_acquire():
    lock = tap.Lock()

    def acquire():
        release = yield lock.Acquire()
        yield tap.Receive("unlock")
        yield release


    def fn():
        t = yield tap.CallFork(acquire)
        yield tap.Sleep(0.001)
        yield tap.Cancel(t)
        t = yield tap.CallFork(acquire)
        yield tap.Sleep(0.001)

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    # TODO fix
    assert str(x.value).startswith("Hanging strands detected waiting for Receive(lock") or \
        str(x.value).startswith("Hanging strands detected waiting for Call(Acquire")


def test_lock_cancel_mid_acquire_trickier():
    a = 0

    lock = tap.Lock()

    def acquire(x, to_cancel):
        yield tap.Receive(str(x))
        release = yield lock.Acquire()
        nonlocal a
        a += 5
        yield tap.Sequence([
            tap.Receive(str(x)),
            tap.Sequence([
                tap.Cancel(to_cancel),
                release,
            ]) if to_cancel is not None else release
        ])


    def fn():
        t1 = yield tap.CallFork(acquire, (1, None))
        t2 = yield tap.CallFork(acquire, (2, t1))
        t3 = yield tap.CallFork(acquire, (3, None))
        yield tap.Broadcast("2")
        yield tap.Broadcast("1")
        yield tap.Sleep(0.001)
        assert a == 5
        yield tap.Broadcast("3")
        yield tap.Broadcast("2")  # this simultaneously tries to cancel 1 and unlocks

        yield tap.Sleep(0.01)
        assert a == 10

        yield tap.Broadcast("1")
        # 1 gets canceled after the acquire, so it's too late to release
        yield tap.Sleep(0.001)
        assert a == 10
        # yield tap.Join([t1, t2, t3])
        yield tap.Cancel(t3)

    tap.run(fn)


def test_queues_get_then_put():
    q = tap.Queue(buffer_size=1)
    a = 0

    def pop_and_add():
        nonlocal a
        b = yield q.Get()
        a += b

    def fn():
        assert not q.has_work()
        t1 = yield tap.CallFork(pop_and_add)
        t2 = yield tap.CallFork(pop_and_add)
        t3 = yield tap.CallFork(pop_and_add)
        yield tap.Sleep(0)
        assert a == 0
        yield q.Put(3)
        assert a == 3

        yield tap.Cancel(t2)
        yield q.Put(5)
        assert a == 8

        yield q.Put(5)
        assert a == 8

        t4 = yield tap.CallFork(pop_and_add)
        yield tap.Sleep(0)
        assert a == 13

        yield tap.Join([t1, t3, t4])

    tap.run(fn)


def test_queues_block_put():
    q = tap.Queue(buffer_size=1)
    a = 0

    def pop_and_add():
        nonlocal a
        b = yield q.Get()
        a += b

    def fn():
        yield tap.CallFork(pop_and_add)
        yield tap.CallFork(pop_and_add)
        yield q.Put(3)
        yield q.Put(5)
        yield q.Put(5)
        yield q.Put(8)

    with pytest.raises(tap.TapystryError) as x:
        tap.run(fn)
    assert a == 8
    # TODO fix
    assert str(x.value).startswith("Hanging strands detected waiting for Receive(get") or \
        str(x.value).startswith("Hanging strands detected waiting for Call(Put")


def test_queues_put_then_get():
    q = tap.Queue(buffer_size=2)

    def fn():
        assert not q.has_work()
        # we have buffer so a put is fine
        yield q.Put(3)
        yield q.Put(5)
        assert q.has_work()
        assert (yield q.Get()) == 3
        assert (yield q.Get()) == 5
        yield q.Put(3)
        yield q.Put(5)

    tap.run(fn)


def test_queues_put_then_get_no_buffer():
    q = tap.Queue(buffer_size=0)

    def fn():
        assert not q.has_work()
        t = yield tap.Fork(q.Put(3))
        yield tap.Sleep(0.01)
        assert q.has_work()
        assert (yield q.Get()) == 3
        yield tap.Join(t)

    tap.run(fn)


def test_queues_put_then_get_late_cancel():
    q = tap.Queue(buffer_size=1)

    def put(x):
        yield q.Put(x)

    def fn():
        yield q.Put(3)
        t1 = yield tap.CallFork(put, (5,))
        t2 = yield tap.CallFork(put, (7,))
        yield tap.Sleep(0)
        assert (yield q.Get()) == 3
        assert (yield q.Get()) == 5
        yield tap.Cancel(t2)  # too late
        assert (yield q.Get()) == 7
        yield tap.Join([t1])

        t = yield tap.Fork(q.Get())
        yield q.Put(3)
        assert (yield tap.Join(t)) == 3

    tap.run(fn)


def test_queues_put_then_get_cancel():
    q = tap.Queue(buffer_size=1)

    def put(x):
        yield q.Put(x)

    def fn():
        yield q.Put(3)
        t1 = yield tap.CallFork(put, (5,))
        t2 = yield tap.CallFork(put, (7,))
        t3 = yield tap.CallFork(put, (9,))
        yield tap.Sleep(0)
        assert (yield q.Get()) == 3
        yield tap.Cancel(t2)  # not too late to cancel
        assert (yield q.Get()) == 5
        assert (yield q.Get()) == 9
        yield tap.Join([t1, t3])

        t = yield tap.Fork(q.Get())
        yield q.Put(3)
        assert (yield tap.Join(t)) == 3

    tap.run(fn)


def test_call_thread():
    a = 0

    cv = threading.Condition()

    def thread_fn():
        nonlocal a
        for _ in range(2):
            with cv:
                cv.wait()
            a += 1
            with cv:
                cv.notify()
        return "done"

    def fn():
        t = yield tap.Fork(tap.CallThread(thread_fn))
        yield tap.Sleep(0.01)
        assert a == 0
        with cv:
            cv.notify()
            cv.wait()
        assert a == 1
        with cv:
            cv.notify()
            cv.wait()
        assert a == 2
        with cv:
            cv.notify()
        assert a == 2

        assert (yield tap.Join(t)) == "done"

    tap.run(fn)


def test_cancel_thread():
    a = 0

    cv = threading.Condition()

    def thread_fn():
        nonlocal a
        for _ in range(2):
            with cv:
                cv.wait()
            a += 1
            with cv:
                cv.notify()

    def fn():
        t = yield tap.Fork(tap.Sequence([
            tap.CallThread(thread_fn),
            tap.CallThread(thread_fn),
        ]))
        yield tap.Sleep(0.01)
        assert a == 0
        with cv:
            cv.notify()
            cv.wait()
        assert a == 1
        # this cancels the second one, but not the first
        yield tap.Cancel(t)
        with cv:
            cv.notify()
            cv.wait()
        assert a == 2
        with cv:
            cv.notify()
        yield tap.Sleep(0.1)
        assert a == 2

    tap.run(fn)


def test_race_threads():
    def fn():
        winner, r = yield tap.Race(dict(
            long=tap.Sleep(0.03),
            short=tap.Sleep(0.02)
        ))
        assert winner == "short"

    tap.run(fn)


def test_immediate_thread():
    def fn():
        def fast():
            pass
        yield tap.Race([tap.CallThread(fast)])

    tap.run(fn)
