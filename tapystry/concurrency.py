from uuid import uuid4
from collections import deque

from tapystry import Call, Broadcast, Receive, TapystryError, as_effect

"""
TODO: have something like a Promise?

def doStuffWithPromise(p):
    ...
    yield p.Resolve(val)

p = Promise()
yield Fork(doStuffWithPromise, (p,))
yield p

reem suggests just running an asyncio event loop to schedule ascynio futures
"""



class Lock():
    """
    Like a traditional lock

    Usage:
        l = Lock()
        release = yield l.Acquire()
        ...
        yield release
    """
    def __init__(self, name=None):
        self._id = uuid4()
        self._q = deque()
        self.name = name or ""
        self._counter = 0

    @as_effect()
    def Acquire(self):
        acquire_id = self._counter
        self._counter += 1

        def remove():
            self._q.remove(acquire_id)

        @Call
        def Release():
            if not len(self._q) or acquire_id != self._q.popleft():
                raise TapystryError(f"Yielded same lock release multiple times?  {self.name}")
            if len(self._q):
                # use immediate=True to make sure receiving thread doesn't get canceled before the receive happens
                yield Broadcast(f"lock.{self._id}.{self._q[0]}", immediate=True)

        if len(self._q) > 0:
            self._q.append(acquire_id)
            yield Receive(f"lock.{self._id}.{acquire_id}", oncancel=remove)
        else:
            self._q.append(acquire_id)
        return Release


class Queue():
    """
    A queue of items.
    Each item can only be taken once.

    A buffer_size value of -1 indicates no limit
    """
    def __init__(self, name=None, buffer_size=0):
        self._id = uuid4()
        self._buffer_size = buffer_size
        self.name = name or ""
        self._buffer = deque()
        # queue of gets (if queue is empty)
        self._gets = deque()
        # queue of puts (if queue is full)
        self._puts = deque()
        self._put_vals = dict()
        self._counter = 0

    @as_effect()
    def Put(self, item):
        put_id = self._counter
        self._counter += 1

        def remove():
            self._puts.remove(put_id)
            self._put_vals.pop(put_id)

        if len(self._gets):
            assert not len(self._puts)
            get_id = self._gets.popleft()
            yield Broadcast(f"put.{self._id}.{get_id}", item, immediate=True)
        else:
            if self._buffer_size >= 0 and len(self._buffer) >= self._buffer_size:
                assert len(self._buffer) == self._buffer_size
                self._puts.append(put_id)
                self._put_vals[put_id] = item
                yield Receive(f"get.{self._id}.{put_id}", oncancel=remove)
            else:
                self._buffer.append(item)

    @as_effect()
    def Get(self):
        get_id = self._counter
        self._counter += 1

        def remove():
            self._gets.remove(get_id)

        if len(self._buffer):
            item = self._buffer.popleft()
            if len(self._puts):
                put_id = self._puts.popleft()
                self._buffer.append(self._put_vals.pop(put_id))
                yield Broadcast(f"get.{self._id}.{put_id}", immediate=True)
        elif len(self._puts):
            assert self._buffer_size == 0
            put_id = self._puts.popleft()
            item = self._put_vals.pop(put_id)
            yield Broadcast(f"get.{self._id}.{put_id}", immediate=True)
        else:
            self._gets.append(get_id)
            item = yield Receive(f"put.{self._id}.{get_id}", oncancel=remove)
        return item

    def has_work(self):
        return len(self._buffer) or len(self._puts)
