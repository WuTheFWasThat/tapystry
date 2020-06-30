from uuid import uuid4
from collections import deque

from tapystry import Call, Send, Receive, TapystryError

class Lock():
    def __init__(self, name=None):
        self._id = uuid4()
        self._q = deque()
        self.name = name or ""
        self._counter = 0

    def Acquire(self):
        acquire_id = self._counter
        self._counter += 1

        def remove():
            self._q.remove(acquire_id)

        def release():
            if not len(self._q) or acquire_id != self._q.popleft():
                raise TapystryError(f"Yielded same lock release multiple times?  {self.name}")
            if len(self._q):
                # use immediate=True to make sure receiving thread doesn't get canceled before the receive happens
                yield Send(f"lock.{self._id}.{self._q[0]}", immediate=True)
        Release = Call(release)

        def acquire():
            if len(self._q) > 0:
                self._q.append(acquire_id)
                yield Receive(f"lock.{self._id}.{acquire_id}", oncancel=remove)
            else:
                self._q.append(acquire_id)
            return Release
        return Call(acquire)
