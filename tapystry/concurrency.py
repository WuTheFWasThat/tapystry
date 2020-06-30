from uuid import uuid4
from collections import deque

from tapystry import Call, Send, Receive, TapystryError

class Lock():
    def __init__(self, name=None):
        self._id = uuid4()
        self._q = deque()
        self.name = name or ""

    def Acquire(self):
        acquire_id = self._id.hex + uuid4().hex

        def release():
            if not len(self._q) or acquire_id != self._q.popleft():
                raise TapystryError(f"Yielded same lock release multiple times?  {self.name}")
            yield Send(f"lock.{self._id}.{acquire_id}")
        Release = Call(release)

        def acquire():
            if len(self._q) > 0:
                last = self._q[-1]
                self._q.append(acquire_id)
                yield Receive(f"lock.{self._id}.{last}")
            else:
                self._q.append(acquire_id)
            return Release
        return Call(acquire)
