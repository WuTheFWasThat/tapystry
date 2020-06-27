import abc
from collections import defaultdict
import queue
from uuid import uuid4


class Effect(metaclass=abc.ABCMeta):
    def __init__(self):
        pass


class Send(Effect):
    def __init__(self, key, value=None):
        self.key = key
        self.value = value


class Receive(Effect):
    def __init__(self, key):
        self.key = key


class Fork(Effect):
    def __init__(self, gen, *args, **kwargs):
        self.gen = gen
        self.args = args
        self.kwargs = kwargs


class Join(Effect):
    def __init__(self, strand):
        self.strand = strand


class TapystryError(Exception):
    pass


_noval = object()


class Strand():
    def __init__(self, gen, args=(), kwargs=None):
        if kwargs is None:
            kwargs = dict()
        self._it = gen(*args, **kwargs)
        self._done = False
        self._result = None
        self.id = uuid4().hex
        # self._canceled = False
        # self._error = None

    def send(self, value=None):
        try:
            return dict(done=False, effect=self._it.send(value))
        except StopIteration as e:
            self._done = True
            self._result = e.value
            return dict(done=True)

    def is_done(self):
        return self._done

    def get_result(self):
        if not self._done:
            raise TapystryError("Tried to get result on a Strand that was still running!")
        return self._result


class _QueueItem():
    def __init__(self, strand, value=_noval):
        self.strand = strand
        self.value = value


def run(gen, args=(), kwargs=None):
    # dict from string to waiting strands
    waiting = defaultdict(list)
    q = queue.SimpleQueue()
    initial_strand = Strand(gen, args, kwargs)
    q.put(_QueueItem(initial_strand))
    while not q.empty():
        item = q.get()
        if item.value == _noval:
            result = item.strand.send()
        else:
            result = item.strand.send(item.value)
        if result['done']:
            wait_key = "join." + item.strand.id
            waiting_strands = waiting[wait_key]
            waiting[wait_key] = []
            for strand in waiting_strands:
                q.put(_QueueItem(strand, item.strand.get_result()))
            continue
        effect = result['effect']

        if not isinstance(effect, Effect):
            raise TapystryError(f"Strand yielded non-effect {type(effect)}")

        if isinstance(effect, Send):
            wait_key = "send." + effect.key
            waiting_strands = waiting[wait_key]
            waiting[wait_key] = []
            for strand in waiting_strands:
                q.put(_QueueItem(strand, effect.value))
            q.put(_QueueItem(item.strand))
        elif isinstance(effect, Receive):
            wait_key = "send." + effect.key
            waiting[wait_key].append(item.strand)
        elif isinstance(effect, Fork):
            strand = Strand(effect.gen, effect.args, effect.kwargs)
            q.put(_QueueItem(strand))
            q.put(_QueueItem(item.strand, strand))
        elif isinstance(effect, Join):
            if effect.strand.is_done():
                q.put(_QueueItem(item.strand, effect.strand.get_result()))
            else:
                wait_key = "join." + effect.strand.id
                waiting[wait_key].append(item.strand)
        else:
            raise TapystryError(f"Unhandled effect type {type(effect)}")

    for k, v in waiting.items():
        if len(v):
            raise TapystryError(f"Hanging strands detected waiting for {k}")

    assert initial_strand.is_done()
    return initial_strand.get_result()
