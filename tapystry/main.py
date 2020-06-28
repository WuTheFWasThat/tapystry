import abc
from collections import defaultdict
import queue
from uuid import uuid4
import types


class Effect(metaclass=abc.ABCMeta):
    def __init__(self):
        pass


class Send(Effect):
    def __init__(self, key, value=None):
        self.key = key
        self.value = value


class Receive(Effect):
    def __init__(self, key, predicate=None):
        self.key = key
        self.predicate = predicate


class Call(Effect):
    def __init__(self, gen, *args, **kwargs):
        self.gen = gen
        self.args = args
        self.kwargs = kwargs


class CallFork(Effect):
    def __init__(self, gen, *args, **kwargs):
        self.gen = gen
        self.args = args
        self.kwargs = kwargs


class First(Effect):
    def __init__(self, strands):
        self.strands = strands


# TODO: does this really need to be an effect?  what's wrong with just exposing _canceled on Strand?
class Cancel(Effect):
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
        self.id = uuid4()
        self._canceled = False
        # self._error = None
        self._children = []
        if not isinstance(self._it, types.GeneratorType):
            self._result = self._it
            self._done = True

    def send(self, value=None):
        assert not self._canceled
        assert not self._done
        try:
            return dict(done=False, effect=self._it.send(value))
        except StopIteration as e:
            self._done = True
            self._result = e.value
            return dict(done=True)

    def __hash__(self):
        return self.id.int

    def __str__(self):
        return self.id.hex

    def is_done(self):
        return self._done

    def get_result(self):
        if not self._done:
            raise TapystryError("Tried to get result on a Strand that was still running!")
        return self._result

    def cancel(self):
        for child in self._children:
            child.cancel()
        self._canceled = True

    def is_canceled(self):
        return self._canceled


class _QueueItem():
    def __init__(self, strand, value=_noval):
        self.strand = strand
        self.value = value


def run(gen, args=(), kwargs=None):
    # dict from string to waiting functions
    waiting = defaultdict(list)
    # dict from strand to waiting key
    hanging_strands = dict()
    # q = queue.SimpleQueue()
    q = queue.LifoQueue()
    initial_strand = Strand(gen, args, kwargs)
    if initial_strand.is_done():
        # wasn't even a generator
        return initial_strand.get_result()
    q.put(_QueueItem(initial_strand))

    def add_waiting_strand(key, strand, fn=None):
        assert strand not in hanging_strands
        hanging_strands[strand] = key

        def receive(val):
            assert strand in hanging_strands
            if fn is not None and not fn(val):
                return False
            del hanging_strands[strand]
            q.put(_QueueItem(strand, val))
            return True
        waiting[key].append(receive)

    def add_racing_strand(racing_strands, race_strand):
        assert race_strand not in hanging_strands
        hanging_strands[race_strand] = "race"

        received = False

        def receive_fn(i):
            def receive(val):
                nonlocal received
                assert not received
                received = True
                for j, strand in enumerate(racing_strands):
                    if j == i:
                        assert strand.is_done()
                    else:
                        assert not strand.is_done()
                        strand.cancel()
                assert race_strand in hanging_strands
                del hanging_strands[race_strand]
                q.put(_QueueItem(race_strand, (i, val)))
            return receive
        for i, strand in enumerate(racing_strands):
            if strand.is_done():
                raise TapystryError(f"Race between effects that are already completed")
            waiting["done." + strand.id.hex].append(receive_fn(i))

    def resolve_waiting(wait_key, value):
        fns = waiting[wait_key]
        # clear first in case it mutates
        waiting[wait_key] = [fn for fn in fns if not fn(value)]


    while not q.empty():
        item = q.get()
        if item.strand.is_canceled():
            continue
        if item.value == _noval:
            result = item.strand.send()
        else:
            result = item.strand.send(item.value)
        if result['done']:
            resolve_waiting("done." + item.strand.id.hex, item.strand.get_result())
            continue
        effect = result['effect']

        if not isinstance(effect, Effect):
            raise TapystryError(f"Strand yielded non-effect {type(effect)}")

        if isinstance(effect, Send):
            # prioritize the triggered stuff over returning the current strand
            q.put(_QueueItem(item.strand))
            resolve_waiting("send." + effect.key, effect.value)
        elif isinstance(effect, Receive):
            add_waiting_strand("send." + effect.key, item.strand, effect.predicate)
        elif isinstance(effect, Call):
            strand = Strand(effect.gen, effect.args, effect.kwargs)
            item.strand._children.append(strand)
            if strand.is_done():
                # wasn't even a generator
                q.put(_QueueItem(item.strand, strand.get_result()))
            else:
                q.put(_QueueItem(strand))
                add_waiting_strand("done." + strand.id.hex, item.strand)
        elif isinstance(effect, CallFork):
            strand = Strand(effect.gen, effect.args, effect.kwargs)
            item.strand._children.append(strand)
            # prioritize starting the forked strand over returning the strand
            q.put(_QueueItem(item.strand, strand))
            if not strand.is_done():
                # otherwise wasn't even a generator
                q.put(_QueueItem(strand))
        elif isinstance(effect, First):
            add_racing_strand(effect.strands, item.strand)
        elif isinstance(effect, Cancel):
            effect.strand.cancel()
            q.put(_QueueItem(item.strand))
        else:
            raise TapystryError(f"Unhandled effect type {type(effect)}")

    for strand, key in hanging_strands.items():
        if not strand.is_canceled():
            raise TapystryError(f"Hanging strands detected waiting for {key}")

    assert initial_strand.is_done()
    return initial_strand.get_result()
