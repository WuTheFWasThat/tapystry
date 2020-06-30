import abc
from collections import defaultdict, deque
from uuid import uuid4
import types
import time


class Effect(metaclass=abc.ABCMeta):
    def __init__(self, type, oncancel=(lambda: None), name=None):
        self.type = type
        self.cancel = oncancel
        self.name = name

    def __str__(self):
        if self.name is not None:
            return f"{self.type}({self.name})"
        return f"{self.type}"


class Broadcast(Effect):
    def __init__(self, key, value=None, name=None, immediate=False, **effect_kwargs):
        self.key = key
        self.value = value
        self.immediate = immediate
        if name is None:
            name = key
        super().__init__(type="Broadcast", name=name, **effect_kwargs)


class Receive(Effect):
    def __init__(self, key, predicate=None, name=None, **effect_kwargs):
        self.key = key
        self.predicate = predicate
        if name is None:
            name = key
        super().__init__(type="Receive", name=name, **effect_kwargs)


class Call(Effect):
    def __init__(self, gen, args=(), kwargs=None, name=None, **effect_kwargs):
        self.gen = gen
        self.args = args
        self.kwargs = kwargs
        if name is None:
            name = gen.__name__
        super().__init__(type="Call", name=name, **effect_kwargs)


class CallFork(Effect):
    def __init__(self, gen, args=(), kwargs=None, name=None, **effect_kwargs):
        self.gen = gen
        self.args = args
        self.kwargs = kwargs
        if name is None:
            name = gen.__name__
        super().__init__(type="CallFork", name=name, **effect_kwargs)


class First(Effect):
    """NOTE: use of this can be dangerous, as it cancels losers"""
    def __init__(self, strands, name=None, cancel_losers=True, **effect_kwargs):
        self.strands = strands
        self.cancel_losers = cancel_losers
        if name is None:
            name = ", ".join([str(x) for x in self.strands])
        self.name = name
        super().__init__(type="Race", name=name, **effect_kwargs)


# TODO: does this really need to be an effect?  what's wrong with just exposing _canceled on Strand?
class Cancel(Effect):
    def __init__(self, strand, name=None, **effect_kwargs):
        self.strand = strand
        if name is None:
            name = str(self.strand)
        super().__init__(type="Cancel", name=name, **effect_kwargs)


class Sleep(Effect):
    def __init__(self, t, name=None, **effect_kwargs):
        self.t = t
        if name is None:
            name = str(t)
        super().__init__(type="Sleep", name=name, **effect_kwargs)


class TapystryError(Exception):
    pass


_noval = object()


class Strand():
    def __init__(self, gen, args=(), kwargs=None, parent=None):
        if kwargs is None:
            kwargs = dict()
        self._it = gen(*args, **kwargs)
        self._done = False
        self._result = None
        self.id = uuid4()
        self._canceled = False
        # self._error = None
        self._children = []
        self._parent = parent
        if not isinstance(self._it, types.GeneratorType):
            self._result = self._it
            self._done = True
        self._effect = None

    def send(self, value=None):
        assert not self._canceled
        assert not self._done
        try:
            effect = self._it.send(value)
            self._effect = effect
            return dict(done=False, effect=effect)
        except StopIteration as e:
            self._done = True
            self._result = e.value
            self._effect = None
            return dict(done=True)

    def __hash__(self):
        return self.id.int

    def __str__(self):
        return f"Strand[{self.id.hex}] (waiting for {self._effect})"

    def stack(self):
        if self._parent is None:
            return [f"Strand[{self.id.hex}]"]
        else:
            stack = list(self._parent[0].stack())
            stack.append(f"{self._parent[1]} Strand[{self.id.hex}]")
            return stack

    def is_done(self):
        return self._done

    def get_result(self):
        if not self._done:
            raise TapystryError("Tried to get result on a Strand that was still running!")
        return self._result

    def cancel(self):
        # if self._done:  ??
        if self._effect is not None:
            self._effect.cancel()
        for child in self._children:
            child.cancel()
        self._canceled = True

    def is_canceled(self):
        return self._canceled


def _indented(lines):
    indent = 0
    s = ""
    for line in lines:
        s += " " * indent + line + "\n"
        indent += 2
    return s


class _QueueItem():
    def __init__(self, effect, strand, wake_time=None):
        self.strand = strand
        self.effect = effect
        self.wake_time = wake_time


def run(gen, args=(), kwargs=None, debug=False):
    # dict from string to waiting functions
    waiting = defaultdict(list)
    # dict from strand to waiting key
    # TODO: gc hanging strands
    hanging_strands = set()
    q = deque()
    initial_strand = Strand(gen, args, kwargs)
    if initial_strand.is_done():
        # wasn't even a generator
        return initial_strand.get_result()

    def queue_effect(effect, strand):
        if not isinstance(effect, Effect):
            raise TapystryError(f"Strand yielded non-effect {type(effect)}")
        if isinstance(effect, Broadcast):
            if effect.immediate:
                q.append(_QueueItem(effect, strand))
            else:
                q.appendleft(_QueueItem(effect, strand))
        elif isinstance(effect, Sleep):
            wake_time = time.time() + effect.t
            q.appendleft(_QueueItem(effect, strand, wake_time))
        else:
            q.append(_QueueItem(effect, strand))

    def advance_strand(strand, value=_noval):
        if strand.is_canceled():
            return
        if value == _noval:
            result = strand.send()
        else:
            result = strand.send(value)
        if result['done']:
            resolve_waiting("done." + strand.id.hex, strand.get_result())
            return
        effect = result['effect']
        queue_effect(effect, strand)

    def add_waiting_strand(key, strand, fn=None):
        assert strand not in hanging_strands
        hanging_strands.add(strand)

        def receive(val):
            assert strand in hanging_strands
            if fn is not None and not fn(val):
                return False
            hanging_strands.remove(strand)
            advance_strand(strand, val)
            return True
        waiting[key].append(receive)

    def add_racing_strand(racing_strands, race_strand, cancel_losers):
        assert race_strand not in hanging_strands
        hanging_strands.add(race_strand)

        received = False

        def receive_fn(i):
            def receive(val):
                nonlocal received
                assert not (cancel_losers and received)
                if received:
                    return
                for j, strand in enumerate(racing_strands):
                    if j == i:
                        assert strand.is_done()
                    else:
                        assert not strand.is_done()
                        if cancel_losers:
                            strand.cancel()
                received = True
                assert race_strand in hanging_strands
                hanging_strands.remove(race_strand)
                advance_strand(race_strand, (i, val))
            return receive
        for i, strand in enumerate(racing_strands):
            if strand.is_done():
                raise TapystryError(f"Race between effects that are already completed")
            waiting["done." + strand.id.hex].append(receive_fn(i))

    def resolve_waiting(wait_key, value):
        fns = waiting[wait_key]
        if debug:
            print("resolving", wait_key, len(fns))
        # clear first in case it mutates
        waiting[wait_key] = [fn for fn in fns if not fn(value)]


    advance_strand(initial_strand)
    while len(q):
        item = q.pop()
        if item.wake_time is not None and item.wake_time > time.time():
            q.appendleft(item)
            continue

        if item.strand.is_canceled():
            continue
        effect = item.effect
        if debug:
            print(f"Handling {effect} (from {item.strand})")

        if not isinstance(effect, Effect):
            raise TapystryError(f"Strand yielded non-effect {type(effect)}")

        if isinstance(effect, Broadcast):
            resolve_waiting("broadcast." + effect.key, effect.value)
            advance_strand(item.strand)
        elif isinstance(effect, Receive):
            add_waiting_strand("broadcast." + effect.key, item.strand, effect.predicate)
        elif isinstance(effect, Call):
            strand = Strand(effect.gen, effect.args, effect.kwargs, parent=(item.strand, effect.name or "call"))
            item.strand._children.append(strand)
            if strand.is_done():
                # wasn't even a generator
                advance_strand(item.strand, strand.get_result())
            else:
                add_waiting_strand("done." + strand.id.hex, item.strand)
                advance_strand(strand)
        elif isinstance(effect, CallFork):
            fork_strand = Strand(effect.gen, effect.args, effect.kwargs, parent=(item.strand, effect.name or "fork"))
            item.strand._children.append(fork_strand)
            advance_strand(item.strand, fork_strand)
            if not fork_strand.is_done():
                # otherwise wasn't even a generator
                advance_strand(fork_strand)
        elif isinstance(effect, First):
            add_racing_strand(effect.strands, item.strand, effect.cancel_losers)
        elif isinstance(effect, Cancel):
            effect.strand.cancel()
            advance_strand(item.strand)
        elif isinstance(effect, Sleep):
            advance_strand(item.strand)
        else:
            raise TapystryError(f"Unhandled effect type {type(effect)}")

    for strand in hanging_strands:
        if not strand.is_canceled():
            # TODO: add notes on how this can happen
            # forgetting to join fork or forgot to cancel subscription?
            # joining thread that never ends
            # receiving message that never gets broadcast
            raise TapystryError(f"Hanging strands detected waiting for {strand._effect}, in {strand.stack()}")

    assert initial_strand.is_done()
    return initial_strand.get_result()
