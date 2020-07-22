import threading
import queue
from concurrent.futures import ThreadPoolExecutor
import inspect
import abc
from collections import defaultdict, deque
from uuid import uuid4
import types
import time


class Effect(metaclass=abc.ABCMeta):
    """
    Base class for effects which can be yielded to the tapystry event loop.
    """
    def __init__(self, type, oncancel=(lambda: None), name=None, caller=None, caller_stack_index=2):
        self.type = type
        self.cancel = oncancel
        self.name = name
        if caller is None:
            caller = inspect.stack()[caller_stack_index]
        self._caller = caller

    def __str__(self):
        if self.name is not None:
            return f"{self.type}({self.name})"
        return f"{self.type}"


class Broadcast(Effect):
    """
    Effect which broadcasts a message for all strands to hear
    """
    def __init__(self, key, value=None, name=None, immediate=False, **effect_kwargs):
        self.key = key
        self.value = value
        self.immediate = immediate
        if name is None:
            name = key
        super().__init__(type="Broadcast", name=name, **effect_kwargs)


class Receive(Effect):
    """
    Effect which waits until it hears a broadcast at the specified key, with value satisfying the specified predicate.
    The tapystry engine returns the matched message's value
    """
    def __init__(self, key, predicate=None, name=None, **effect_kwargs):
        self.key = key
        self.predicate = predicate
        if name is None:
            name = key
        super().__init__(type="Receive", name=name, **effect_kwargs)


class Call(Effect):
    """
    Effect which spins up a new strand by calling generator on the specified arguments,
    The tapystry engine returns the generator's return value
    """
    def __init__(self, gen, args=(), kwargs=None, name=None, **effect_kwargs):
        self.gen = gen
        self.args = args
        self.kwargs = kwargs
        if name is None:
            name = gen.__name__
        super().__init__(type="Call", name=name, **effect_kwargs)


class CallFork(Effect):
    """
    Effect which spins up a new strand by calling generator on the specified arguments
    The tapystry engine immediately returns a Strand object.
    """
    def __init__(self, gen, args=(), kwargs=None, name=None, **effect_kwargs):
        self.gen = gen
        self.args = args
        self.kwargs = kwargs
        if name is None:
            name = gen.__name__
        super().__init__(type="CallFork", name=name, **effect_kwargs)


class CallThread(Effect):
    """
    # TODO: make this thread able to yield back to the event loop?
    Effect which spins up a function in a new thread
    The tapystry engine returns the function's return value
    NOTE: what runs within the thread
    - is *not* a generator, it cannot yield effects back
    - can *not* be canceled
    """
    def __init__(self, f, args=(), kwargs=None, name=None, **effect_kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs or dict()
        if name is None:
            name = f.__name__
        super().__init__(type="CallThread", name=name, **effect_kwargs)



class First(Effect):
    """
    Effect which returns when one of the strands is done.
    The tapystry engine returns the index of the winning strand, and its value.
    NOTE: Use of this can be dangerous and can lead to deadlocks, as it cancels losers.
          It is safer to us higher-level APIs such as Race and Join
    """
    def __init__(self, strands, name=None, cancel_losers=True, **effect_kwargs):
        self.strands = strands
        self.cancel_losers = cancel_losers
        if name is None:
            name = ", ".join([str(x) for x in self.strands])
        self.name = name
        super().__init__(type="Race", name=name, **effect_kwargs)


# TODO: does this really need to be an effect?  what's wrong with just exposing _canceled on Strand?
class Cancel(Effect):
    """
    Effect which cancels the strand specified
    """
    def __init__(self, strand, name=None, **effect_kwargs):
        self.strand = strand
        if name is None:
            name = str(self.strand)
        super().__init__(type="Cancel", name=name, **effect_kwargs)


class Intercept(Effect):
    """
    Effect which waits until the engine finds an effect matching the given predicate, and allows you to modify the yielded value of that effect.
    This is intended for testing only, and can only be used in test_mode.
    The tapystry engine returns a tuple of (effect, inject), where `effect` is the effect intercepted, and `inject` is a function taking a value, and returning an effect that yields that value for the intercepted effect.
    """
    def __init__(self, predicate=None, name=None, **effect_kwargs):
        self.predicate = predicate
        if name is None:
            name = ""
        super().__init__(type="Intercept", name=name, **effect_kwargs)


class DebugTree(Effect):
    """
    Effect which returns the state of the entire tapystry engine
    TODO: make the return value more structured (currently just a string)
    """
    def __init__(self, **effect_kwargs):
        super().__init__(type="DebugTree", **effect_kwargs)



class TapystryError(Exception):
    pass


_noval = object()


class Strand():
    def __init__(self, caller, gen, args=(), kwargs=None, *, parent, edge=None):
        if kwargs is None:
            kwargs = dict()
        self._it = gen(*args, **kwargs)
        self._done = False
        self._result = None
        self.id = uuid4()
        self._canceled = False
        # self._error = None
        self._live_children = []
        self._parent = parent
        if not isinstance(self._it, types.GeneratorType):
            self._result = self._it
            self._done = True
        self._effect = None
        if self._parent is None:
            self._parent_effect = None
            assert edge is None
        else:
            self._parent._live_children.append(self)
            self._parent_effect = self._parent._effect
            self._edge = edge
            assert self._parent_effect is not None
            assert self._edge is not None

        self._caller = caller
        self._future = None

    def send(self, value=None):
        assert not self._canceled
        assert not self._done
        try:
            effect = self._it.send(value)
            self._effect = effect
            return dict(done=False, effect=effect)
        except StopIteration as e:
            self._done = True
            if self._parent is not None:
                self._parent._live_children.remove(self)
            self._result = e.value
            self._effect = None
            return dict(done=True)
        except Exception as e:
            tb = e.__traceback__.tb_next
            line = tb.tb_lineno
            # line = tb.tb_frame.f_code.co_firstlineno
            # line number is not exactly right?
            raise TapystryError(
                "\n".join([
                    f"Exception caught at",
                    f"{self.stack()}",
                    f":",
                    f"File {tb.tb_frame.f_code.co_filename}, line {line}, in {tb.tb_frame.f_code.co_name}",
                    f"{type(e).__name__}: {e}",
                ])
            )

    def __hash__(self):
        return self.id.int

    def __str__(self):
        return f"Strand[{self.id.hex}] (waiting for {self._effect})"

    def _debuglines(self):
        return [
            f"File {self._caller.filename}, line {self._caller.lineno}, in {self._caller.function}",
            f"  {self._caller.code_context[0].strip()}",
        ]

    def stack(self):
        # if self._parent is None:
        #     return [f"Strand[{self.id.hex}]"]
        # else:
        #     stack = list(self._parent.stack())
        #     stack.append(f"{self._parent[1]} Strand[{self.id.hex}]")
        #     return stack

        s = "\n".join(self._debuglines())
        if self._parent is None:
            return s
        else:
            return "\n".join([
                self._parent.stack(),
                f"Yields effect {self._parent_effect}, created at",
                s
            ])

    def _treelines(self, indent=0):
        lines = [" " * indent + line for line in self._debuglines()]
        for c in self._live_children:
            lines.extend(
                c._treelines(indent + 2)
            )
        return lines

    def tree(self):
        return "\n".join(self._treelines())

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
        for child in self._live_children:
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
    def __init__(self, effect, strand):
        self.strand = strand
        self.effect = effect


def run(gen, args=(), kwargs=None, debug=False, test_mode=False, max_threads=None):
    # dict from string to waiting functions
    waiting = defaultdict(list)
    # dict from strand to waiting key
    # TODO: gc hanging strands
    hanging_strands = set()

    q = deque()

    # list of intercept items
    intercepts = []

    initial_strand = Strand(inspect.stack()[1], gen, args, kwargs, parent=None)
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

    def make_injector(intercepted_strand):
        def inject(value):
            advance_strand(intercepted_strand, value)
            hanging_strands.remove(intercepted_strand)
        return lambda x: Call(inject, (x,))

    threads_q = queue.Queue()
    executor = ThreadPoolExecutor(max_workers=max_threads)
    thread_strands = dict()  # dict from thread to callback

    def handle_call_thread(effect, strand):
        future = executor.submit(effect.f, *effect.args, **effect.kwargs)
        id = uuid4()

        def done_callback(f):
            assert f == future
            assert f.done()
            if future.cancelled():
                assert strand._canceled
                threads_q.put((None, id))
            else:
                threads_q.put((f.result(), id))

        thread_strands[id] = strand
        future.add_done_callback(done_callback)

    def handle_item(item):
        if item.strand.is_canceled():
            return

        effect = item.effect
        if isinstance(effect, Intercept):
            if not test_mode:
                raise TapystryError(f"Cannot intercept outside of test mode!")
            intercepts.append(item)
            hanging_strands.add(item.strand)
            return

        if test_mode:
            intercepted = False
            for intercept_item in intercepts:
                intercept_effect = intercept_item.effect
                if intercept_effect.predicate is None or intercept_effect.predicate(effect):
                    intercepted = True
                    break
            if intercepted:
                hanging_strands.remove(intercept_item.strand)
                intercepts.remove(intercept_item)
                hanging_strands.add(item.strand)
                advance_strand(intercept_item.strand, (effect, make_injector(item.strand)))
                return

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
            strand = Strand(effect._caller, effect.gen, effect.args, effect.kwargs, parent=item.strand, edge=effect.name or "call")
            if strand.is_done():
                # wasn't even a generator
                advance_strand(item.strand, strand.get_result())
            else:
                add_waiting_strand("done." + strand.id.hex, item.strand)
                advance_strand(strand)
        elif isinstance(effect, CallFork):
            fork_strand = Strand(effect._caller, effect.gen, effect.args, effect.kwargs, parent=item.strand, edge=effect.name or "fork")
            advance_strand(item.strand, fork_strand)
            if not fork_strand.is_done():
                # otherwise wasn't even a generator
                advance_strand(fork_strand)
        elif isinstance(effect, CallThread):
            handle_call_thread(effect, item.strand)
        elif isinstance(effect, First):
            add_racing_strand(effect.strands, item.strand, effect.cancel_losers)
        elif isinstance(effect, Cancel):
            effect.strand.cancel()
            advance_strand(item.strand)
        elif isinstance(effect, CallThread):
            advance_strand(item.strand)
        elif isinstance(effect, DebugTree):
            advance_strand(item.strand, initial_strand.tree())
        else:
            raise TapystryError(f"Unhandled effect type {type(effect)}: {item.strand.stack()}")

    advance_strand(initial_strand)
    while True:
        if not (len(q) or len(thread_strands)):
            break

        while thread_strands:
            try:
                result, id = threads_q.get(block=len(q) == 0)
                strand = thread_strands[id]
                if not strand.is_canceled():
                    advance_strand(strand, value=result)
                else:
                    assert result is None
                del thread_strands[id]
            except queue.Empty:
                break

        if len(q):
            item = q.pop()
            handle_item(item)

    for strand in hanging_strands:
        if not strand.is_canceled():
            # TODO: add notes on how this can happen
            # forgetting to join fork or forgot to cancel subscription?
            # joining thread that never ends
            # receiving message that never gets broadcast
            raise TapystryError(f"Hanging strands detected waiting for {strand._effect}, in {strand.stack()}")

    assert initial_strand.is_done()
    return initial_strand.get_result()
