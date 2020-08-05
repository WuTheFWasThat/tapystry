import time

from tapystry import Effect, Strand, Call, Broadcast, Receive, CallFork, First, Cancel, TapystryError, CallThread, Wrapper
from tapystry import as_effect


@as_effect("Sequence")
def Sequence(effects, name=None):
    """Do each of the effects.
    Effects can be a (nested) list/dict structure
    """
    if isinstance(effects, Effect):
        val = yield effects
        return val
    if isinstance(effects, list):
        results = []
        for effect in effects:
            if isinstance(effect, (list, dict)):
                result = yield Sequence(effect, name)
            else:
                result = yield effect
            results.append(result)
    else:
        if not isinstance(effects, dict):
            raise TapystryError(f"Input to Sequence should be an Effect")
        results = dict()
        for k, effect in effects.items():
            if isinstance(effect, (list, dict)):
                result = yield Sequence(effect, name)
            else:
                result = yield effect
            results[k] = result
    return results


@as_effect("Join")
def Join(strands, name=None):
    """
    Returns the result of a strand (or nested structure of strands)
    """
    if isinstance(strands, Strand):
        if strands.is_done():
            return strands.get_result()
        key, val = yield First([strands], name=f"Join({name})")
        assert key == 0
        return val
    elif isinstance(strands, list):
        vals = yield Sequence([Join(v, name=name) for v in strands])
        return vals
    else:
        if not isinstance(strands, dict):
            raise TapystryError(f"Input to Join should be a Strand (or nested list/dict of them): {strands}")
        vals = yield Sequence({k: Join(v, name=name) for k, v in strands.items()})
        return vals


def Fork(effects, *, run_first=False):
    """Do each of the effects in parallel.
    Returns a strand, whose result reflects the same structure as the effects passed in
    """

    def call_fork(effect):
        val = yield effect
        return val

    def fork_effects(effects):
        if isinstance(effects, Effect):
            return CallFork(call_fork, (effects,), run_first=run_first)
        elif isinstance(effects, list):
            return [fork_effects(v) for v in effects]
        else:
            if not isinstance(effects, dict):
                raise TapystryError(f"Input to Fork should be an Effect (or nested list/dict of them): {effects}")
            return {k: fork_effects(v) for k, v in effects.items()}

    return Wrapper(Sequence(fork_effects(effects)), type="Fork")


@as_effect("Race")
def Race(effects, name=None, ensure_cancel=True):
    """Wait for the first of the effects to finish.
    Returns a tuple with the key of the winning item, and its value
    """
    if isinstance(effects, list):
        keys = list(range(len(effects)))
        effects_array = effects
    else:
        if not isinstance(effects, dict):
            raise TapystryError(f"Input to Race should be an Effect (or nested list/dict of them): {effects}")
        keys, effects_array = zip(*effects.items())
    strands = []
    for key, effect in zip(keys, effects_array):
        strand = yield Fork(effect)
        strands.append(strand)
    i, result = yield First(strands, name=f"Race({name})", ensure_cancel=ensure_cancel)
    return keys[i], result


@as_effect("Subscribe", forked=True)
def Subscribe(message_key, fn, predicate=None, leading_only=False, latest_only=False):
    """
    Upon receiving any message, runs the specified function on the sent value.
    Returns the strand running the subscription.

    By default, runs on every single message (like takeEvery in redux-saga)
    If leading_only is True, then we don't start new calls to fn while old ones are running (like takeLeading)
    If latest_only is True, then we cancel old calls to make way for new calls (like takeLatest)
    """
    if leading_only and latest_only:
        raise TapystryError(f"Subscribe cannot set both leading_only and latest_only")

    task = None
    while True:
        msg = yield Receive(message_key, predicate=predicate)
        if leading_only:
            yield Call(fn, (msg,))
        else:
            if latest_only and task is not None:
                yield Cancel(task)
            task = yield CallFork(fn, (msg,))


def Sleep(t):
    return CallThread(time.sleep, args=(t,))

