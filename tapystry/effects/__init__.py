from tapystry import Effect, Strand, Call, Send, Receive, CallFork, First


def All(effects):
    """Do each of the effects.
    Effects can be a (nested) list/dict structure
    """
    def all():
        if isinstance(effects, Effect):
            val = yield effects
            return val
        if isinstance(effects, list):
            results = []
            for effect in effects:
                if isinstance(effect, (list, dict)):
                    result = yield All(effect)
                else:
                    result = yield effect
                results.append(result)
        else:
            assert isinstance(effects, dict)
            results = dict()
            for k, effect in effects.items():
                if isinstance(effect, (list, dict)):
                    result = yield All(effect)
                else:
                    result = yield effect
                results[k] = result
        return results
    return Call(all)


def Join(strands):
    """
    Returns the result of a strand (or nested structure of strands)
    """
    def join():
        if isinstance(strands, Strand):
            if strands.is_done():
                return strands.get_result()
            key, val = yield First([strands])
            assert key == 0
            return val
        elif isinstance(strands, list):
            vals = yield All([Join(v) for v in strands])
            return vals
        else:
            assert isinstance(strands, dict), strands
            vals = yield All({k: Join(v) for k, v in strands.items()})
            return vals

    return Call(join)


def Fork(effects):
    """Do each of the effects in parallel.
    Returns a strand, whose result reflects the same structure as the effects passed in
    """

    def call_fork(effect):
        val = yield effect
        return val

    def fork_effects(effects):
        if isinstance(effects, Effect):
            return CallFork(call_fork, effects)
        elif isinstance(effects, list):
            return [fork_effects(v) for v in effects]
        else:
            assert isinstance(effects, dict)
            return {k: fork_effects(v) for k, v in effects.items()}

    return All(fork_effects(effects))


def Race(effects):
    """Wait for the first of the effects to finish.
    Returns a tuple with the key of the winning item, and its value
    """
    def race():
        if isinstance(effects, list):
            keys = list(range(len(effects)))
            effects_array = effects
        else:
            assert isinstance(effects, dict)
            keys, effects_array = zip(*effects.items())
        strands = []
        for key, effect in zip(keys, effects_array):
            strand = yield Fork(effect)
            if strand.is_done():
                return key, strand.get_result()
            strands.append(strand)
        i, result = yield First(strands)
        return keys[i], result
    return Call(race)
