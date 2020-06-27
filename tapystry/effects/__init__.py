from tapystry.main import Call, Send, Receive, CallFork, First


def Join(task):
    def join():
        if task.is_done():
            return task.get_result()
        key, val = yield First([task])
        assert key == 0
        return val

    return Call(join)


def Fork(effect):
    def fn():
        x = yield effect
        return x
    return CallFork(fn)


def Parallel(effects):
    """
    Fork each of the effects.
    Returns a new effect which forks all
    """
    def parallel():
        tasks = yield All([Fork(e) for e in effects])
        results = yield All([Join(t) for t in tasks])
        return results
    return CallFork(parallel)


def All(effects):
    """Do each of the effects.
    Effects can be a (nested) list/dict structure
    """
    def all():
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


def Race(effects):
    """Wait for the first of the effects to finish
    """
    def race():
        if isinstance(effects, list):
            keys = list(range(len(effects)))
            effects_array = effects
        else:
            assert isinstance(effects, dict)
            keys, effects_array = zip(*effects.items())
        tasks = []
        for key, effect in zip(keys, effects_array):
            task = yield Fork(effect)
            if task.is_done():
                return key, task.get_result()
            tasks.append(task)
        i, result = yield First(tasks)
        return keys[i], result
    return Call(race)
