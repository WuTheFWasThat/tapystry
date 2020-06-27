from tapystry.main import Send, Receive, Fork, Join


def Parallel(effects):
    """
    Fork each of the effects.
    Returns a new effect which joins all
    """
    def run_effect(effect):
        x = yield effect
        return x

    def parallel():
        tasks = []
        for effect in effects:
            task = yield Fork(run_effect, effect)
            tasks.append(task)

        results = []
        for t in tasks:
            result = yield Join(t)
            results.append(result)
        return results

    return Fork(parallel)
