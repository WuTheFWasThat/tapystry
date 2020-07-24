from tapystry.main import Call, CallFork, Wrapper, run

from functools import wraps


def as_effect(type=None, forked=False):
    """
    Creates a decorator that turns a normal generator into an effect constructor
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if forked:
                effect = CallFork(f, args=args, kwargs=kwargs)
            else:
                effect = Call(f, args=args, kwargs=kwargs)
            if type is not None:
                effect = Wrapper(effect, type=type)
            return effect
        return wrapper
    return decorator


def runnable(f):
    """
    Wraps a tapestry generator and turns it into a normal function.
    Use on top-level things that you would tap.run
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        return run(f, args=args, kwargs=kwargs)
    return wrapper
