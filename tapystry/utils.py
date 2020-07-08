from tapystry.main import Call, CallFork, run

from functools import wraps


def as_effect(forked=False):
    """
    Creates a decorator that turns a normal generator into an effect constructor
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if forked:
                return CallFork(f, args=args, kwargs=kwargs)
            else:
                return Call(f, args=args, kwargs=kwargs)
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
