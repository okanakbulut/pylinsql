import asyncio
import functools
import time


def _log_func_timing(f, args, kw, sec: float):
    print("func: %r args: [%r, %r] took: %2.4f sec" % (f.__name__, args, kw, sec))


def timing(f):
    "Decorator to log"

    if asyncio.iscoroutinefunction(f):

        @functools.wraps(f)
        async def wrap(*args, **kw):
            ts = time.time()
            result = await f(*args, **kw)
            te = time.time()
            _log_func_timing(f, args, kw, te - ts)
            return result

    else:

        @functools.wraps(f)
        def wrap(*args, **kw):
            ts = time.time()
            result = f(*args, **kw)
            te = time.time()
            _log_func_timing(f, args, kw, te - ts)
            return result

    return wrap
