import asyncio
import functools
import time


def _log_func_timing(f, args, kw, sec: float):
    print("func: %r args: [%r, %r] took: %2.4f sec" % (f.__name__, args, kw, sec))


def timing(func):
    "Decorator to log how long a function takes to execute."

    if asyncio.iscoroutinefunction(func):
        # asynchronous function
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            ts = time.time()
            result = await func(*args, **kwargs)
            te = time.time()
            _log_func_timing(func, args, kwargs, te - ts)
            return result

    else:
        # synchronous function
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            ts = time.time()
            result = func(*args, **kwargs)
            te = time.time()
            _log_func_timing(func, args, kwargs, te - ts)
            return result

    return wrapper
