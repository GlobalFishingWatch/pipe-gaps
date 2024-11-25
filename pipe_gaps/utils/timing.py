import logging
from typing import Callable

from time import time
from functools import wraps

logger = logging.getLogger("utils")


def timing(f, quiet=False) -> Callable:
    """Decorator to measure execution time of a function."""

    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        elapsed_time = te - ts

        if not quiet:
            logger.info(
                "func: {} took: {} sec".format(f.__name__, round(elapsed_time, 4))
            )

        return result, elapsed_time

    return wrap
