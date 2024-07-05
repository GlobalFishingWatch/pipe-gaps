import json
import logging

from importlib_resources import files

from time import time
from functools import wraps

logger = logging.getLogger(__name__)


def timing(f, quiet=False):
    """Decorator to measure execution time of a function."""
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        elapsed_time = te - ts

        if not quiet:
            logger.info('func: {} took: {} sec'.format(f.__name__, round(elapsed_time, 4)))

        return result, elapsed_time
    return wrap


def get_sample_messages():
    # Reads contents with UTF-8 encoding and returns str.
    return json_load(files('pipe_gaps.data').joinpath("sample_messages.json"))


def json_load(path):
    with open(path) as file:
        return json.load(file)
