import logging
from operator import itemgetter
from functools import cached_property
from typing import List


logger = logging.getLogger(__name__)


class Key:
    """Defines a single or composite key for grouping elements."""
    def __init__(self, keys: List[str]):
        self.keys = keys

    def __repr__(self):
        return str(self.keys)

    @cached_property
    def func(self):
        # itemgetter returns a callable for each key
        return {k: itemgetter(k) for k in self.keys}

    def list(self):
        return self.keys

    def label(self) -> str:
        """Returns a formatted label for the key fields."""
        return "And".join(s.title() for s in self.keys)

    def format(self, values):
        """Formats key-value pairs as a string."""
        if not isinstance(values, (tuple, list)):
            values = [values]

        return "({})".format(
            ', '.join([f'{k}={v}' for k, v in zip(self.keys, values)])
        )
