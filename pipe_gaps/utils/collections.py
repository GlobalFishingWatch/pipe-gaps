import operator
import itertools
from heapq import heapify, _siftup

import pandas as pd
from rich.progress import track


def pairwise(iterable):
    # In itertools.pairwise from python 3.10.
    a, b = itertools.tee(iterable)
    next(b, None)

    return zip(a, b)


def list_sort(items, method="timsort", key=None):
    """Sorts a list in-place."""

    def timsort(lst, key=None, **kwargs):
        if key is not None:
            key = operator.itemgetter(key)

        lst.sort(key=key, **kwargs)

    available_methods = {
        "timsort": timsort,
        "heapsort": heapsort,
        "heapsort_pd": heapsort_pd,
    }

    if method not in available_methods:
        raise NotImplementedError(f"Sorting method {method} not implemented.")

    available_methods[method](items, key=key)


def heapsort_pd(lst, key=None):
    if key is None:
        key = 0

    df = pd.DataFrame(lst)
    df.sort_values(kind="mergesort", inplace=True, by=key)
    lst[:] = df.to_dict('records')


class heapsort:
    """Heapsort for a list of dicts.

    Complexity:
        Time: O(n * log(N)).
        Space: O(1). Unless a sorting key is passed...


    https://stackoverflow.com/questions/62329870/python-sort-in-constant-space-o1
    """
    def __init__(self, lst, key=None):
        if key is not None:
            lst[:] = [(x[key], x) for x in lst]

        self.list = lst
        self._heapsort(self.list)

        if key is not None:
            lst[:] = [x[1] for x in lst]

    # @profile  # noqa  # Uncomment to run memory profiler
    def _heapsort(self, lst):
        heapify(lst)

        iterable = reversed(range(1, len(lst)))

        iterable = track(iterable, total=len(lst), description="Sorting...")

        for self.size in iterable:
            lst[0], lst[self.size] = lst[self.size], lst[0]
            _siftup(self, 0)

        lst.reverse()

    def __len__(self):
        return self.size

    def __getitem__(self, index):
        return self.list[index]

    def __setitem__(self, index, value):
        self.list[index] = value
