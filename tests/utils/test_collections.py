import operator
from copy import copy

from pipe_gaps.utils.collections import heapsort, heapsort_pd, list_sort


def test_list_sort():
    lst = [4, 2, 5, 1, 3]
    original = copy(lst)
    list_sort(lst, method="timsort") == sorted(original)

    lst = [4, 2, 5, 1, 3]
    original = copy(lst)
    list_sort(lst, method="heapsort") == sorted(original)

    lst = [4, 2, 5, 1, 3]
    original = copy(lst)
    list_sort(lst, method="heapsort_pd") == sorted(original)


def test_heapsort():
    sort_key = "timestamp"

    lst = [{sort_key: 3}, {sort_key: 5}, {sort_key: 1}]
    key = operator.itemgetter(sort_key)
    heapsort(lst, key=sort_key)
    assert lst == sorted(lst, key=key)


def test_heapsort_pd():
    sort_key = "timestamp"

    lst = [{sort_key: 3}, {sort_key: 5}, {sort_key: 1}]
    original = copy(lst)
    key = operator.itemgetter(sort_key)
    heapsort_pd(lst, key=sort_key)
    assert lst == sorted(original, key=key)
