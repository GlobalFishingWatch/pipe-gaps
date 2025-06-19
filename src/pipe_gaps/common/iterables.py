import bisect
from typing import Callable, List, Optional, TypeVar

T = TypeVar("T")
K = TypeVar("K")


def binary_search_first_ge(
    items: List[T],
    start_value: K,
    key: Callable[[T], K],
) -> Optional[int]:
    """Find index of first item in sorted `items` whose key >= `start_value` using binary search.

    This function performs a binary search to efficiently locate the leftmost index
    where the key of the item is greater than or equal to `start_value`.

    Args:
        items:
            Sorted list of items.

        start_value:
            The value to compare to.

        key:
            Function to extract a comparable key from each item.

    Returns:
        Index of the first item with key >= start_value, or None if no such item exists.
    """
    keys = [key(item) for item in items]
    idx = bisect.bisect_left(keys, start_value)
    if idx == len(keys):
        return None

    return idx
