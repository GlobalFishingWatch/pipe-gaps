"""Utility functions for working with dictionaries.

This module provides helpers for common dictionary operations such as filtering
or transforming key-value pairs. These utilities are intended to simplify and
standardize dictionary manipulation across the codebase.
"""


def copy_dict_without(dictionary: dict, keys: list) -> dict:
    """Returns a shallow copy of the given dictionary excluding specified keys.

    Args:
        dictionary:
            The source dictionary to copy.

        keys:
            A list of keys to remove from the resulting dictionary.

    Returns:
        A new dictionary with the specified keys removed.
    """
    return {k: v for k, v in dictionary.items() if k not in keys}
