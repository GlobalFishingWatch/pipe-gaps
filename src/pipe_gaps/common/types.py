import typing
from typing import Any


class NamedTupleDictAccessMixin:
    """A mixin class to provide dictionary-like access methods (__getitem__, items, keys)
    for typing.NamedTuple subclasses.

    This mixin relies on the presence of the `_asdict()` method, which is inherent
    to typing.NamedTuple.
    """
    def __getitem__(self, key: str) -> Any:
        """Allows dictionary-style access by key (e.g., instance['field_name']).
        Raises KeyError if the key does not exist."""
        try:
            return getattr(self, key)
        except AttributeError:
            # Re-raise as KeyError for dictionary-like behavior
            raise KeyError(f"'{key}' not found in {self.__class__.__name__}")

    def items(self) -> typing.ItemsView[str, Any]:
        """Returns a view of the NamedTuple's items as key-value pairs.
        Behaves like dict.items()."""
        return self._asdict().items()

    def keys(self) -> typing.KeysView[str]:
        """Returns a view of the NamedTuple's keys.
        Behaves like dict.keys().
        """
        return self._asdict().keys()

    def __contains__(self, key: str) -> bool:
        """Allows checking for key existence (e.g., 'field' in instance)."""
        return hasattr(self, key) and not key.startswith('_')
