import logging
from typing import Any

logger = logging.getLogger(__name__)


class SideInputs:
    """Wraps Beam MultiMap side inputs and provides keyed value lookup.

    Args:
        side_inputs:
            Beam MultiMap side input.
    """

    def __init__(self, side_inputs: Any) -> None:
        self._side_inputs = side_inputs

    def get(self, key: Any) -> list[Any]:
        """Returns all values for the given key, or empty list if not found.

        Args:
            key:
                Key to look up in the side inputs.
        """
        try:
            values = list(self._side_inputs[key])
        except KeyError:
            logger.debug("Key {} was not found in side inputs.".format(key))
            return []

        if not values:
            logger.debug("No values found for key {}.".format(key))
            return []

        if not isinstance(values[0], dict):
            # beam.MultiMap encapsulates value in an iterable of iterables (wtf?).
            values = [list(v)[0] for v in values]

        return values

    def get_first(self, key: Any) -> Any | None:
        """Returns the first value for the given key, or None if not found.

        Args:
            key:
                Key to look up in the side inputs.
        """
        first = None

        values = self.get(key)
        if values:
            first = values[0]

        return first
