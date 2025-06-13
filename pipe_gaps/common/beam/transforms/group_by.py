import logging
from functools import cached_property
from typing import List, Union

import apache_beam as beam
from apache_beam import PCollection

logger = logging.getLogger(__name__)


class Key:
    """Defines a single or composite key for grouping elements."""
    def __init__(self, keys: List[str]):
        self.keys = keys

    def __repr__(self):
        return str(self.keys)

    @cached_property
    def func(self):
        return {
            k: lambda x: x[k]
            for k in self.keys
        }

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


class GroupBy(beam.PTransform):
    """Groups elements by specified keys using Beam's GroupBy transform.

    This transform wraps Beam's native `GroupBy` with a dynamically generated label
    that reflects the keys used for grouping. For example, if keys `["user", "country"]`
    are passed, the step label will be `"GroupByUserAndCountry"`.

    This makes the Dataflow graph easier to read and debug, as each grouping operation
    is clearly identified by its grouping keys.

    Args:
        keys:
            List of string keys to group by.
            These keys are used both to extract grouping fields from each element
            and to generate a descriptive label for the transform.

        label:
            A label to describe which elements are being grouped by.
            Useful when you want to reuse this same transform in different places of a pipeline.
    """
    def __init__(self, keys: Union[Key, List[str]], label: str = ""):
        self.key = keys
        self.label = label

        if isinstance(self.key, (list, tuple)):
            self.key = Key(self.key)

        label = f"Group{self.label}By{self.key.name()}"
        super().__init__(label=label)

    def expand(self, pcoll: PCollection) -> PCollection:
        """Applies GroupBy with dynamic key extractors and descriptive labeling.

        Args:
            pcoll:
                Input PCollection of dict-like elements to group.

        Returns:
            PCollection where elements are grouped by the specified keys,
            wrapped in a step with a human-readable label.
        """
        logger.info(f"Grouping inputs by keys: {self.key.keys}.")

        return pcoll | beam.GroupBy(**self.key.func)
