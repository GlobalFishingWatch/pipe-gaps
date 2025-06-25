import logging
from typing import List, Union

import apache_beam as beam
from apache_beam import PCollection

from pipe_gaps.common.key import Key

logger = logging.getLogger(__name__)


class GroupBy(beam.PTransform):
    """Groups elements by specified keys using Beam's GroupBy transform.

    This transform wraps Beam's native `GroupBy` with a dynamically generated label
    that reflects the keys used for grouping. For example, if keys `["user", "country"]`
    are passed, the step label will be `"GroupByUserAndCountry"`.

    This makes the Dataflow graph easier to read and debug, as each grouping operation
    is clearly identified by its grouping keys.

    Args:
        key:
            List of string keys to group by.
            These keys are used both to extract grouping fields from each element
            and to generate a descriptive label for the transform.

        elements:
            A label to describe which elements are being grouped by.
            Useful when you want to reuse this same transform in different places of a pipeline.

        **kwargs:
            Additional keyword arguments passed to base PTransform class.
    """
    def __init__(self, key: Union[Key, List[str]], elements: str = "", **kwargs):
        super().__init__(**kwargs)
        self.key = key

        if isinstance(self.key, (list, tuple)):
            self.key = Key(self.key)

        transform_label = f"Group{elements}By{self.key.label()}"
        super().__init__(label=transform_label)

    def expand(self, pcoll: PCollection) -> PCollection:
        """Applies GroupBy with dynamic key extractors and descriptive labeling.

        Args:
            pcoll:
                Input PCollection of dict-like elements to group.

        Returns:
            PCollection where elements are grouped by the specified keys,
            wrapped in a step with a human-readable label.
        """
        return pcoll | beam.GroupBy(**self.key.func)
