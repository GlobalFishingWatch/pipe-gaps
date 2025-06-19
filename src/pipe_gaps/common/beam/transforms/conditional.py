from typing import Any
import apache_beam as beam
from apache_beam import PCollection, PTransform


class Conditional(beam.PTransform):
    """Conditionally applies a given PTransform to a PCollection.

    If the condition is False, returns the input PCollection unchanged.

    Args:
        transform:
            The PTransform to apply if the condition is True.

        condition:
            Whether to apply the transform.
    """
    def __init__(
        self,
        transform: PTransform,
        condition: bool = True,
    ) -> None:
        self.condition = condition
        self.transform = transform

    def expand(self, pcoll: PCollection[Any]) -> PCollection[Any]:
        """Applies the transform if the condition is True, otherwise returns input unchanged."""
        if self.condition:
            return pcoll | self.transform

        return pcoll
