"""Transforms for applying sliding windows in Apache Beam."""
from typing import Dict, Any

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.transforms.window import SlidingWindows


class ApplySlidingWindows(beam.PTransform):
    """A PTransform that applies sliding windows to a PCollection.

    Optionally assigns event-time timestamps based on a configurable field
    before applying the windowing strategy.

    Args:
        period:
            The window period (interval between window start times), in seconds.

        offset:
            The offset to apply to window start times, in seconds.

        assign_timestamps:
            Whether to assign timestamps using a field from each element.

        timestamp_field:
            The name of the field containing the timestamp (UNIX time, in seconds).
    """

    def __init__(
        self,
        period: float,
        offset: float,
        assign_timestamps: bool = False,
        timestamp_field: str = "timestamp",
    ):
        self.period = period
        self.offset = offset
        self.assign_timestamps = assign_timestamps
        self.timestamp_field = timestamp_field

    def expand(self, pcoll: PCollection[Dict[str, Any]]) -> PCollection[Dict[str, Any]]:
        """Apply sliding windows to the input PCollection.

        Optionally assigns event-time timestamps using the configured timestamp field.

        Args:
            pcoll:
                A PCollection of dictionaries containing a timestamp field.

        Returns:
            A windowed PCollection with sliding windows applied.
        """
        size = self.period + self.offset

        if self.assign_timestamps:
            field = self.timestamp_field
            pcoll = pcoll | "AddTimestamps" >> beam.Map(
                lambda e: beam.window.TimestampedValue(e, e[field])
            )

        return pcoll | "ApplySlidingWindows" >> beam.WindowInto(
            SlidingWindows(size=size, period=self.period, offset=self.offset)
        )
