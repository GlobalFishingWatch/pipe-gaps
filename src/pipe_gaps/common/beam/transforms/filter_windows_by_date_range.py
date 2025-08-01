from datetime import date, timedelta
from typing import Tuple, Any

import apache_beam as beam
from apache_beam.transforms.window import IntervalWindow
from apache_beam import PCollection


class FilterWindowsByDateRange(beam.PTransform):
    """Filters elements in windows that intersect a specified date range."""
    def __init__(self, date_range: Tuple[date, date], offset: int = 0, **kwargs: Any) -> None:
        """
        Args:
            date_range:
                Tuple of (start_date, end_date) to filter windows.

            offset:
                Optional offset in seconds to add to window start time.

            **kwargs:
                Additional keyword arguments passed to base PTransform class.
        """
        super().__init__(**kwargs)
        self._date_range = date_range
        self._offset = offset

    def expand(self, pcoll: PCollection) -> PCollection:
        """Filters elements by checking window intersection with date range.

        Args:
            pcoll:
                Input PCollection with windowed elements.

        Returns:
            Filtered PCollection with only elements in windows intersecting date_range.
        """
        return pcoll | "FilterByWindowDateRange" >> beam.Filter(
            lambda elem, window=beam.DoFn.WindowParam: self._filter_window(window)
        )

    def _filter_window(self, window: IntervalWindow) -> bool:
        window_start = (
            window.start.to_utc_datetime(has_tz=True)
            + timedelta(seconds=self._offset)
        ).date()

        return window_start < self._date_range[1]
