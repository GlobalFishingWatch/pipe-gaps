"""Module with beam transforms for processing input pcollections."""

import apache_beam as beam

from ..fns.base import BaseFn

import logging
import operator
from pipe_gaps.core import gap_detector as gd


logger = logging.getLogger(__name__)


def get_boundaries(item):
    key, messages = item
    minimum = min(messages, key=lambda x: x["timestamp"])
    maximum = max(messages, key=lambda x: x["timestamp"])

    return {"ssvid": key.ssvid, "year": key.year, "borders": (minimum, maximum)}


def process_boundaries(item):
    key, items = item

    items_sorted = sorted(items, key=operator.itemgetter("year"))
    pairs = zip(items_sorted[:-1], items_sorted[1:])

    boundaries = {}
    for left, right in pairs:
        boundary_key = f"{left["year"]}-{right["year"]}"
        boundaries[boundary_key] = [left["borders"][0], right["borders"][1]]

    gaps = [gd.detect(messages) for messages in boundaries.values()]
    logger.info(f"Found {len(gaps)} gaps analyzing boundaries for key={key}...")

    return gaps


class Core(beam.PTransform):
    def __init__(self, core_fn: BaseFn):
        """A core beam transform for pipelines.

        This transform will:
            1. Groups input p-collection by a key defined in core_fn.
            2. Process groups in parallel, applying core_fn.
            3. Ungroups the results and assigns the schema defined in core_fn.

        Args:
            core_fn: The Fn that encapsulates the core transform.
        """
        self._core_fn = core_fn

    def expand(self, pcoll):
        groups = pcoll | "GroupBySsvidAndYear" >> beam.GroupBy(self._core_fn.parallelization_unit)
        processed_groups = groups | beam.ParDo(self._core_fn)
        flatten_groups = processed_groups | beam.FlatMapTuple(lambda k, v: v).with_output_types(
            self._core_fn.type())

        boundaries = groups | beam.Map(get_boundaries)
        boundaries = boundaries | beam.GroupBy(lambda item: item["ssvid"])
        processed_boundaries = boundaries | beam.Map(process_boundaries) | beam.LogElements()

        flatten_boundaries = processed_boundaries | beam.FlatMap().with_output_types(
            self._core_fn.type())

        return (flatten_groups, flatten_boundaries) | beam.Flatten()
