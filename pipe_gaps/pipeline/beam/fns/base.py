"""This modules defines a base Fn useful to wire core algorithms with apache beam."""
from typing import NamedTuple, Type
from abc import ABC, abstractmethod

import apache_beam as beam

from pipe_gaps.pipeline.common import ProcessingUnitKey


class BaseFn(beam.DoFn, ABC):
    """Base type for Fns to use in Core transforms."""

    @abstractmethod
    def process(self, element: tuple) -> list:
        """Process elements of a keyed p-collection.

        Args:
            element: Processing unit (key, inputs).

        Returns:
            Processed items (key, outputs).
        """

    @staticmethod
    @abstractmethod
    def type() -> NamedTuple:
        """Returns the schema for the output items."""

    @staticmethod
    @abstractmethod
    def processing_unit_key() -> Type[ProcessingUnitKey]:
        """Returns the class with from_dict method that defines
            the key to groupby inputs for this Fn. Meant to be used in a beam.GroupBy transform.

        Returns:
            A ProcessingUnitKey class.
        """
