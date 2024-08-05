"""This modules defines a base Fn useful to wire core algorithms with apache beam."""
from typing import NamedTuple
from abc import ABC, abstractmethod
from dataclasses import dataclass

import apache_beam as beam


@dataclass(eq=True, frozen=True)
class ProcessingUnit:
    """Defines a key to group by input pcollections."""

    @classmethod
    @abstractmethod
    def from_dict(cls, item: dict) -> "ProcessingUnit":
        """Creates an instance from a dictionary."""


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
    def parallelization_unit(item) -> ProcessingUnit:
        """Defines the parallelization unit for this Fn.

        Args:
            item: Description

        Returns:
            TYPE: Description
        """
        return ProcessingUnit.from_dict(item)
