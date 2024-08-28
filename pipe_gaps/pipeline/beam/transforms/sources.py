"""Module with beam transforms for reading input pcollections."""
from pathlib import Path
from typing import Union

import apache_beam as beam

from pipe_gaps import queries
from pipe_gaps.utils import json_load
from pipe_gaps.pipeline import schemas


def sources_factory(kind, **kwargs):
    SOURCES_MAP = {
        "query": ReadFromQuery,
        "json": ReadFromJson
    }

    if kind not in SOURCES_MAP:
        raise NotImplementedError(f"Source transform {kind} not implemented.")

    return SOURCES_MAP[kind].build(**kwargs)


class ReadFromJson(beam.PTransform):
    """Beam transform to read pcollection from a JSON file.

    Args:
        transform: the p-Transform to use.
    """
    def __init__(self, transform: beam.PTransform):
        self._transform = transform

    @classmethod
    def build(
        cls, input_file: Union[str, Path], schema: str = None, lines=False, **kwargs
    ) -> "ReadFromJson":
        """Builds a ReadFromJson instance.

        Args:
            input_file: The filepath to read.
            schema: The schema for the p-collection.
            lines: If True, interprets JSON file as JSONLines format.
            **kwargs: Extra keyword arguments for beam.io.Create constructor.

        Returns:
            An instance of ReadFromJson.
        """

        # beam.ReadFromJson returns BeamSchema objects, and then we need to convert to dict...
        # inputs = (
        #     p
        #     | beam.io.ReadFromJson(str(input_file), lines=False, convert_dates=False)
        #     | beam.Map(lambda x: dict(x._asdict())).with_output_types(Message)
        # )
        transform = beam.Create(json_load(input_file, lines=lines))

        if schema is not None:
            transform = transform.with_output_types(schemas.get_schema(schema))

        return cls(transform)

    def expand(self, pcoll):
        return pcoll | self._transform


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    """Mocks beam.io.ReadFromBigQuery.

    Args:
        elements: Elements to use as output pcollection.
    """
    def __init__(self, elements: list[dict] = None, **kwargs):
        self._elements = elements

        if self._elements is None:
            self._elements = []

    def expand(self, pcoll):
        return pcoll | beam.Create(self._elements)


class ReadFromQuery(beam.PTransform):
    """Beam transform to read pcollection from BigQuery table.

    Args:
        transform: the p-Transform to use.
    """
    def __init__(self, transform: beam.PTransform):
        self._transform = transform

    @classmethod
    def build(
        cls,
        query_name: str, query_params: dict, mock_db_client=False, **kwargs
    ) -> "ReadFromQuery":
        """Builds a ReadFromQuery instance.

        Args:
            query_name: The name of the query.
            query_params: The parameters of the query.
            mock_db_client: If True, uses a mock for the database client.
            **kwargs: Extra keyword arguments for beam.io.ReadFromBigQuery constructor.

        Returns:
            An instance of ReadFromQuery.
        """
        query = queries.get_query(query_name, query_params)

        class_ = beam.io.ReadFromBigQuery

        if mock_db_client:
            class_ = ReadFromBigQueryMock

        transform = class_(use_standard_sql=True, query=query.render(), **kwargs)
        transform = transform.with_output_types(query.schema())

        return cls(transform)

    def expand(self, pcoll):
        return pcoll | self._transform
