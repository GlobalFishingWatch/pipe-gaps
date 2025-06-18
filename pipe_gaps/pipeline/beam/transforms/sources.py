"""Module with reusable PTransforms for reading input PCollections."""
from pathlib import Path
from typing import Union

import apache_beam as beam

from pipe_gaps import queries
from pipe_gaps.common.io import json_load
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
    """Beam transform to read Pcollection from a JSON file.

    Args:
        transform: the PTransform to use.
    """
    def __init__(self, transform: beam.PTransform):
        self._transform = transform

    @classmethod
    def build(
        cls, input_file: Union[str, Path], schema: str = None, lines: bool = False, **kwargs
    ) -> "ReadFromJson":
        """Builds a ReadFromJson instance.

        Args:
            input_file: The filepath to read.
            schema: The schema for the PCollection type. If None, uses dict.
            lines: If True, interprets JSON file as JSONLines format.
            **kwargs: Extra keyword arguments for beam.io.Create constructor.

        Returns:
            An instance of ReadFromJson.
        """

        # Why not use beam.ReadFromJson instead of (beam.Create + json_load)?
        # The thing is that beam.ReadFromJson returns BeamSchema objects,
        # and then we need to convert those objects to dict...
        # inputs = (
        #     p
        #     | beam.io.ReadFromJson(str(input_file), lines=False, convert_dates=False)
        #     | beam.Map(lambda x: dict(x._asdict())).with_output_types(Message)
        # )

        schema = dict
        if schema is None:
            schema = schemas.get_schema(schema)

        transform = beam.Create(json_load(input_file, lines=lines, coder=schema))
        transform.with_output_types(schema)

        return cls(transform)

    def expand(self, pcoll):
        return pcoll | self._transform


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    """Mocks beam.io.ReadFromBigQuery.

    Args:
        elements: Elements to use as output Pcollection.
    """
    def __init__(self, elements: list[dict] = None, **kwargs):
        self._elements = elements

        if self._elements is None:
            self._elements = []

    def expand(self, pcoll):
        return pcoll | beam.Create(self._elements)


class ReadFromQuery(beam.PTransform):
    """Beam transform to read Pcollection from BigQuery table.

    Args:
        transform: the PTransform to use.
    """
    def __init__(self, transform: beam.PTransform):
        self._transform = transform

    @classmethod
    def build(
        cls,
        query_name: str,
        query_params: dict,
        use_schema: bool = False,
        mock_db_client: bool = False,
        method: str = beam.io.ReadFromBigQuery.Method.EXPORT,
        **kwargs
    ) -> "ReadFromQuery":
        """Builds a ReadFromQuery instance.

        Args:
            query_name: The name of the query.
            query_params: The parameters of the query.
            use_schema: If true, uses query defined schema as PCollection type. If not, uses dict.
            mock_db_client: If True, uses a mock for the database client.
            method: The method to use to read from BigQuery. It may be EXPORT or DIRECT_READ.
            **kwargs: Extra keyword arguments for beam.io.ReadFromBigQuery constructor.

        Returns:
            An instance of ReadFromQuery.
        """
        query = queries.get_query(query_name, query_params)

        class_ = beam.io.ReadFromBigQuery

        if mock_db_client:
            class_ = ReadFromBigQueryMock

        transform = class_(use_standard_sql=True, query=query.render(), method=method, **kwargs)

        if use_schema:
            transform = transform.with_output_types(query.schema())

        return cls(transform)

    def expand(self, pcoll):
        return pcoll | self._transform
