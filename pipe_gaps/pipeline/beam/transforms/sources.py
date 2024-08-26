"""Module with beam transforms for reading input pcollections."""
from pathlib import Path
from typing import Union, NamedTuple

import apache_beam as beam

from pipe_gaps.utils import json_load


class ReadFromJson(beam.PTransform):
    """Beam transform to read pcollection from a JSON file.

    Args:
        input_file (Union[str, Path]): The filepath to read.
        schema (NamedTuple): The schema for the pcollection.
        lines: If True, interprets JSON file as JSONLines format.
    """
    def __init__(self, input_file: Union[str, Path], schema: NamedTuple, lines: bool = False):
        self._input_file = input_file
        self._schema = schema
        self._lines = lines

    def expand(self, pcoll):
        # beam.ReadFromJson returns BeamSchema objects, and then we need to convert to dict...
        # inputs = (
        #     p
        #     | beam.io.ReadFromJson(str(input_file), lines=False, convert_dates=False)
        #     | beam.Map(lambda x: dict(x._asdict())).with_output_types(Message)
        # )
        return pcoll | beam.Create(
            json_load(self._input_file, lines=self._lines)).with_output_types(self._schema)


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
        schema (NamedTuple): The schema for the pcollection.
        mock_db_client (bool, optional): If True, uses ReadFromBigQueryMock transform.
        **kwargs: Extra keyword arguments for beam.io.ReadFromBigQuery constructor.
    """
    def __init__(self, schema: NamedTuple, mock_db_client: bool = False, **kwargs):
        class_ = beam.io.ReadFromBigQuery

        if mock_db_client:
            class_ = ReadFromBigQueryMock

        self._transform = class_(**kwargs).with_output_types(schema)

    def expand(self, pcoll):
        return pcoll | self._transform
