from typing import Union, NamedTuple

from pathlib import Path
import apache_beam as beam

from pipe_gaps.utils import json_load


class ReadFromJson(beam.PTransform):
    def __init__(self, input_file: Union[str, Path], schema: NamedTuple):
        self._input_file = input_file
        self._schema = schema

    def expand(self, pcoll):
        # ReadFromJson returns BeamSchema objects, and then we need to convert to dict...
        # TODO: consider using JSON Lines format.
        # inputs = (
        #     p
        #     | beam.io.ReadFromJson(str(input_file), lines=False, convert_dates=False)
        #     | beam.Map(lambda x: dict(x._asdict())).with_output_types(Message)
        # )
        return pcoll | beam.Create(json_load(self._input_file)).with_output_types(self._schema)


class ReadFromBigQueryMock(beam.io.ReadFromBigQuery):
    def expand(self, pcoll):
        return pcoll | beam.Create([])


class ReadFromQuery(beam.PTransform):
    def __init__(self, schema: NamedTuple, mock_db_client: bool = False, **kwargs):
        class_ = beam.io.ReadFromBigQuery

        if mock_db_client:
            class_ = ReadFromBigQueryMock

        self._transform = class_(**kwargs).with_output_types(schema)

    def expand(self, pcoll):
        return pcoll | self._transform
