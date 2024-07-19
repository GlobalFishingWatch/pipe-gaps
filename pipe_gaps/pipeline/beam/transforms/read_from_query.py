from typing import NamedTuple

import apache_beam as beam


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
