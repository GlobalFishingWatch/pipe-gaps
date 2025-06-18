"""Module with reusable PTransforms for reading input PCollections."""
from pathlib import Path
from typing import Union

import apache_beam as beam

from pipe_gaps.common.io import json_load
from pipe_gaps.assets import schemas


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
