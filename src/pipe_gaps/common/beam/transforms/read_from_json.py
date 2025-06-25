"""Module with reusable source PTransform for reading JSON inputs."""
from pathlib import Path
from typing import Union, Callable, Any

import apache_beam as beam

from pipe_gaps.common.io import json_load
from pipe_gaps.common.beam.exceptions import PTransformError


class ReadFromJson(beam.PTransform):
    """Beam transform to read a PCollection from a JSON file.

    This transform loads a local JSON or JSONLines file eagerly (outside the pipeline),
    then injects the resulting records into the pipeline using `beam.Create`.

    Useful for testing, prototyping, or controlled ingestion.

    Args:
        input_file:
            Path to the local file to read.

        coder:
            Callable to apply to each decoded record. Defaults to `dict`.

        lines:
            If True, interprets the input as newline-delimited JSON (JSONLines).

        create_kwargs:
            Optional dictionary of keyword arguments to pass to `beam.Create`.
            Use this to control serialization, type hints, etc.

        **kwargs:
            Additional keyword arguments passed to base PTransform class.

    Raises:
        PTransformError:
            If the input file does not exist at pipeline construction time.

    Example:
        >>> with beam.Pipeline() as p:
        ...     pcoll = p | ReadFromJson('data/input.json', lines=True)
        ...     pcoll | beam.Map(print)
    """
    def __init__(
        self,
        input_file: Union[str, Path],
        coder: Callable = dict,
        lines: bool = False,
        create_kwargs: dict = None,
        **kwargs: Any,
    ) -> None:
        """Builds a ReadFromJson instance."""
        super().__init__(**kwargs)
        self._input_file = Path(input_file)
        self._coder = coder
        self._lines = lines
        self._create_kwargs = create_kwargs or {}

    def expand(self, p):  # p is the Pipeline, not a PCollection
        """Apply transform to pipeline 'p': create PCollection from loaded JSON data."""

        # Why not use beam.ReadFromJson instead of (beam.Create + json_load)?
        # The thing is that beam.ReadFromJson returns BeamSchema objects,
        # and then we need to convert those objects to dict...
        # inputs = (
        #     p
        #     | beam.io.ReadFromJson(str(input_file), lines=False, convert_dates=False)
        #     | beam.Map(lambda x: dict(x._asdict())).with_output_types(Message)
        # )

        if not self._input_file.exists():
            raise PTransformError(f"Input file does not exist: {self._input_file}")

        data = json_load(self._input_file, lines=self._lines, coder=self._coder)
        return p | beam.Create(data, **self._create_kwargs).with_output_types(self._coder)
