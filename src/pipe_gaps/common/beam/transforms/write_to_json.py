"""Module with reusable PTransforms for writing output PCollections."""
import json
from typing import Any
from pathlib import Path
from datetime import datetime

import apache_beam as beam
# from apache_beam.io.fileio import default_file_naming


class WriteToJson(beam.PTransform):
    """Writes PCollection as JSON.

    Args:
        output_dir:
            Output directory.

        output_prefix:
            Prefix to use in filename/s.

        **kwargs:
            Additional keyword arguments passed to base PTransform class.
    """
    WORKDIR_DEFAULT = "workdir"

    def __init__(
        self, output_dir: str = WORKDIR_DEFAULT, output_prefix: str = "", **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._output_dir = Path(output_dir)

        time = datetime.now().isoformat(timespec="seconds").replace("-", "").replace(":", "")
        self._output_prefix = f"beam-{output_prefix}-{time}"

        self._prefix = self._output_dir.joinpath(self._output_prefix).as_posix()
        self._shard_name_template = ''
        self._suffix = ".json"

        # This is what beam.io.WriteToText does to construct the path.
        self.path = Path(''.join([self._prefix, self._shard_name_template, self._suffix]))

    def expand(self, pcoll):
        return pcoll | 'WriteToJson' >> (
            beam.Map(json.dumps) |
            beam.io.WriteToText(
                self._prefix,
                shard_name_template=self._shard_name_template,
                file_name_suffix=self._suffix
            )
        )
        """
        Why not use beam.io.WriteToJson?
        `WriteToJson` has issues writing to local files.
        It raises an error because it interprets the filepath as an invalid GCS location.
        It works fine when used with `ReadFromBigQuery` and specifying a GCS location there.
        Also, under the hood, it uses pandas.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToJson

        Example usage:

            file_naming = default_file_naming(prefix=self._output_prefix, suffix=".json")
            return pcoll | beam.io.WriteToJson(
                self._output_dir.as_posix(),
                file_naming=file_naming,
                lines=True,
                indent=4,
            )

        On the other hand, `beam.io.WriteToText` is more predictable and reliable
        for local file outputs.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        """
