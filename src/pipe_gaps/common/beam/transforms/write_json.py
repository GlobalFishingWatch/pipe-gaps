"""Module with reusable PTransforms for writing output PCollections."""
import json
from datetime import datetime
from pathlib import Path

import apache_beam as beam
# from apache_beam.io.fileio import default_file_naming


class WriteJson(beam.PTransform):
    """Writes PCollection as JSON.

    Args:
        output_dir: Output directory.
        output_prefix: Prefix to use in filename/s.
    """
    WORKDIR_DEFAULT = "workdir"

    def __init__(self, output_dir: str = WORKDIR_DEFAULT, output_prefix: str = ""):
        self._output_dir = Path(output_dir)

        time = datetime.now().isoformat(timespec="seconds").replace("-", "").replace(":", "")
        self._output_prefix = f"beam-{output_prefix}-{time}"

        self._prefix = self._output_dir.joinpath(self._output_prefix).as_posix()
        self._shard_name_template = ''
        self._suffix = ".json"

        # This is what beam.io.WriteToText does to construct the path.
        self.path = Path(''.join([self._prefix, self._shard_name_template, self._suffix]))

    @classmethod
    def build(cls, *args, **kwargs):
        return cls(*args, **kwargs)

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
        The thing is that beam.io.WriteToJson has issues writing locally.
        Raises an error because interprets the filepath as an invalid gcs_location...
        It works when you use ReadFromBigQuery and set the gcs_location there (very rare behavior).
        Also, it uses pandas under the hood.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToJson.

        file_naming = default_file_naming(prefix=self._output_prefix, suffix=".json")
        return pcoll | beam.io.WriteToJson(
            self._output_dir.as_posix(),
            file_naming=default_file_naming(prefix=self._output_prefix, suffix=".json"),
            lines=True,
            indent=4,
        )

        On the other hand, beam.io.WriteToText PTransform is more predictable.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText.
        """
