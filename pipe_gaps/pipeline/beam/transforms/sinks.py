"""Module with beam transforms for writing output pcollections."""
import json
from pathlib import Path

import apache_beam as beam
# from apache_beam.io.fileio import default_file_naming


def sinks_factory(kind, **kwargs):
    SINKS_MAP = {
        "json": WriteJson
    }

    if kind not in SINKS_MAP:
        raise NotImplementedError(f"Sink transform {kind} not implemented.")

    return SINKS_MAP[kind](**kwargs)


class WriteJson(beam.PTransform):
    """Writes p-collection as JSON.

    Args:
        output_dir: Output directory.
        output_prefix: Prefix to use in filename/s.
    """
    def __init__(self, output_dir: Path = Path("workdir"), output_prefix: str = ""):
        self._output_dir = output_dir
        self._output_prefix = output_prefix

        self._prefix = self._output_dir.joinpath(self._output_prefix).as_posix()
        self._shard_name_template = ''
        self._suffix = ".json"

        # This is what beam does to construct the path.
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
        beam.io.WriteToJson pTransform is more direct but has issues writing locally.
        Raises an error because interprets the filepath as an invalid gcs_location...
        It works when you use ReadFromBigQuery and set the gcs_location there (very rare behavior).
        It uses pandas under the hood.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToJson.

        beam.io.WriteToText pTransform is more predictable.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText.

        file_naming = default_file_naming(prefix=self._output_prefix, suffix=".json")
        return pcoll | beam.io.WriteToJson(
            self._output_dir.as_posix(),
            file_naming=default_file_naming(prefix=self._output_prefix, suffix=".json"),
            lines=False,  # TODO: consider using JSON Lines format (lines=True).
            indent=4,
        )
        """
