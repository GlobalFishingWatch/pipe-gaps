import apache_beam as beam

from apache_beam.io.fileio import default_file_naming


class WriteJson(beam.PTransform):
    def __init__(self, output_dir, output_prefix=""):
        self._output_dir = output_dir
        self._file_naming = default_file_naming(prefix=output_prefix, suffix=".json")

    def expand(self, pcoll):
        # TODO: consider using JSON Lines format (lines=True).
        return pcoll | beam.io.WriteToJson(
            self._output_dir,
            file_naming=self._file_naming,
            lines=False,
            indent=4,
        )
