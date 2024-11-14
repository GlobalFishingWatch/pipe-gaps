"""Module with beam transforms for writing output pcollections."""
import json
from datetime import datetime
from pathlib import Path
from importlib_resources import files

import apache_beam as beam
# from apache_beam.io.fileio import default_file_naming

from pipe_gaps.utils import json_load


def sinks_factory(kind, **kwargs):
    SINKS_MAP = {
        "json": WriteJson,
        "bigquery": WriteBigQueryTable
    }

    if kind not in SINKS_MAP:
        raise NotImplementedError(f"Sink transform {kind} not implemented.")

    return SINKS_MAP[kind].build(**kwargs)


BIGQUERY_SCHEMAS = {
    "gaps": json_load(files("pipe_gaps.pipeline.schemas").joinpath("ais-gaps.json"))
}


def get_bigquery_schema(name):
    if name not in BIGQUERY_SCHEMAS:
        raise NotImplementedError(
            f"Schema with name '{name}' not implemented!. "
            f"Available schemas: {list(BIGQUERY_SCHEMAS.keys())}.")

    return BIGQUERY_SCHEMAS[name]


class WriteJson(beam.PTransform):
    """Writes p-collection as JSON.

    Args:
        output_dir: Output directory.
        output_prefix: Prefix to use in filename/s.
    """
    def __init__(self, output_dir: str = "workdir", output_prefix: str = ""):
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


class WriteToBigQueryMock(beam.io.WriteToBigQuery):
    """Mocks beam.io.WriteToBigQuery."""
    def expand(self, pcoll):
        pass


class WriteBigQueryTable(beam.PTransform):
    """Writes p-collection in BigQuery table.

    Args:
        **kwargs: keyword arguments for beam.io.WriteToBigQuery constructor.
    """
    def __init__(self, transform: beam.PTransform):
        self._transform = transform

    @classmethod
    def build(
        cls,
        schema: str = None,
        description: str = None,
        mock_db_client=False,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        **kwargs
    ):
        """Builds a WriteBigQueryTable instance.

        Args:
            schema: name of the schema for the output table.
            description: description for the output table.
            mock_db_client: If True, uses a mock for the database client.
            write_disposition: whether to overwrite table or just append.
            **kwargs: keyword arguments for beam.io.WriteToBigQuery constructor.

        Returns:
            An instance of WriteBigQueryTable.
        """
        if schema is not None:
            schema = cls._build_schema(schema)

        class_ = beam.io.WriteToBigQuery

        if mock_db_client:
            class_ = WriteToBigQueryMock

        transform = class_(
            schema=schema,
            additional_bq_parameters=cls._build_bq_params(description),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=write_disposition,
            validate=True,
            **kwargs
        )

        return cls(transform)

    @classmethod
    def _build_schema(cls, name: str):
        schema = {}
        schema["fields"] = get_bigquery_schema(name)

        return schema

    @classmethod
    def _build_bq_params(cls, description: str = None):
        return {
            "destinationTableProperties": {
                "description": description
            }
        }

    def expand(self, pcoll):
        return pcoll | self._transform
