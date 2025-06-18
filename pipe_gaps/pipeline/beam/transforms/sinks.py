"""Module with reusable PTransforms for writing output PCollections."""
from importlib_resources import files

# from apache_beam.io.fileio import default_file_naming

from pipe_gaps.common.io import json_load


BIGQUERY_SCHEMAS = {
    "gaps": json_load(files("pipe_gaps.assets.schemas").joinpath("ais-gaps.json"))
}


def get_bigquery_schema(name):
    if name not in BIGQUERY_SCHEMAS:
        raise NotImplementedError(
            f"Schema with name '{name}' not implemented!. "
            f"Available schemas: {list(BIGQUERY_SCHEMAS.keys())}.")

    return BIGQUERY_SCHEMAS[name]
