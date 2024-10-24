import typing

from pipe_gaps.pipeline.beam.transforms.sinks import BIGQUERY_SCHEMAS
from pipe_gaps.core import GapDetector
from pipe_gaps.utils import json_load

from pipe_gaps.queries import AISGap


OUTPUT_GAPS_KEY_NOT_IN_SCHEMA = "Output gaps query contains key '{}' not present in output schema."
SCHEMA_KEY_NOT_IN_OUTPUT_GAP = "Output schema contains key '{}' not present in output gaps."

GAPS_QUERY_KEY_NOT_IN_SCHEMA = "Gaps query contains key '{}' not present in output schema."
SCHEMA_KEY_NOT_IN_GAPS_QUERY = "Output schema contains key '{}' not present in Gaps query."


def test_output_gaps_comply_schema(input_file):
    messages = json_load(input_file)
    schema = BIGQUERY_SCHEMAS["gaps"]

    detector = GapDetector(threshold=1.2, normalize_output=True)
    gap = detector.create_gap(messages[0], messages[1])

    schema_keys = [f["name"] for f in schema]

    for k in gap:
        assert k in schema_keys, OUTPUT_GAPS_KEY_NOT_IN_SCHEMA.fomat(k)

    for k in schema_keys:
        assert k in gap, SCHEMA_KEY_NOT_IN_OUTPUT_GAP.format(k)


def test_input_gaps_comply_schema(input_file):
    schema = BIGQUERY_SCHEMAS["gaps"]
    output_gap_schema_keys = [f["name"] for f in schema]

    input_gaps_query_schema_keys = typing.get_type_hints(AISGap).keys()

    for k in input_gaps_query_schema_keys:
        assert k in output_gap_schema_keys, GAPS_QUERY_KEY_NOT_IN_SCHEMA.format(k)

    for k in output_gap_schema_keys:
        assert k in input_gaps_query_schema_keys, SCHEMA_KEY_NOT_IN_GAPS_QUERY.format(k)
