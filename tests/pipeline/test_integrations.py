from pipe_gaps.pipeline.beam.transforms.sinks import BIGQUERY_SCHEMAS
from pipe_gaps.core import GapDetector
from pipe_gaps.utils import json_load


def test_output_gaps_comply_schema(input_file):
    messages = json_load(input_file)
    schema = BIGQUERY_SCHEMAS["gaps"]

    detector = GapDetector(threshold=1.2, normalize_output=True)
    gap = detector.create_gap(messages[0], messages[1])

    schema_keys = [f["name"] for f in schema]

    for k in gap:
        assert k in schema_keys, f"Key '{k}' in output gap not present in schema."

    for k in schema_keys:
        assert k in gap, f"Key '{k}' in schema not present in output gap."
