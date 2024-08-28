import pytest
from datetime import datetime

from pipe_gaps.pipeline.common import DetectGaps
from pipe_gaps.pipeline.beam.transforms import Core, ReadFromJson
from pipe_gaps.pipeline import BeamPipeline, PipelineError
from pipe_gaps.pipeline import pipe_beam
from pipe_gaps.utils import json_save, json_load

from tests.conftest import TestCases


def input_file_config(input_file, schema):
    return dict(kind="json", input_file=input_file, schema=schema)


def test_with_input_file(input_file):
    core_config = dict(threshold=0.5, eval_last=False)
    inputs = [input_file_config(input_file, schema="messages")]

    pipe = BeamPipeline.build(inputs=inputs, core=core_config)
    pipe.run()


def test_multiple_sources(input_file):
    core = Core(core_process=DetectGaps.build())
    read1 = "Read1" >> ReadFromJson.build(input_file, schema="messages")
    read2 = "Read2" >> ReadFromJson.build(input_file, schema="messages")

    sources = [read1, read2]
    sinks = []

    pipe = BeamPipeline(sources, core, sinks, output_path=input_file)
    pipe.run()


def test_no_output_path(input_file):
    inputs = [input_file_config(input_file, schema="messages")]
    pipe = BeamPipeline.build(inputs=inputs)

    with pytest.raises(PipelineError):
        pipe.output_path.is_file()


def test_distance_from_shore_is_null(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    for m in messages:
        m["distance_from_shore_m"] = None

    json_save(messages, input_file)
    core_config = dict(threshold=0.5, eval_last=False)
    inputs = [input_file_config(input_file, schema="messages")]

    pipe = BeamPipeline.build(inputs=inputs, core=core_config)
    pipe.run()


def test_with_input_query():
    input_query = dict(
        kind="query",
        mock_db_client=True,
        query_name="messages",
        query_params=dict(
            start_date=datetime(2024, 1, 1).date().isoformat(),
            end_date=datetime(2024, 1, 1).date().isoformat()
        )
    )
    pipe = BeamPipeline.build(inputs=[input_query])
    pipe.run()


def test_save_json(tmp_path, input_file):
    inputs = [input_file_config(input_file, schema="messages")]

    pipe = BeamPipeline.build(inputs=inputs, work_dir=tmp_path, save_json=True)
    pipe.run()

    assert pipe.output_path.is_file()


@pytest.mark.parametrize(
    "messages, threshold, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["threshold"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.GAP_BETWEEN_YEARS
    ],
)
def test_gap_between_years(tmp_path, messages, threshold, expected_gaps):
    # Checks that a gap between years is properly detected.

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    core_config = dict(threshold=threshold, eval_last=False)

    inputs = [input_file_config(input_file, schema="messages")]

    pipe = BeamPipeline.build(
        inputs=inputs,
        work_dir=tmp_path,
        core=core_config,
        save_json=True
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)
    assert len(gaps) == expected_gaps


@pytest.mark.parametrize(
    "messages, threshold, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["threshold"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.OPEN_GAPS
    ],
)
def test_open_gaps(tmp_path, messages, threshold, expected_gaps):
    # Checks that a gap between years is properly detected.

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    core_config = dict(threshold=threshold, eval_last=True)
    inputs = [input_file_config(input_file, schema="messages")]

    pipe = BeamPipeline.build(
        inputs=inputs,
        work_dir=tmp_path,
        core=core_config,
        save_json=True
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)
    assert len(gaps) == expected_gaps

    if len(gaps) > 0:
        for gap in gaps:
            assert gap["ON"] is None


@pytest.mark.parametrize(
    "messages, open_gaps, threshold, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["open_gaps"],
            case["threshold"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.CLOSING_GAPS
    ],
)
def test_closing_gaps(tmp_path, messages, open_gaps, threshold, expected_gaps):
    # Checks that an existing open gap is properly closed.

    input_file = tmp_path.joinpath("messages-test.json")
    json_save(messages, input_file)
    inputs = [input_file_config(input_file, schema="messages")]

    side_input_file = tmp_path.joinpath("open-gaps-test.json")
    json_save(open_gaps, side_input_file, lines=True)
    side_inputs_config = dict(kind="json", input_file=side_input_file, schema="gaps", lines=True)
    side_inputs = [side_inputs_config]

    pipe = BeamPipeline.build(
        inputs=inputs,
        side_inputs=side_inputs,
        work_dir=tmp_path,
        core=dict(threshold=threshold),
        save_json=True
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)
    assert len(gaps) == expected_gaps

    if len(gaps) > 0:
        for gap in gaps:
            assert gap["ON"] is not None


def test_verbose(tmp_path, input_file):
    import logging
    pipe_beam.logger.setLevel(logging.DEBUG)

    core_config = dict(threshold=0.5, eval_last=False)
    inputs = [input_file_config(input_file, schema="messages")]

    pipe = BeamPipeline.build(inputs=inputs, core=core_config)
    pipe.run()
