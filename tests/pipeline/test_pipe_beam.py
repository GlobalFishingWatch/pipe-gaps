import pytest
from datetime import datetime

from pipe_gaps.pipeline.schemas import Message
from pipe_gaps.pipeline.beam.fns import DetectGapsFn
from pipe_gaps.pipeline.beam.transforms import Core, ReadFromJson
from pipe_gaps.pipeline import BeamPipeline, PipelineError
from pipe_gaps.pipeline import pipe_beam
from pipe_gaps.utils import json_save, json_load

from tests.conftest import TestCases


def test_with_input_file(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    pipe = BeamPipeline.build(input_file=input_file, core=dict(threshold=0.5))
    pipe.run()


def test_multiple_sources(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    core = Core(core_fn=DetectGapsFn())
    read1 = "Read1" >> ReadFromJson(input_file, schema=Message)
    read2 = "Read2" >> ReadFromJson(input_file, schema=Message)

    sources = [read1, read2]
    sinks = []

    pipe = BeamPipeline(sources, core, sinks, output_path=input_file)
    pipe.run()


def test_no_output_path(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    pipe = BeamPipeline.build(input_file=input_file)
    with pytest.raises(PipelineError):
        pipe.output_path.is_file()


def test_distance_from_shore_is_null(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    for m in messages:
        m["distance_from_shore_m"] = None

    json_save(messages, input_file)

    pipe = BeamPipeline.build(input_file=input_file, core=dict(threshold=0.5))
    pipe.run()


def test_with_input_params():
    input_query = {
        "start_date": datetime(2024, 1, 1).date().isoformat(),
        "end_date": datetime(2024, 1, 1).date().isoformat(),
    }

    pipe = BeamPipeline.build(mock_db_client=True, input_query=input_query)
    pipe.run()


def test_save_json(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    pipe = BeamPipeline.build(input_file=input_file, work_dir=tmp_path, save_json=True)
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

    pipe = BeamPipeline.build(
        input_file=input_file,
        work_dir=tmp_path,
        core=dict(threshold=threshold),
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

    pipe = BeamPipeline.build(
        input_file=input_file,
        work_dir=tmp_path,
        core=dict(threshold=threshold, eval_last=True),
        save_json=True
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)
    assert len(gaps) == expected_gaps

    if len(gaps) > 0:
        for gap in gaps:
            assert gap["ON"] is None


def test_verbose(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    import logging
    pipe_beam.logger.setLevel(logging.DEBUG)

    pipe = BeamPipeline.build(input_file=input_file, core=dict(threshold=0.5))
    pipe.run()
