import pytest
from datetime import datetime

from pipe_gaps.pipeline import NaivePipeline, NoInputsFound
from pipe_gaps.utils import json_save, json_load
from tests.conftest import TestCases


def test_no_messages(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save([], input_file)

    pipe = NaivePipeline.build(input_file=input_file)
    with pytest.raises(NoInputsFound):
        pipe.run()


def test_with_input_file(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    pipe = NaivePipeline.build(input_file=input_file)
    pipe.run()


def test_with_input_query():
    input_query = dict(
        start_date=datetime(2024, 1, 1).date().isoformat(),
        end_date=datetime(2024, 1, 1).date().isoformat(),
    )

    pipe = NaivePipeline.build(mock_db_client=True, input_query=input_query)
    pipe.run()


def test_save_json(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    pipe = NaivePipeline.build(input_file=input_file, work_dir=tmp_path, save_json=True)
    pipe.run()

    assert pipe.output_path.is_file()


def test_save_stats(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    pipe = NaivePipeline.build(input_file=input_file, save_stats=True)
    pipe.run()


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

    input_file = tmp_path.joinpath("test-border-cases.json")
    json_save(messages, input_file)

    pipe = NaivePipeline.build(
        input_file=input_file,
        work_dir=tmp_path,
        core=dict(threshold=threshold),
        save_json=True
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=False)
    assert len(gaps) == expected_gaps
