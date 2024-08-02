import pytest
from datetime import datetime

from pipe_gaps.pipeline import NaivePipeline, NoInputsFound
from pipe_gaps.utils import json_save


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


def test_with_query_params():
    query_params = dict(
        start_date=datetime(2024, 1, 1).date().isoformat(),
        end_date=datetime(2024, 1, 1).date().isoformat(),
    )

    pipe = NaivePipeline.build(mock_db_client=True, query_params=query_params)
    pipe.run()


def test_save_json(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    pipe = NaivePipeline.build(input_file=input_file, save_json=True)
    pipe.run()


def test_save_stats(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    pipe = NaivePipeline.build(input_file=input_file, save_stats=True)
    pipe.run()
