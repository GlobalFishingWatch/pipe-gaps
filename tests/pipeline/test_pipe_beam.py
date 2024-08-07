from datetime import datetime

from pipe_gaps.pipeline import BeamPipeline
from pipe_gaps.utils import json_save


def test_with_input_file(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
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

    pipe = BeamPipeline.build(input_file=input_file, save_json=True)
    pipe.run()
