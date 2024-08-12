from datetime import datetime

from pipe_gaps.pipeline import BeamPipeline
from pipe_gaps.utils import json_save, json_load


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

    pipe = BeamPipeline.build(input_file=input_file, work_dir=tmp_path, save_json=True)
    pipe.run()

    assert pipe.output_path.is_file()


def test_border_case(tmp_path):
    # Checks that a gap between years is properly detected.
    messages = [
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": datetime(2023, 12, 31, 22).timestamp(),
            "distance_from_shore_m": 1
        },
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": datetime(2023, 12, 31, 23).timestamp(),
            "distance_from_shore_m": 1
        },
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": datetime(2024, 1, 1, 1).timestamp(),
            "distance_from_shore_m": 1
        }
    ]

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    pipe = BeamPipeline.build(
        input_file=input_file, work_dir=tmp_path, core=dict(threshold=1), save_json=True
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)

    assert len(gaps) == 1
