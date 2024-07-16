from datetime import datetime
import pytest

from pipe_gaps import pipe
from pipe_gaps.utils import json_save


def test_pipe_validation(tmp_path):
    with pytest.raises(ValueError):
        pipe.run()

    with pytest.raises(ValueError):
        query_params = {"start_date": datetime(2024, 1, 1).date()}
        pipe.run(query_params=query_params)

    filepath = tmp_path / "test.json"
    json_save({}, filepath)

    # with pytest.raises(pipe.NoMessagesFound):
    #    pipe.run(input_file=filepath)


def test_pipe_saved_file(tmp_path):
    messages = [
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": "2024-01-04 20:48:40.000000 UTC",
        }
    ]

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    # Without saving results.
    pipe.run(input_file=input_file, work_dir=tmp_path, save_json=False)

    # Saving results.
    pipe.run(input_file=input_file, work_dir=tmp_path, save_json=True)
    output_path = tmp_path.joinpath(f"{pipe.OUTPUT_PREFIX}-{input_file.stem}.json")
    assert output_path.is_file()
