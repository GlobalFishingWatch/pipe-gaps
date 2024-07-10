from datetime import datetime
from pathlib import Path

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

    with pytest.raises(pipe.NoMessagesFound):
        pipe.run(input_file=filepath)


def test_pipe_saved_file(tmp_path):
    messages = [{
        "ssvid": "226013750",
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": "2024-01-04 20:48:40.000000 UTC",
    }]

    filepath = tmp_path / "test.json"
    json_save(messages, filepath)

    output_path = tmp_path.joinpath("gaps.json")
    pipe.run(input_file=filepath, work_dir=tmp_path, save=False)
    assert not Path(output_path).is_file()

    pipe.run(input_file=filepath, work_dir=tmp_path, save=True)
    assert Path(output_path).is_file()
