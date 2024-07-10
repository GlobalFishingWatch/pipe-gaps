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

    with pytest.raises(pipe.NoMessagesFound):
        pipe.run(input_file=filepath)
