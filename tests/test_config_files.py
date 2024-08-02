import glob
import json

import pytest

from pipe_gaps.pipeline import Config
from pipe_gaps.utils import json_load


@pytest.mark.skip(reason="Interface not clear yet.")
def test_config_files():
    for file in glob.glob("config/*.json"):
        try:
            config_dict = json_load(file)
            config = Config(**config_dict)
            config.validate()
        except json.decoder.JSONDecodeError as e:
            raise Exception(f"Error in {file}: {e}")
