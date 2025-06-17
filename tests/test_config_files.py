import glob
import json

import pytest

from pipe_gaps.utils import json_load
from pipe_gaps.cli import build_pipeline

from pydantic import ValidationError, validate_call


@pytest.mark.skip("refactoring")
def test_config_files():
    for file in glob.glob("config/*.json"):
        try:
            config_dict = json_load(file)
            validate_call(build_pipeline)(**config_dict)
        except (json.decoder.JSONDecodeError, ValidationError) as e:
            raise Exception(f"Error in {file}: {e}")
