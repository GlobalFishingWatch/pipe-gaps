import glob
import json

from pipe_gaps.pipeline import Config
from pipe_gaps.utils import json_load

from pydantic import ValidationError


def test_config_files():
    for file in glob.glob("config/*.json"):
        try:
            config_dict = json_load(file)
            config_dict.pop("pipe_type")
            config = Config.model_validate(config_dict)
            config.validate()
        except (json.decoder.JSONDecodeError, ValidationError) as e:
            raise Exception(f"Error in {file}: {e}")
